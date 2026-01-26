// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Claude (Anthropic AI)

#include "index/DeltaTriplesWriter.h"

#include "index/CompressedRelation.h"
#include "index/DeltaTriplesPaths.h"
#include "index/IndexImpl.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/File.h"
#include "util/Serializer/FileSerializer.h"

using namespace ad_utility::delta_triples;

// ____________________________________________________________________________
DeltaTriplesWriter::DeltaTriplesWriter(const IndexImpl& index,
                                       std::string baseDir)
    : index_{index}, baseDir_{std::move(baseDir)} {}

// ____________________________________________________________________________
IdTable DeltaTriplesWriter::extractAndSortTriples(
    const LocatedTriplesPerBlock& locatedTriples,
    const qlever::KeyOrder& keyOrder, bool filterInserts) {
  // Extract all located triples.
  auto allTriples = locatedTriples.extractAllTriples();

  // Create result table with 4 columns (col0, col1, col2, graph).
  IdTable result{4, ad_utility::makeUnlimitedAllocator<Id>()};
  if (allTriples.empty()) {
    return result;
  }

  // Filter by insert/delete flag and copy to IdTable.
  for (const auto& lt : allTriples) {
    // Skip triples that don't match the filter.
    if (lt.insertOrDelete_ != filterInserts) {
      continue;
    }

    const auto& ids = lt.triple_.ids();
    result.push_back({ids[0], ids[1], ids[2], ids[3]});
  }

  // Sort by permutation order. The triples are already permuted, so we just
  // need to ensure they're sorted.
  auto comp = [](const auto& a, const auto& b) {
    return std::tie(a[0], a[1], a[2], a[3]) < std::tie(b[0], b[1], b[2], b[3]);
  };
  std::ranges::sort(result.begin(), result.end(), comp);

  return result;
}

// ____________________________________________________________________________
std::vector<CompressedBlockMetadata>
DeltaTriplesWriter::writeSortedTriplesToFile(const IdTable& sortedTriples,
                                             const std::string& filename) {
  if (sortedTriples.empty()) {
    // No triples to write, return empty metadata.
    return {};
  }

  // Determine block size from the index configuration.
  // TODO: Make this configurable or derive from index settings.
  constexpr size_t DELTA_BLOCK_SIZE = 80'000;

  std::vector<CompressedBlockMetadata> blockMetadata;
  ad_utility::File outfile{filename, "w"};

  // Write triples in blocks.
  for (size_t startRow = 0; startRow < sortedTriples.numRows();
       startRow += DELTA_BLOCK_SIZE) {
    size_t endRow =
        std::min(startRow + DELTA_BLOCK_SIZE, sortedTriples.numRows());
    size_t numRows = endRow - startRow;

    // Extract this block's rows.
    IdTable block{4, ad_utility::makeUnlimitedAllocator<Id>()};
    block.resize(numRows);
    for (size_t i = 0; i < numRows; ++i) {
      for (size_t col = 0; col < 4; ++col) {
        block(i, col) = sortedTriples(startRow + i, col);
      }
    }

    // Compress and write each column.
    std::vector<CompressedBlockMetadata::OffsetAndCompressedSize> offsets;
    for (const auto& column : block.getColumns()) {
      auto offsetInFile = outfile.tell();
      std::vector<char> compressed =
          ZstdWrapper::compress(column.data(), column.size() * sizeof(Id));
      outfile.write(compressed.data(), compressed.size());
      offsets.push_back({offsetInFile, compressed.size()});
    }

    // Create block metadata.
    const auto& firstRow = block[0];
    const auto& lastRow = block[numRows - 1];

    CompressedBlockMetadata metadata;
    metadata.offsetsAndCompressedSize_ = std::move(offsets);
    metadata.numRows_ = numRows;
    metadata.firstTriple_ = {firstRow[0], firstRow[1], firstRow[2],
                             firstRow[3]};
    metadata.lastTriple_ = {lastRow[0], lastRow[1], lastRow[2], lastRow[3]};
    metadata.blockIndex_ = blockMetadata.size();

    // For delta triples, we don't compute graph info (can be added later).
    metadata.graphInfo_ = std::nullopt;
    metadata.containsDuplicatesWithDifferentGraphs_ = false;

    blockMetadata.push_back(std::move(metadata));
  }

  outfile.close();

  // Write metadata to the end of the file using serialization.
  ad_utility::serialization::FileWriteSerializer metadataWriter{filename,
                                                                std::ios::app};
  metadataWriter << blockMetadata;

  return blockMetadata;
}

// ____________________________________________________________________________
std::vector<CompressedBlockMetadata> DeltaTriplesWriter::writePermutation(
    Permutation::Enum permutation, const LocatedTriplesPerBlock& locatedTriples,
    bool isInsert, bool useTemporary) {
  // Get the file path.
  std::string path;
  if (useTemporary) {
    path = isInsert ? getDeltaTempInsertsPath(baseDir_, permutation)
                    : getDeltaTempDeletesPath(baseDir_, permutation);
  } else {
    path = isInsert ? getDeltaInsertsPath(baseDir_, permutation)
                    : getDeltaDeletesPath(baseDir_, permutation);
  }

  // Get the key order for this permutation.
  auto keyOrder = Permutation::toKeyOrder(permutation);

  // Extract and sort triples, filtering by insert/delete.
  IdTable sortedTriples =
      extractAndSortTriples(locatedTriples, keyOrder, isInsert);

  // Write to file and return metadata.
  return writeSortedTriplesToFile(sortedTriples, path);
}

// ____________________________________________________________________________
void DeltaTriplesWriter::writeAllPermutations(
    const LocatedTriplesPerBlockAllPermutations<false>& locatedTriplesNormal,
    const LocatedTriplesPerBlockAllPermutations<true>& locatedTriplesInternal,
    bool useTemporary) {
  // Write all regular permutations. Note: each LocatedTriplesPerBlock contains
  // both inserts and deletes, so we call writePermutation twice with different
  // filter flags.
  for (auto permutation : Permutation::ALL) {
    const auto& locatedTriples =
        locatedTriplesNormal[static_cast<size_t>(permutation)];

    // Write inserts and deletes separately.
    writePermutation(permutation, locatedTriples, true, useTemporary);
    writePermutation(permutation, locatedTriples, false, useTemporary);
  }

  // Write internal permutations (only PSO and POS).
  for (auto permutation : Permutation::INTERNAL) {
    const auto& locatedTriples =
        locatedTriplesInternal[static_cast<size_t>(permutation)];

    writePermutation(permutation, locatedTriples, true, useTemporary);
    writePermutation(permutation, locatedTriples, false, useTemporary);
  }
}

// ____________________________________________________________________________
void DeltaTriplesWriter::commitTemporaryFiles() {
  // Atomically rename all temporary files to permanent files.
  for (auto permutation : Permutation::ALL) {
    std::string tempInserts = getDeltaTempInsertsPath(baseDir_, permutation);
    std::string permInserts = getDeltaInsertsPath(baseDir_, permutation);
    std::string tempDeletes = getDeltaTempDeletesPath(baseDir_, permutation);
    std::string permDeletes = getDeltaDeletesPath(baseDir_, permutation);

    // Rename if temporary files exist.
    if (ad_utility::isRegularFile(tempInserts)) {
      std::rename(tempInserts.c_str(), permInserts.c_str());
    }
    if (ad_utility::isRegularFile(tempDeletes)) {
      std::rename(tempDeletes.c_str(), permDeletes.c_str());
    }
  }

  // Same for internal permutations.
  for (auto permutation : Permutation::INTERNAL) {
    std::string tempInserts = getDeltaTempInsertsPath(baseDir_, permutation);
    std::string permInserts = getDeltaInsertsPath(baseDir_, permutation);
    std::string tempDeletes = getDeltaTempDeletesPath(baseDir_, permutation);
    std::string permDeletes = getDeltaDeletesPath(baseDir_, permutation);

    if (ad_utility::isRegularFile(tempInserts)) {
      std::rename(tempInserts.c_str(), permInserts.c_str());
    }
    if (ad_utility::isRegularFile(tempDeletes)) {
      std::rename(tempDeletes.c_str(), permDeletes.c_str());
    }
  }
}
