// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Claude (Anthropic AI)

#include "index/OnDiskDeltaTriples.h"

#include "index/CompressedRelation.h"
#include "index/DeltaTriplesPaths.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/File.h"
#include "util/Serializer/FileSerializer.h"

using namespace ad_utility::delta_triples;

// ____________________________________________________________________________
OnDiskDeltaTriples::OnDiskDeltaTriples(std::string baseDir)
    : baseDir_{std::move(baseDir)} {}

// ____________________________________________________________________________
bool OnDiskDeltaTriples::hasOnDiskDeltas() const {
  return ql::ranges::any_of(permutations_, &DeltaPermutationFiles::hasDeltas);
}

// ____________________________________________________________________________
bool OnDiskDeltaTriples::hasOnDiskDeltasForPermutation(
    PermutationEnum permutation) const {
  return permutations_[static_cast<size_t>(permutation)].hasDeltas();
}

// ____________________________________________________________________________
const std::vector<CompressedBlockMetadata>&
OnDiskDeltaTriples::getInsertBlocksMetadata(PermutationEnum permutation) const {
  return permutations_[static_cast<size_t>(permutation)].insertsMetadata_;
}

// ____________________________________________________________________________
const std::vector<CompressedBlockMetadata>&
OnDiskDeltaTriples::getDeleteBlocksMetadata(PermutationEnum permutation) const {
  return permutations_[static_cast<size_t>(permutation)].deletesMetadata_;
}

// ____________________________________________________________________________
bool OnDiskDeltaTriples::hasOnDiskDeltasForBlock(PermutationEnum permutation,
                                                 size_t blockIndex) const {
  const auto& perm = permutations_[static_cast<size_t>(permutation)];

  // Check if any insert or delete block matches this blockIndex.
  auto hasBlock = [blockIndex](const auto& metadata) {
    return ql::ranges::any_of(metadata, [blockIndex](const auto& block) {
      return block.blockIndex_ == blockIndex;
    });
  };

  return hasBlock(perm.insertsMetadata_) || hasBlock(perm.deletesMetadata_);
}

// ____________________________________________________________________________
ad_utility::File& OnDiskDeltaTriples::getFile(PermutationEnum permutation,
                                              bool isInsert,
                                              const std::string& path) const {
  auto& perm = permutations_[static_cast<size_t>(permutation)];
  auto& fileOpt = isInsert ? perm.insertsFile_ : perm.deletesFile_;

  // Lazily open the file if not already open.
  if (!fileOpt.has_value()) {
    fileOpt.emplace(path, "r");
  }

  return fileOpt.value();
}

// ____________________________________________________________________________
std::optional<IdTable> OnDiskDeltaTriples::readBlockFromFile(
    const std::vector<CompressedBlockMetadata>& metadata,
    ad_utility::File& file, size_t blockIndex,
    CompressedRelationReader::ColumnIndicesRef columns,
    const CompressedRelationReader::Allocator& allocator) const {
  // Find the block metadata matching the requested blockIndex.
  auto it = ql::ranges::find_if(metadata, [blockIndex](const auto& block) {
    return block.blockIndex_ == blockIndex;
  });

  if (it == metadata.end()) {
    return std::nullopt;
  }

  const auto& blockMetadata = *it;

  // Read compressed columns from file.
  CompressedBlock compressedBlock;
  if (!blockMetadata.offsetsAndCompressedSize_.has_value()) {
    // Block has no data on disk (shouldn't happen for delta files).
    return std::nullopt;
  }

  const auto& offsets = blockMetadata.offsetsAndCompressedSize_.value();
  compressedBlock.reserve(columns.size());

  for (auto columnIndex : columns) {
    if (static_cast<size_t>(columnIndex) >= offsets.size()) {
      // Column not present in this block.
      compressedBlock.emplace_back();
      continue;
    }

    const auto& offsetAndSize = offsets[columnIndex];
    std::vector<char> compressedColumn(offsetAndSize.compressedSize_);

    file.seek(offsetAndSize.offsetInFile_, SEEK_SET);
    file.read(compressedColumn.data(), compressedColumn.size());
    compressedBlock.push_back(std::move(compressedColumn));
  }

  // Decompress the block.
  IdTable result{columns.size(), allocator};
  result.resize(blockMetadata.numRows_);

  for (size_t i = 0; i < compressedBlock.size(); ++i) {
    auto col = result.getColumn(i);
    // Decompress column using ZstdWrapper.
    auto numBytesRead = ZstdWrapper::decompressToBuffer(
        compressedBlock[i].data(), compressedBlock[i].size(), col.data(),
        blockMetadata.numRows_ * sizeof(Id));
    AD_CORRECTNESS_CHECK(numBytesRead == blockMetadata.numRows_ * sizeof(Id));
  }

  return result;
}

// ____________________________________________________________________________
std::optional<IdTable> OnDiskDeltaTriples::readInsertBlock(
    PermutationEnum permutation, size_t blockIndex,
    CompressedRelationReader::ColumnIndicesRef columns,
    const CompressedRelationReader::Allocator& allocator) const {
  const auto& perm = permutations_[static_cast<size_t>(permutation)];
  if (perm.insertsMetadata_.empty()) {
    return std::nullopt;
  }

  std::string path = getDeltaInsertsPath(
      baseDir_, static_cast<Permutation::Enum>(permutation));
  auto& file = getFile(permutation, true, path);
  return readBlockFromFile(perm.insertsMetadata_, file, blockIndex, columns,
                           allocator);
}

// ____________________________________________________________________________
std::optional<IdTable> OnDiskDeltaTriples::readDeleteBlock(
    PermutationEnum permutation, size_t blockIndex,
    CompressedRelationReader::ColumnIndicesRef columns,
    const CompressedRelationReader::Allocator& allocator) const {
  const auto& perm = permutations_[static_cast<size_t>(permutation)];
  if (perm.deletesMetadata_.empty()) {
    return std::nullopt;
  }

  std::string path = getDeltaDeletesPath(
      baseDir_, static_cast<Permutation::Enum>(permutation));
  auto& file = getFile(permutation, false, path);
  return readBlockFromFile(perm.deletesMetadata_, file, blockIndex, columns,
                           allocator);
}

// ____________________________________________________________________________
void OnDiskDeltaTriples::loadFromDisk() {
  for (auto permutation : Permutation::ALL) {
    auto& perm = permutations_[static_cast<size_t>(permutation)];

    // Try to load insert metadata.
    std::string insertsPath = getDeltaInsertsPath(baseDir_, permutation);
    try {
      ad_utility::serialization::FileReadSerializer insertsSerializer{
          insertsPath};
      insertsSerializer >> perm.insertsMetadata_;
    } catch (...) {
      // File doesn't exist or is corrupted, leave metadata empty.
      perm.insertsMetadata_.clear();
    }

    // Try to load delete metadata.
    std::string deletesPath = getDeltaDeletesPath(baseDir_, permutation);
    try {
      ad_utility::serialization::FileReadSerializer deletesSerializer{
          deletesPath};
      deletesSerializer >> perm.deletesMetadata_;
    } catch (...) {
      // File doesn't exist or is corrupted, leave metadata empty.
      perm.deletesMetadata_.clear();
    }
  }
}

// ____________________________________________________________________________
void OnDiskDeltaTriples::deleteFiles() {
  // Close all open files first.
  for (auto& perm : permutations_) {
    perm.insertsFile_.reset();
    perm.deletesFile_.reset();
    perm.insertsMetadata_.clear();
    perm.deletesMetadata_.clear();
  }

  // Delete files for all permutations.
  for (auto permutation : Permutation::ALL) {
    std::string insertsPath = getDeltaInsertsPath(baseDir_, permutation);
    std::string deletesPath = getDeltaDeletesPath(baseDir_, permutation);

    ad_utility::deleteFile(insertsPath, false);  // Don't throw if not exists.
    ad_utility::deleteFile(deletesPath, false);
  }
}
