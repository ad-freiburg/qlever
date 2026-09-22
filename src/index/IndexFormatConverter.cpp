// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/IndexFormatConverter.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "backports/filesystem.h"
#include "backports/keywords.h"
#include "engine/idTable/IdTable.h"
#include "global/Constants.h"
#include "global/FileSuffixConstants.h"
#include "global/MaterializedViewConstants.h"
#include "index/CompressedRelationWriter.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/DeltaTriples.h"
#include "index/Index.h"
#include "index/IndexFormatVersion.h"
#include "index/IndexImpl.h"
#include "index/IndexMetaData.h"
#include "index/IndexSwap.h"
#include "index/LocalVocab.h"
#include "index/PatternCreator.h"
#include "index/Permutation.h"
#include "index/ScanSpecification.h"
#include "util/Algorithm.h"
#include "util/AllocatorWithLimit.h"
#include "util/BitUtils.h"
#include "util/CancellationHandle.h"
#include "util/CompactStringVector.h"
#include "util/Exception.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/File.h"
#include "util/FilesystemHelpers.h"
#include "util/InputRangeUtils.h"
#include "util/Log.h"
#include "util/NBitInteger.h"
#include "util/ProgressBar.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/json.h"

namespace qlever::indexFormatConverter {

namespace {

namespace fs = ql::filesystem;

// Return a human-readable representation of the given index format `version`.
std::string versionAsString(const IndexFormatVersion& version) {
  return absl::StrCat("PR = ", version.prNumber_,
                      ", Date = ", version.date_.toStringAndType().first);
}

// The source format's `Id` bit layout (see `global/ValueId.h` as of
// `qlever::previousIndexFormatVersion`): 4 datatype bits + a 60-bit payload
// in one 64-bit word. Datatype numbering is unchanged from the target
// format; only the payload width (and, for a few datatypes, its encoding)
// differs -- see `convert()` below.
struct LegacyId {
  static constexpr uint8_t numDatatypeBits = 4;
  static constexpr uint8_t numDataBits = 64 - numDatatypeBits;
  static constexpr uint64_t payloadMask =
      ad_utility::bitMaskForLowerBits(numDataBits);

  uint64_t bits_;

  uint8_t datatypeBits() const noexcept {
    return static_cast<uint8_t>(bits_ >> numDataBits);
  }
  uint64_t payload() const noexcept { return bits_ & payloadMask; }

  // Convert to the target format's `Id`. Throws on an invalid datatype or on
  // `LocalVocabIndex` (never valid on disk, see `convertId` in the header).
  // Most datatypes just zero-extend the payload into the wider field. Three
  // need actual re-encoding, because the source format packs their value
  // differently depending on the payload width: `Double` stores the
  // IEEE-754 bits shifted right by `numDatatypeBits` (shifting back left
  // restores them); `Int` uses a 60-bit two's complement encoding
  // (`NBitInteger<60>`), decoded and re-encoded via `Id::makeFromInt`;
  // `EncodedVal` (`EncodedIriManager.h`) uses the entire payload width like
  // `Double` and is shifted the same way.
  Id convert() const {
    auto datatypeBits = this->datatypeBits();
    if (datatypeBits > static_cast<uint8_t>(Datatype::MaxValue)) {
      throw std::runtime_error{absl::StrCat(
          "Encountered an `Id` with the invalid datatype ",
          static_cast<int>(datatypeBits),
          ", the index that is converted is corrupted")};
    }
    auto datatype = static_cast<Datatype>(datatypeBits);
    if (datatype == Datatype::LocalVocabIndex) {
      throw std::runtime_error{
          "Encountered an `Id` of type `LocalVocabIndex`, which must never be "
          "stored on disk (it holds a pointer into the memory of the process "
          "that created it), so the index that is converted is corrupted"};
    }
    if (datatype == Datatype::Double) {
      return Id::fromBits(
          {static_cast<uint8_t>(datatype), payload() << numDatatypeBits});
    }
    if (datatype == Datatype::Int) {
      return Id::makeFromInt(
          ad_utility::NBitInteger<numDataBits>::fromNBit(payload()));
    }
    if (datatype == Datatype::EncodedVal) {
      return Id::fromBits(
          {static_cast<uint8_t>(datatype), payload() << numDatatypeBits});
    }
    return Id::fromBits({static_cast<uint8_t>(datatype), payload()});
  }

  template <typename T>
  friend std::true_type allowTrivialSerialization(LegacyId, T);
};

// Mirrors `CompressedBlockMetadata::PermutedTriple` with `LegacyId` in place
// of `Id`, so the generic `ad_utility::serialization` framework can read it
// from a source-format permutation file; converted via `convert()` on read.
struct LegacyPermutedTriple {
  LegacyId col0Id_;
  LegacyId col1Id_;
  LegacyId col2Id_;
  LegacyId graphId_;

  template <typename T>
  friend std::true_type allowTrivialSerialization(LegacyPermutedTriple, T);
};

// Mirrors `CompressedBlockMetadata`, field for field, with
// `LegacyPermutedTriple`/`LegacyId` in place of `PermutedTriple`/`Id`, so the
// same (de)serialization code parses a source-format permutation's per-block
// metadata correctly. `OffsetAndCompressedSize` is reused as is: it stores a
// file offset and byte count, neither of which depends on the width of `Id`.
struct LegacyCompressedBlockMetadata {
  std::optional<std::vector<CompressedBlockMetadata::OffsetAndCompressedSize>>
      offsetsAndCompressedSize_;
  size_t numRows_;
  LegacyPermutedTriple firstTriple_;
  LegacyPermutedTriple lastTriple_;
  std::optional<std::vector<LegacyId>> graphInfo_;
  bool containsDuplicatesWithDifferentGraphs_;
  size_t blockIndex_;
};

// Field order must stay in lockstep with `CompressedBlockMetadata`'s own
// serialization (see `CompressedRelation.h`).
AD_SERIALIZE_FUNCTION(LegacyCompressedBlockMetadata) {
  if constexpr (ad_utility::serialization::WriteSerializer<S>) {
    AD_CORRECTNESS_CHECK(arg.offsetsAndCompressedSize_.has_value());
  } else {
    static_assert(ad_utility::serialization::ReadSerializer<S>);
    arg.offsetsAndCompressedSize_.emplace();
  }
  serializer | arg.offsetsAndCompressedSize_.value();
  serializer | arg.numRows_;
  serializer | arg.firstTriple_;
  serializer | arg.lastTriple_;
  serializer | arg.graphInfo_;
  serializer | arg.containsDuplicatesWithDifferentGraphs_;
  serializer | arg.blockIndex_;
}

// Everything needed from a source-format permutation's per-block metadata,
// read from its trailing metadata blob (mirrors
// `IndexMetaData::appendToFile`/`readFromFile`). `firstTriple_`/
// `lastTriple_`/`totalElements_` are already converted, used only by
// `verifyConvertedPermutation`'s sanity check. The per-relation `.meta` file
// is deliberately never read: `writePermutation` recomputes it from the
// converted row data.
struct LegacyPermutationSummary {
  size_t numColumns_;
  size_t totalElements_;
  CompressedBlockMetadata::PermutedTriple firstTriple_;
  CompressedBlockMetadata::PermutedTriple lastTriple_;
  std::vector<LegacyCompressedBlockMetadata> blocks_;
};

// Read the `LegacyPermutationSummary` of the permutation file `filename` (in
// the source format).
LegacyPermutationSummary readLegacyPermutationSummary(
    const std::string& filename) {
  ad_utility::File permutationFile{filename, "r"};
  auto [endOfMeta, startOfMeta] = permutationFile.getLastOffset();
  std::vector<char> buf(static_cast<size_t>(endOfMeta - startOfMeta));
  permutationFile.read(buf.data(), buf.size(), startOfMeta);
  ad_utility::serialization::ByteBufferReadSerializer serializer{
      std::move(buf)};

  // Mirrors `IndexMetaData`'s own `AD_SERIALIZE_FRIEND_FUNCTION` (see
  // `IndexMetaData.h`): same fields, same order, `LegacyCompressedBlockMetadata`
  // in place of `CompressedBlockMetadata`.
  uint64_t magicNumber;
  serializer | magicNumber;
  if (magicNumber != MAGIC_NUMBER_FOR_SERIALIZATION) {
    throw WrongFormatException{
        "The binary format of this index is not supported by this "
        "converter. Please rebuild the index."};
  }
  uint64_t version;
  serializer | version;
  if (version != V_CURRENT) {
    throw WrongFormatException{
        "The binary format of this index is not supported by this "
        "converter. Please rebuild the index."};
  }
  std::string name;
  serializer | name;
  std::vector<LegacyCompressedBlockMetadata> blocks;
  serializer | blocks;
  [[maybe_unused]] off_t offsetAfter;
  serializer | offsetAfter;
  size_t totalElements;
  serializer | totalElements;
  [[maybe_unused]] size_t numDistinctCol0;
  serializer | numDistinctCol0;

  auto convertTriple = [](const LegacyPermutedTriple& triple) {
    return CompressedBlockMetadata::PermutedTriple{
        triple.col0Id_.convert(), triple.col1Id_.convert(),
        triple.col2Id_.convert(), triple.graphId_.convert()};
  };
  size_t numColumns =
      blocks.empty() ? NumColumnsIndexBuilding
                     : blocks.front().offsetsAndCompressedSize_->size();
  CompressedBlockMetadata::PermutedTriple firstTriple{};
  CompressedBlockMetadata::PermutedTriple lastTriple{};
  if (!blocks.empty()) {
    firstTriple = convertTriple(blocks.front().firstTriple_);
    lastTriple = convertTriple(blocks.back().lastTriple_);
  }
  return {numColumns, totalElements, firstTriple, lastTriple,
         std::move(blocks)};
}

// Read, decompress, and convert the column at `offset` (which holds `numRows`
// `Id`s in the source format) from `file`.
std::vector<Id> readAndConvertLegacyColumn(
    const ad_utility::File& file,
    const CompressedBlockMetadata::OffsetAndCompressedSize& offset,
    size_t numRows) {
  std::vector<char> compressed(offset.compressedSize_);
  file.read(compressed.data(), offset.compressedSize_, offset.offsetInFile_);
  std::vector<LegacyId> legacyIds(numRows);
  auto numBytesRead = ZstdWrapper::decompressToBuffer(
      compressed.data(), compressed.size(), legacyIds.data(),
      numRows * sizeof(LegacyId));
  AD_CORRECTNESS_CHECK(numBytesRead == numRows * sizeof(LegacyId));
  std::vector<Id> result;
  result.reserve(numRows);
  for (const auto& legacyId : legacyIds) {
    result.push_back(legacyId.convert());
  }
  return result;
}

// Counterpart of `scanAndConvertIds` for a source-format permutation: a lazy
// range converting one block at a time. `summary` is taken by reference, not
// by value: the per-block transform only reads a block, and both call sites
// need `summary` again afterward (for `verifyConvertedPermutation`), so a
// copy of `summary.blocks_` would be wasted. Safe because both callers
// consume the returned range synchronously via `writePermutation` before
// touching `summary` again.
ad_utility::InputRangeTypeErased<IdTableStatic<0>> legacyScanAndConvert(
    std::string filename, const LegacyPermutationSummary& summary,
    std::function<void(size_t)> progress) {
  auto transformed =
      summary.blocks_ |
      ql::views::transform(
          [filename = std::move(filename), numColumns = summary.numColumns_,
           progress = std::move(progress)](
              const LegacyCompressedBlockMetadata& block) {
            ad_utility::File file{filename, "r"};
            IdTableStatic<0> table{numColumns,
                                   ad_utility::makeUnlimitedAllocator<Id>()};
            table.resize(block.numRows_);
            const auto& offsets = block.offsetsAndCompressedSize_.value();
            for (size_t col = 0; col < numColumns; ++col) {
              auto ids =
                  readAndConvertLegacyColumn(file, offsets.at(col), block.numRows_);
              ql::ranges::copy(ids, table.getColumn(col).begin());
            }
            progress(block.numRows_);
            return table;
          });
  return ad_utility::InputRangeTypeErased{std::move(transformed)};
}

// The `MATERIALIZED_VIEWS_VERSION` of a source-format index's views: this
// transition does not change a view's own metadata format (only the `Id`s in
// its permutation, see `LegacyId` above), so it's already the current one.
constexpr size_t materializedViewsVersionOfSourceFormat =
    MATERIALIZED_VIEWS_VERSION;

// The permutations of an index, as pairs of "twins" (like `PSO` and `POS`),
// together with the information whether the pair is the pair of internal
// permutations. The multiplicities of the last column of a permutation are
// stored in the metadata of its twin (see
// `IndexMetaData::exchangeMultiplicities`), which is why the permutations have
// to be converted pairwise.
using PermutationPair = std::pair<Permutation::Enum, Permutation::Enum>;
const std::array<std::pair<PermutationPair, bool>, 4> permutationPairs{
    std::pair{PermutationPair{Permutation::PSO, Permutation::POS}, false},
    std::pair{PermutationPair{Permutation::PSO, Permutation::POS}, true},
    std::pair{PermutationPair{Permutation::SPO, Permutation::SOP}, false},
    std::pair{PermutationPair{Permutation::OPS, Permutation::OSP}, false}};

// Return the base name that the permutation files of the index with the base
// name `basename` share. The internal permutations have an additional infix.
std::string basenameForPermutations(std::string_view basename,
                                    bool isInternal) {
  return absl::StrCat(basename, isInternal ? QLEVER_INTERNAL_INDEX_INFIX : "");
}

// Return the name of the file that stores the given `permutation` of the index
// with the base name `basename`.
std::string filenameForPermutation(std::string_view basename,
                                   const Permutation& permutation,
                                   bool isInternal) {
  return absl::StrCat(basenameForPermutations(basename, isInternal),
                      PERMUTATION_FILE_INFIX, permutation.fileSuffix());
}

// The key of the index format version in the configuration of an index. NOTE:
// This is a `const char*` and not a `std::string_view`, because the latter
// cannot be used to look up a key in a `nlohmann::json` object.
constexpr const char* indexFormatVersionKey = "index-format-version";

// Check that the source and the target format of this converter (see
// `sourceVersion` and `targetVersion`) still are the previous resp. the current
// index format. If they are not, then the index format has changed again and
// this converter has to be updated (see the note at
// `qlever::indexFormatVersion`), so this is a programming error and not
// something that a user can fix.
void checkThatTheSupportedFormatsAreUpToDate() {
  AD_CORRECTNESS_CHECK(
      targetVersion == indexFormatVersion,
      "The index converter converts to the index format ",
      versionAsString(targetVersion), ", but the current index format is ",
      versionAsString(indexFormatVersion),
      ". The converter has to be updated to the current index format.");
  AD_CORRECTNESS_CHECK(
      sourceVersion == previousIndexFormatVersion,
      "The index converter converts from the index format ",
      versionAsString(sourceVersion),
      ", but the index format that precedes the current one is ",
      versionAsString(previousIndexFormatVersion),
      ". The converter has to be updated to that index format.");
}

// Read the configuration of the index with the base name `basename`, and check
// that it is in the source format (see the documentation of
// `convertIndexToCurrentFormat`).
nlohmann::json readAndCheckConfiguration(const std::string& basename) {
  std::string filename = absl::StrCat(basename, CONFIGURATION_FILE);
  if (!fs::exists(filename)) {
    throw std::runtime_error{absl::StrCat(
        "The file \"", filename, "\" does not exist, so \"", basename,
        "\" is not the base name of a QLever index. Note that the base name "
        "includes the name of the index, for example `index-dir/wikidata`.")};
  }
  nlohmann::json configuration;
  ad_utility::makeIfstream(filename) >> configuration;

  if (!configuration.contains(indexFormatVersionKey)) {
    throw std::runtime_error{absl::StrCat(
        "The index \"", basename,
        "\" was built before versioning was introduced for QLever's index "
        "format, it is much too old to be converted. Please rebuild it.")};
  }
  auto version =
      configuration.at(indexFormatVersionKey).get<IndexFormatVersion>();
  if (version == targetVersion) {
    throw std::runtime_error{absl::StrCat(
        "The index \"", basename, "\" already is in the current index format (",
        versionAsString(version), "), so there is nothing to convert.")};
  }
  if (version != sourceVersion) {
    throw std::runtime_error{absl::StrCat(
        "The index \"", basename, "\" is in the index format (",
        versionAsString(version),
        "), but this converter only converts indexes in the format (",
        versionAsString(sourceVersion), ") to the format (",
        versionAsString(targetVersion), "). Please rebuild the index.")};
  }
  return configuration;
}

// Throw if the index with the base name `basename` has persisted updates. Those
// contain `Id`s as well, but converting them is deliberately not supported (see
// the documentation of `convertIndexToCurrentFormat`).
void throwIfPersistedUpdatesExist(const std::string& basename) {
  std::string filename = absl::StrCat(basename, UPDATE_TRIPLES_SUFFIX);
  if (fs::exists(filename)) {
    throw std::runtime_error{absl::StrCat(
        "The index \"", basename, "\" has persisted updates (the file \"",
        filename,
        "\"), which this converter does not convert. Please either materialize "
        "them into the index (by rebuilding it) or delete the file (which "
        "discards the updates) before converting the index.")};
  }
}

// Return a callback for the given `progressBar`, which reports that `numSteps`
// steps have been processed and displays an update when one is due. The
// callback is threadsafe (see `ConcurrentProgressBar`), which matters because
// the two permutations of a pair are converted concurrently, see
// `convertPermutations` below.
//
// NOTE: The rebuild reports its progress in exactly the same way, see
// `IndexRebuilder.cpp`.
std::function<void(size_t)> progressCallbackFor(
    ad_utility::ConcurrentProgressBar& progressBar) {
  return [&progressBar](size_t numSteps) {
    progressBar.add(numSteps);
    if (auto update = progressBar.update()) {
      AD_LOG_INFO << update->getProgressString() << std::flush;
    }
  };
}

// The batch size for a progress bar with the given `total`, chosen such that
// about 1000 progress lines are written, that is, one line per mille of the
// total. Each line costs a formatted string and a lock, which is nothing
// compared to converting a per mille of an index. The lower bound keeps tiny
// indexes (in particular, those of the unit tests) from producing a progress
// line for almost every block.
//
// NOTE: The rebuild computes its batch size in the same way, but with about 50
// lines per phase (`IndexRebuilder.cpp`).
size_t batchSizeFor(size_t total) {
  return std::max<size_t>(total / 1000, 100'000);
}

// Write the given `blocks` as a single permutation to the file `filename`, and
// return its metadata. The metadata is not yet written to disk, because it is
// only complete once the multiplicities have been exchanged with the twin
// permutation (see `permutationPairs` above).
//
// NOTE: The block size of the permutation is not stored in an index, so the
// converted permutation uses the default, exactly like a freshly built index
// (`IndexImpl::blocksizePermutationPerColumn_`, which nothing but a unit test
// ever changes, and correspondingly `blocksizeOfConvertedPermutations` here).
// The blocks of the converted permutation may therefore differ from the blocks
// of the permutation that it was converted from, which is irrelevant for its
// content, but not for its metadata: a relation that is large enough to occupy
// blocks of its own in the permutation that is converted can be small enough to
// share a block with other relations in the converted permutation. Such a
// relation has no `CompressedRelationMetadata` of its own anymore, that
// metadata is derived from its block instead (see
// `CompressedRelationReader::getMetadataForSmallRelation`). The number of
// blocks, the `numRows_` and the multiplicities of the converted permutation
// can therefore differ from those of the permutation that it was converted
// from; they are exactly those that a freshly built index would have.
IndexMetaData writePermutation(
    const std::string& filename, size_t numColumns,
    ad_utility::InputRangeTypeErased<IdTableStatic<0>> blocks) {
  auto writer = std::make_unique<CompressedRelationWriter>(
      numColumns, ad_utility::File{filename, "w"},
      blocksizeOfConvertedPermutations());
  IndexMetaData metaData;
  auto callback =
      [&metaData](ql::span<const CompressedRelationMetadata> metadata) {
        for (const auto& relationMetadata : metadata) {
          metaData.add(relationMetadata);
        }
      };
  // The blocks already are in the correct order (the conversion does not
  // change the order of the `Id`s, see `LegacyId::convert` above), so the
  // identity is the correct key order here.
  auto [numDistinctCol0, blockMetadata] =
      CompressedRelationWriter::createPermutation(
          {std::move(writer), callback}, std::move(blocks),
          KeyOrder{0, 1, 2, 3}, {}, /* showProgressBar = */ false);
  metaData.blockData() = std::move(blockMetadata);
  metaData.calculateStatistics(numDistinctCol0);
  return metaData;
}

// Append the `metaData` of the permutation that was written to `filename` to
// that file, and to the corresponding metadata file.
void writeMetaData(IndexMetaData& metaData, const std::string& filename) {
  ad_utility::File permutationFile{filename, "r+"};
  ad_utility::File metaFile{absl::StrCat(filename, META_FILE_SUFFIX), "w"};
  metaData.appendToFile(permutationFile, metaFile);
}

// Check that the permutation that was written (`newMetaData`) has the same
// content as the permutation that it was converted from (`oldSummary`). Only
// the number of triples and the first and last triple are compared, which is
// cheap because it only looks at the metadata that is in memory anyway.
// `oldSummary`'s `firstTriple_`/`lastTriple_` are already converted (see
// `readLegacyPermutationSummary`), so they are compared to `newMetaData`'s
// directly, without calling `convertId` again.
void verifyConvertedPermutation(const LegacyPermutationSummary& oldSummary,
                                const IndexMetaData& newMetaData,
                                const std::string& filename) {
  // NOTE: This can only fail if the converter itself is broken, hence a
  // correctness check and not an exception with a user-facing message.
  auto check = [&filename](bool condition) {
    AD_CORRECTNESS_CHECK(
        condition, "The converted permutation \"", filename,
        "\" does not have the same content as the permutation it was converted "
        "from. The converted index is incomplete and has to be deleted.");
  };
  check(oldSummary.totalElements_ == newMetaData.totalElements());
  const auto& newBlocks = newMetaData.blockData();
  check(oldSummary.blocks_.empty() == newBlocks.empty());
  if (oldSummary.blocks_.empty()) {
    return;
  }
  check(oldSummary.firstTriple_ == newBlocks.front().firstTriple_);
  check(oldSummary.lastTriple_ == newBlocks.back().lastTriple_);
}

// Whether the index with the base name `basename` has the given permutation.
// An index built with `--only-pso-and-pos-permutations` has neither `SPO` and
// `SOP` nor `OPS` and `OSP`.
bool hasPermutation(const std::string& basename,
                    Permutation::Enum permutationEnum, bool isInternal) {
  Permutation permutation{permutationEnum,
                          ad_utility::makeUnlimitedAllocator<Id>()};
  return fs::exists(filenameForPermutation(basename, permutation, isInternal));
}

// A permutation of the index in the source format, as loaded by
// `loadPermutation` below: enough to convert it (`summary_`, plus `filename_`
// for `legacyScanAndConvert` to reread and decompress its actual row data) and
// to name the corresponding file of the converted index (`permutationEnum_`).
struct LoadedLegacyPermutation {
  Permutation::Enum permutationEnum_;
  std::string filename_;
  LegacyPermutationSummary summary_;
};

// Load the given permutation of the index with the base name `basename` from
// disk (in the source format). Return `nullopt` if it does not exist.
std::optional<LoadedLegacyPermutation> loadPermutation(
    const std::string& basename, Permutation::Enum permutationEnum,
    bool isInternal) {
  if (!hasPermutation(basename, permutationEnum, isInternal)) {
    return std::nullopt;
  }
  Permutation dummy{permutationEnum, ad_utility::makeUnlimitedAllocator<Id>()};
  std::string filename = filenameForPermutation(basename, dummy, isInternal);
  return LoadedLegacyPermutation{permutationEnum, filename,
                                 readLegacyPermutationSummary(filename)};
}

// The number of normal and of internal permutations that the index with the
// base name `basename` has, in the same way in which `convertPermutations`
// below determines which permutations it converts.
Index::NumNormalAndInternal numPermutationsOfIndex(
    const std::string& basename) {
  Index::NumNormalAndInternal numPermutations{};
  for (const auto& permutationPair : permutationPairs) {
    auto [enumA, enumB] = permutationPair.first;
    bool isInternal = permutationPair.second;
    for (auto permutationEnum : {enumA, enumB}) {
      if (hasPermutation(basename, permutationEnum, isInternal)) {
        ++(isInternal ? numPermutations.internal : numPermutations.normal);
      }
    }
  }
  return numPermutations;
}

// Convert a single permutation of an index and write the result to the index
// with the base name `newBasename`. Return the metadata of the new permutation,
// which still has to be written to disk, see `writePermutation` above.
//
// NOTE: The two permutations of a pair are converted concurrently (see
// `convertPermutations` below), so this must not touch any state that is
// shared between them. That is why the files of the old permutation are
// recorded by the caller and not here, and why the progress is reported to a
// threadsafe `progress` callback instead of being logged here.
IndexMetaData convertPermutation(const LoadedLegacyPermutation& oldPermutation,
                                 const std::string& newBasename,
                                 bool isInternal,
                                 const std::function<void(size_t)>& progress) {
  Permutation dummy{oldPermutation.permutationEnum_,
                    ad_utility::makeUnlimitedAllocator<Id>()};
  std::string newFilename =
      filenameForPermutation(newBasename, dummy, isInternal);
  auto newMetaData = writePermutation(
      newFilename, oldPermutation.summary_.numColumns_,
      legacyScanAndConvert(oldPermutation.filename_, oldPermutation.summary_,
                          progress));
  newMetaData.setName(
      std::string{Permutation::toString(oldPermutation.permutationEnum_)});
  verifyConvertedPermutation(oldPermutation.summary_, newMetaData,
                             newFilename);
  return newMetaData;
}

// Convert all permutations of the index with the base name `oldBasename` and
// write them to the index with the base name `newBasename`. The `numTriples`
// are the numbers of triples from the configuration of that index, which are
// the total for the progress bar below.
void convertPermutations(const std::string& oldBasename,
                         const std::string& newBasename,
                         const Index::NumNormalAndInternal& numTriples,
                         std::vector<fs::path>& handledFiles) {
  // Each triple is written once per permutation, which gives the total number
  // of triples that the conversion of the permutations writes.
  auto numPermutations = numPermutationsOfIndex(oldBasename);
  size_t numTriplesTotal = numPermutations.normal * numTriples.normal +
                           numPermutations.internal * numTriples.internal;
  AD_LOG_INFO << "Converting " << numPermutations.normalAndInternal_()
              << " permutations (" << numPermutations.normal << " normal and "
              << numPermutations.internal << " internal, "
              << ad_utility::withThousandSeparators(numTriplesTotal)
              << " triples in total) ..." << std::endl;
  ad_utility::ConcurrentProgressBar progressBar{
      "Triples converted: ", numTriplesTotal, batchSizeFor(numTriplesTotal)};
  auto progress = progressCallbackFor(progressBar);

  for (const auto& permutationPair : permutationPairs) {
    auto [enumA, enumB] = permutationPair.first;
    // Whether the pair is the pair of internal permutations.
    //
    // NOTE: Deliberately not part of a structured binding, because it is
    // captured by the lambda below, which is only valid in C++20.
    bool isInternal = permutationPair.second;
    auto permutationA = loadPermutation(oldBasename, enumA, isInternal);
    auto permutationB = loadPermutation(oldBasename, enumB, isInternal);
    if (!permutationA.has_value() && !permutationB.has_value()) {
      // The index does not have this pair of permutations at all, which is the
      // case for `SPO`, `SOP`, `OPS`, and `OSP` if the index was built with
      // `--only-pso-and-pos-permutations`.
      continue;
    }
    if (!permutationA.has_value() || !permutationB.has_value()) {
      throw std::runtime_error{absl::StrCat(
          "The index \"", oldBasename, "\" has only one of the permutations ",
          Permutation::toString(enumA), " and ", Permutation::toString(enumB),
          ", so it is incomplete and cannot be converted")};
    }
    // Record the files of the two old permutations (see
    // `checkAllFilesWereHandled`). This happens here and not in
    // `convertPermutation`, because `handledFiles` is shared between the two
    // conversions below, which run concurrently.
    auto recordOldFiles = [&handledFiles](const LoadedLegacyPermutation& permutation) {
      handledFiles.emplace_back(permutation.filename_);
      handledFiles.emplace_back(
          absl::StrCat(permutation.filename_, META_FILE_SUFFIX));
    };
    recordOldFiles(*permutationA);
    recordOldFiles(*permutationB);

    // Convert the two permutations of the pair concurrently. They are
    // independent of each other (each has its own reader, its own writer, and
    // its own metadata), and a single conversion uses only few threads
    // (`lazy-index-scan-num-threads` for reading and
    // `permutation-writer-num-threads` for writing), so there are cores to
    // spare. One of the two conversions runs on this thread, so that only one
    // additional thread is needed.
    //
    // NOTE: If the conversion on this thread throws, the destructor of
    // `futureB` waits for the other conversion to finish before the exception
    // leaves this function. That is exactly what we want: no thread must still
    // be writing to the incomplete index when the caller handles the error.
    auto convert = [&newBasename, isInternal,
                    &progress](const LoadedLegacyPermutation& permutation) {
      return convertPermutation(permutation, newBasename, isInternal, progress);
    };
    auto futureB =
        std::async(std::launch::async, convert, std::cref(*permutationB));
    auto newMetaA = convert(*permutationA);
    auto newMetaB = futureB.get();
    // The multiplicities of the last column of a permutation are stored in the
    // metadata of its twin, so they have to be exchanged before the metadata is
    // written.
    newMetaA.exchangeMultiplicities(newMetaB);
    Permutation dummyA{permutationA->permutationEnum_,
                       ad_utility::makeUnlimitedAllocator<Id>()};
    Permutation dummyB{permutationB->permutationEnum_,
                       ad_utility::makeUnlimitedAllocator<Id>()};
    writeMetaData(newMetaA,
                  filenameForPermutation(newBasename, dummyA, isInternal));
    writeMetaData(newMetaB,
                  filenameForPermutation(newBasename, dummyB, isInternal));
  }
  progressBar.logFinalProgressString();
}

// Convert the patterns of the index with the base name `oldBasename` (if it has
// them) and write them to the index with the base name `newBasename`.
//
// NOTE: `PatternCreator::readPatternsFromFile` is hardcoded to
// `CompactVectorOfStrings<Id>`, so the patterns file is read directly
// instead, using `CompactVectorOfStrings<LegacyId>`.
void convertPatterns(const std::string& oldBasename,
                     const std::string& newBasename,
                     std::vector<fs::path>& handledFiles) {
  std::string oldFilename = absl::StrCat(oldBasename, PATTERNS_FILE_SUFFIX);
  if (!fs::exists(oldFilename)) {
    return;
  }
  handledFiles.emplace_back(oldFilename);
  AD_LOG_INFO << "Converting the patterns ..." << std::endl;
  PatternStatistics statistics;
  CompactVectorOfStrings<LegacyId> legacyPatterns;
  ad_utility::serialization::FileReadSerializer patternReader{oldFilename};
  patternReader >> statistics;
  patternReader >> legacyPatterns;

  std::vector<std::vector<Id>> converted;
  converted.reserve(legacyPatterns.size());
  for (auto pattern : legacyPatterns) {
    std::vector<Id> convertedPattern;
    convertedPattern.reserve(pattern.size());
    for (const auto& legacyId : pattern) {
      convertedPattern.push_back(legacyId.convert());
    }
    converted.push_back(std::move(convertedPattern));
  }
  PatternCreator::writePatternsToFile(
      absl::StrCat(newBasename, PATTERNS_FILE_SUFFIX),
      CompactVectorOfStrings<Id>{converted}, statistics);
}

// Convert the materialized view with the given `name` of the index with the
// base name `oldBasename` and write it to the index with the base name
// `newBasename`.
void convertMaterializedView(const std::string& oldBasename,
                             const std::string& newBasename,
                             const std::string& name) {
  AD_LOG_INFO << "Converting the materialized view \"" << name << "\" ..."
              << std::endl;
  std::string oldViewBasename = materializedViewFilenameBase(oldBasename, name);
  std::string newViewBasename = materializedViewFilenameBase(newBasename, name);

  // Convert the permutation of the view. A view always is a single `SPO`
  // permutation and never has a twin, so unlike the permutations of the index
  // itself, its multiplicities are not exchanged.
  std::string oldFilename = absl::StrCat(oldViewBasename, VIEW_SPO_SUFFIX);
  auto oldSummary = readLegacyPermutationSummary(oldFilename);
  std::string newFilename = absl::StrCat(newViewBasename, VIEW_SPO_SUFFIX);
  ad_utility::ConcurrentProgressBar progressBar{
      "Triples converted: ", oldSummary.totalElements_,
      batchSizeFor(oldSummary.totalElements_)};
  auto newMetaData = writePermutation(
      newFilename, oldSummary.numColumns_,
      legacyScanAndConvert(oldFilename, oldSummary,
                          progressCallbackFor(progressBar)));
  progressBar.logFinalProgressString();
  newMetaData.setName(newViewBasename);
  verifyConvertedPermutation(oldSummary, newMetaData, newFilename);
  writeMetaData(newMetaData, newFilename);

  // Copy the metadata of the view, with the version of the on-disk format of
  // the views raised to the current one (an unconverted view has to be rejected
  // by the engine, see `MATERIALIZED_VIEWS_VERSION`). The view that is read has
  // to be in the version that belongs to the source format, see
  // `materializedViewsVersionOfSourceFormat`.
  nlohmann::json viewInfo;
  ad_utility::makeIfstream(absl::StrCat(oldViewBasename, VIEW_INFO_SUFFIX)) >>
      viewInfo;
  auto viewsVersion = viewInfo.at("version").get<size_t>();
  if (viewsVersion != materializedViewsVersionOfSourceFormat) {
    throw std::runtime_error{absl::StrCat(
        "The materialized view \"", name, "\" of the index \"", oldBasename,
        "\" is stored in the format version ", viewsVersion,
        ", but this converter only converts views in the format version ",
        materializedViewsVersionOfSourceFormat,
        ". Please delete the view and create it again after the conversion.")};
  }
  viewInfo["version"] = MATERIALIZED_VIEWS_VERSION;
  ad_utility::makeOfstream(absl::StrCat(newViewBasename, VIEW_INFO_SUFFIX))
      << viewInfo.dump() << std::endl;
}

// Convert all materialized views of the index with the base name `oldBasename`
// and write them to the index with the base name `newBasename`.
void convertMaterializedViews(const std::string& oldBasename,
                              const std::string& newBasename) {
  // Each view has exactly one info file, so the names of the views are exactly
  // the infixes of those files (`<basename>.view.<name><suffix>`).
  auto viewFiles =
      qlever::util::filesWithBaseNameAndSuffix(oldBasename, VIEW_FILE_INFIX);
  std::string prefix = materializedViewFilenameBase(oldBasename, "");
  std::vector<std::string> names;
  for (const auto& file : viewFiles) {
    std::string_view filename = file.native();
    if (!ql::ends_with(filename, VIEW_INFO_SUFFIX)) {
      continue;
    }
    AD_CORRECTNESS_CHECK(ql::starts_with(filename, prefix));
    filename.remove_prefix(prefix.size());
    filename.remove_suffix(VIEW_INFO_SUFFIX.size());
    names.emplace_back(filename);
  }
  // Every file of every view has to be handled by the conversion of that view,
  // else the converted view would be incomplete.
  size_t numExpectedFiles = names.size() * VIEW_ALL_SUFFIXES.size();
  if (viewFiles.size() != numExpectedFiles) {
    throw std::runtime_error{absl::StrCat(
        "The index \"", oldBasename, "\" has ", viewFiles.size(),
        " files that belong to a materialized view, but its ", names.size(),
        " views consist of ", numExpectedFiles,
        " files. Please delete the files that do not belong to a complete "
        "view, or delete the incomplete views.")};
  }
  for (const auto& name : names) {
    convertMaterializedView(oldBasename, newBasename, name);
  }
}

// Copy all files of the index with the base name `oldBasename` that need no
// conversion to the index with the base name `newBasename`. These are the files
// that contain no `Id`s at all: the vocabulary, the text index (which stores
// plain integers and reconstructs its `Id`s when it is read), the settings, and
// the state of the graph-name allocation.
void copyFilesThatNeedNoConversion(const std::string& oldBasename,
                                   const std::string& newBasename,
                                   std::vector<fs::path>& handledFiles) {
  auto copy = [&handledFiles, &oldBasename,
               &newBasename](const fs::path& oldFile) {
    std::string_view filename = oldFile.native();
    AD_CORRECTNESS_CHECK(ql::starts_with(filename, oldBasename));
    filename.remove_prefix(oldBasename.size());
    handledFiles.emplace_back(oldFile);
    fs::copy_file(oldFile, absl::StrCat(newBasename, filename));
  };
  AD_LOG_INFO << "Copying the files that need no conversion ..." << std::endl;
  for (auto suffix :
       {SETTINGS_FILE_SUFFIX, ALLOCATED_GRAPHS_SUFFIX, TEXT_INDEX_FILE_SUFFIX,
        TEXT_VOCAB_FILE_SUFFIX, TEXT_DOCS_DB_FILE_SUFFIX}) {
    fs::path oldFile = absl::StrCat(oldBasename, suffix);
    if (fs::exists(oldFile)) {
      copy(oldFile);
    }
  }
  // The set of files of the vocabulary depends on its type, so enumerate them
  // via their common prefix (the same mechanism as in
  // `IndexImpl::allIndexFiles`).
  ql::ranges::for_each(
      qlever::util::filesWithBaseNameAndSuffix(oldBasename, VOCAB_SUFFIX),
      copy);
}

// Check that every file of the index with the base name `oldBasename` was
// either converted or copied. This makes sure that a file type that is added to
// an index in the future is not silently dropped by this converter.
//
// NOTE: Such a file can only appear if `IndexImpl::allIndexFiles` was extended
// without extending this converter, hence a correctness check and not an
// exception with a user-facing message.
void checkAllFilesWereHandled(const std::string& oldBasename,
                              const std::vector<fs::path>& handledFiles) {
  std::vector<std::string> missingFiles;
  for (const auto& file : IndexImpl::allIndexFiles(oldBasename)) {
    if (!ad_utility::contains(handledFiles, file)) {
      missingFiles.push_back(file.native());
    }
  }
  AD_CORRECTNESS_CHECK(missingFiles.empty(),
                       "The following files of the index \"", oldBasename,
                       "\" were neither converted nor copied: ",
                       absl::StrJoin(missingFiles, ", "),
                       ". The index converter has to be extended for them. The "
                       "converted index is incomplete and has to be deleted.");
}

}  // namespace

// _____________________________________________________________________________
std::string conversionDescription() {
  return absl::StrCat(
      "Upgrade an index in the index format (", versionAsString(sourceVersion),
      ") to an index in the index format (", versionAsString(targetVersion),
      ") in place. The only difference between the two formats is the "
      "numbering of the datatypes of the IDs: the datatype for the words of a "
      "secondary vocabulary was inserted in the middle, which renumbered the "
      "datatypes after it. The IDs of the index are therefore rewritten, and "
      "nothing else changes.\n\nThe upgraded index is first written to the "
      "subdirectory `",
      stagingDirPrefix,
      "<current datetime>.tmp` and checked. Only then, the index in the old "
      "format is moved to the subdirectory `",
      retiredDirPrefix,
      "<datetime of its build>` and the upgraded index takes its "
      "place.\n\nNote that rebuilding the index from its input files is still "
      "the recommended way to move to a new index format, because it also "
      "profits from all improvements to the index building since the index "
      "was built.");
}

// _____________________________________________________________________________
Id convertId(uint64_t legacyBits) { return LegacyId{legacyBits}.convert(); }

// _____________________________________________________________________________
void convertIndexToCurrentFormat(const std::string& oldBasename,
                                 const std::string& newBasename) {
  AD_CONTRACT_CHECK(!oldBasename.empty() && !newBasename.empty(),
                    "The base names of the indexes must not be empty");
  // NOTE: This is a user-facing error and not a requirement violation, because
  // passing the same base name twice is an easy mistake to make on the command
  // line of `qlever-upgrade-index`.
  if (fs::path{oldBasename}.lexically_normal() ==
      fs::path{newBasename}.lexically_normal()) {
    throw std::runtime_error{
        "The base name of the converted index has to differ from the base "
        "name of the index that is converted, because the index that is "
        "converted is not modified"};
  }
  checkThatTheSupportedFormatsAreUpToDate();

  auto configuration = readAndCheckConfiguration(oldBasename);
  throwIfPersistedUpdatesExist(oldBasename);

  // The converted index must not overwrite any existing file.
  fs::path newDirectory = fs::path{newBasename}.parent_path();
  if (!newDirectory.empty()) {
    fs::create_directories(newDirectory);
  }
  if (qlever::util::doesDirectoryContainFileWithBasename(newBasename)) {
    throw std::runtime_error{absl::StrCat(
        "There already are files with the base name \"", newBasename,
        "\", but the converted index must not overwrite any of them. Please "
        "choose a base name that is not in use yet.")};
  }

  AD_LOG_INFO << "Converting the index \"" << oldBasename << "\" to \""
              << newBasename << "\" ..." << std::endl;

  // The files of the old index that have been converted or copied, see
  // `checkAllFilesWereHandled`. The configuration file is handled at the very
  // end, but is already listed here.
  std::vector<fs::path> handledFiles{
      absl::StrCat(oldBasename, CONFIGURATION_FILE)};

  convertPermutations(
      oldBasename, newBasename,
      static_cast<Index::NumNormalAndInternal>(configuration.at("num-triples")),
      handledFiles);
  convertPatterns(oldBasename, newBasename, handledFiles);
  copyFilesThatNeedNoConversion(oldBasename, newBasename, handledFiles);
  checkAllFilesWereHandled(oldBasename, handledFiles);
  convertMaterializedViews(oldBasename, newBasename);

  // Write the configuration last, with the version of the target format. An
  // index without its configuration file cannot be loaded at all, so if the
  // conversion is interrupted, the incomplete index is not mistaken for a
  // complete one.
  configuration[indexFormatVersionKey] = targetVersion;
  ad_utility::makeOfstream(absl::StrCat(newBasename, CONFIGURATION_FILE))
      << configuration.dump(4) << std::endl;

  AD_LOG_INFO << "Conversion of the index completed, the converted index is \""
              << newBasename << "\"" << std::endl;
}

namespace {
// Check that the upgraded index with the base name `newBasename` can be
// loaded, and that the number of triples of each of its permutations matches
// `expectedNumTriples`, the number of triples from the configuration of the
// index that was upgraded. The number of triples of a permutation comes from
// the metadata of that permutation (the configuration is copied unchanged by
// the conversion, so comparing configurations would check nothing), so a
// conversion bug that loses or duplicates rows of a permutation is caught
// here, before the upgraded index replaces the original one.
void checkUpgradedIndex(const std::string& newBasename,
                        const Index::NumNormalAndInternal& expectedNumTriples) {
  AD_LOG_INFO << "Checking that the upgraded index can be loaded ..."
              << std::endl;
  // Loading an index logs the vocabulary, every permutation, and the patterns,
  // and unloading it logs one more message. All of that is noise here, only a
  // warning or an error of the load is of interest.
  //
  // NOTE: Declared before `index`, so that it is destroyed after it and hence
  // also covers the message that the destruction of `index` logs.
  ad_utility::ScopedLogLevel scopedLogLevel{LogLevel::Enum::WARN};
  Index index{ad_utility::makeUnlimitedAllocator<Id>()};
  // Load what the index has: the patterns and the permutations beyond `PSO`
  // and `POS` are optional.
  index.usePatterns() =
      fs::exists(absl::StrCat(newBasename, PATTERNS_FILE_SUFFIX));
  index.loadAllPermutations() =
      fs::exists(absl::StrCat(newBasename, PERMUTATION_FILE_INFIX, ".ops"));
  index.createFromOnDiskIndex(newBasename, false);

  auto checkNumTriples = [](const Permutation& permutation, size_t expected) {
    size_t actual = permutation.numTriples();
    if (actual != expected) {
      throw std::runtime_error{absl::StrCat(
          "The ", permutation.readableName(),
          " permutation of the upgraded index has ", actual,
          " triples, but the index that was upgraded has ", expected,
          " according to its configuration. This is a bug in the conversion; "
          "the original index was not modified.")};
    }
  };
  std::vector<Permutation::Enum> permutations{Permutation::Enum::PSO,
                                              Permutation::Enum::POS};
  if (index.loadAllPermutations()) {
    permutations.insert(permutations.end(),
                        {Permutation::Enum::SPO, Permutation::Enum::SOP,
                         Permutation::Enum::OSP, Permutation::Enum::OPS});
  }
  for (auto p : permutations) {
    checkNumTriples(index.getImpl().getPermutation(p),
                    expectedNumTriples.normal);
  }
  // The triples with QLever-internal predicates live in a nested permutation
  // of `PSO` and `POS` (see `IndexImpl::createFromOnDiskIndex`).
  for (auto p : {Permutation::Enum::PSO, Permutation::Enum::POS}) {
    checkNumTriples(index.getImpl().getPermutation(p).internalPermutation(),
                    expectedNumTriples.internal);
  }
}
}  // namespace

// _____________________________________________________________________________
void upgradeIndexInPlace(const std::string& basename) {
  // Read the configuration of the index that is to be upgraded; this also
  // checks that the index exists and is exactly in the source format.
  auto configuration = readAndCheckConfiguration(basename);

  // Derive the staging and retirement directories (see the header for the
  // naming scheme and its rationale). There is no command-line option for
  // choosing these directories, hence no hint for the (unlikely) error that
  // the default names are all taken.
  IndexSwapNaming naming{std::string{stagingDirPrefix},
                         std::string{retiredDirPrefix},
                         IndexImpl::dateOfIndexBuild(configuration, basename),
                         /* retiredDirConflictHint_ */ ""};
  IndexSwapConfig config =
      makeIndexSwapConfig(basename, naming, std::nullopt, std::nullopt);

  convertIndexToCurrentFormat(basename, config.newIndexSource());
  checkUpgradedIndex(config.newIndexSource(),
                     static_cast<Index::NumNormalAndInternal>(
                         configuration.at("num-triples")));
  moveIndexIntoPlace(config);
  AD_LOG_INFO << "The upgrade was successful: the upgraded index is at \""
              << config.newIndexTarget() << "\"" << std::endl;
  AD_LOG_INFO << "The index in the old format was moved to the directory \""
              << fs::path{config.oldIndexTarget()}.parent_path().string()
              << "\"" << std::endl;
}

}  // namespace qlever::indexFormatConverter
