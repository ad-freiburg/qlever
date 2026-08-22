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

#include <array>
#include <cstdint>
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
#include "index/ConstantsIndexBuilding.h"
#include "index/DeltaTriples.h"
#include "index/IndexFormatVersion.h"
#include "index/IndexImpl.h"
#include "index/IndexMetaData.h"
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
#include "util/File.h"
#include "util/FilesystemHelpers.h"
#include "util/InputRangeUtils.h"
#include "util/Log.h"
#include "util/json.h"

namespace qlever::indexFormatConverter {

namespace {

namespace fs = ql::filesystem;

// Return a human-readable representation of the given index format `version`.
std::string versionAsString(const IndexFormatVersion& version) {
  return absl::StrCat("PR = ", version.prNumber_,
                      ", Date = ", version.date_.toStringAndType().first);
}

// The `Datatype`s of the source format (see `sourceVersion`), in the order of
// the numeric values that they had in that format. In other words, an `Id`
// whose datatype bits are `i` in the source format is an `Id` of datatype
// `datatypesOfSourceFormat[i]` in the target format. The only difference
// between the two formats is that `Datatype::SecondaryVocabIndex` was inserted
// (see the note there), which is why this array is exactly the current enum
// without that datatype.
constexpr std::array<Datatype, 12> datatypesOfSourceFormat{
    Datatype::Undefined,
    Datatype::Bool,
    Datatype::Int,
    Datatype::Double,
    Datatype::VocabIndex,
    Datatype::LocalVocabIndex,
    Datatype::TextRecordIndex,
    Datatype::Date,
    Datatype::GeoPoint,
    Datatype::WordVocabIndex,
    Datatype::BlankNodeIndex,
    Datatype::EncodedVal};

// Return true iff `datatypes` is strictly ascending. NOTE: This is `consteval`,
// because it is only ever used in the `static_assert` below.
QL_CONSTEVAL bool isStrictlyAscending(
    const std::array<Datatype, 12>& datatypes) noexcept {
  for (size_t i = 1; i < datatypes.size(); ++i) {
    if (!(datatypes[i - 1] < datatypes[i])) {
      return false;
    }
  }
  return true;
}

// The conversion preserves the relative order of all datatypes of the source
// format. This is what makes it possible to convert a permutation by rewriting
// its `Id`s one by one: the result is still sorted, so it does not have to be
// sorted again. If a future change of the format violates this, then this
// converter is not applicable to it.
static_assert(isStrictlyAscending(datatypesOfSourceFormat));

// Exactly one datatype was added, so no datatype of the source format was
// removed or duplicated above.
static_assert(datatypesOfSourceFormat.size() + 1 ==
              static_cast<size_t>(Datatype::MaxValue) + 1);

// The version of the on-disk format of the materialized views (see
// `MATERIALIZED_VIEWS_VERSION`) that the views of an index in the source format
// have. That version was raised together with the index format, so the views
// have to be converted as well.
constexpr size_t materializedViewsVersionOfSourceFormat = 1;
static_assert(materializedViewsVersionOfSourceFormat + 1 ==
              MATERIALIZED_VIEWS_VERSION);

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

// An empty `LocatedTriplesState` for the given `permutation`, which is what a
// scan of that permutation requires. It is empty because the index that is
// converted has no delta triples (see `throwIfPersistedUpdatesExist` above).
std::shared_ptr<LocatedTriplesState> makeEmptyLocatedTriplesState(
    const Permutation& permutation) {
  LocatedTriplesPerBlockAllPermutations<false> emptyLocatedTriples;
  emptyLocatedTriples.at(static_cast<size_t>(permutation.permutation()))
      .setOriginalMetadata(permutation.metaData().blockDataShared());
  // NOTE: The located triples of the internal permutations deliberately stay
  // untouched. `loadPermutation` below loads every permutation with
  // `Permutation::Type::NORMAL`, including the internal ones, so a scan always
  // looks up its located triples in the array above.
  LocatedTriplesPerBlockAllPermutations<true> emptyInternalLocatedTriples;
  LocalVocab emptyVocab;
  return std::make_shared<LocatedTriplesState>(
      LocatedTriplesState{emptyLocatedTriples, emptyInternalLocatedTriples,
                          emptyVocab.getLifetimeExtender(), 0});
}

// Return the number of columns that the given `permutation` has on disk. Note
// that this is not stored explicitly, but can be read off the metadata of any
// of its blocks.
size_t getNumColumns(const Permutation& permutation) {
  const auto& blocks = permutation.metaData().blockData();
  if (blocks.empty()) {
    // The permutation is empty, so its number of columns is irrelevant. Use the
    // minimum, which is what the index builder would use.
    return NumColumnsIndexBuilding;
  }
  const auto& offsets = blocks.front().offsetsAndCompressedSize_;
  AD_CORRECTNESS_CHECK(offsets.has_value(),
                       "A block that was read from disk always knows the "
                       "offsets of its columns");
  return offsets.value().size();
}

// Return the columns of a permutation with `numColumns` columns that a scan has
// to request explicitly, that is, all columns except for the three columns of
// the (permuted) triple itself. These are the graph column and, for the
// permutations that store the patterns, the two pattern columns.
std::vector<ColumnIndex> getAdditionalColumns(size_t numColumns) {
  AD_CORRECTNESS_CHECK(numColumns >= NumColumnsIndexBuilding);
  std::vector<ColumnIndex> additionalColumns;
  for (size_t column = NumColumnsIndexBuilding - 1; column < numColumns;
       ++column) {
    additionalColumns.push_back(static_cast<ColumnIndex>(column));
  }
  AD_CORRECTNESS_CHECK(additionalColumns.at(0) == ADDITIONAL_COLUMN_GRAPH_ID);
  return additionalColumns;
}

// Return a lazy full scan of `permutation` in which all `Id`s are converted to
// the current index format. The returned range has to be consumed before
// `permutation` is destroyed.
ad_utility::InputRangeTypeErased<IdTableStatic<0>> scanAndConvertIds(
    const Permutation& permutation) {
  auto locatedTriplesState = makeEmptyLocatedTriplesState(permutation);
  auto scanSpecAndBlocks = permutation.getScanSpecAndBlocks(
      ScanSpecification{std::nullopt, std::nullopt, std::nullopt},
      *locatedTriplesState);
  auto additionalColumns = getAdditionalColumns(getNumColumns(permutation));
  // The cancellation handle of the scan, which never cancels anything.
  //
  // NOTE: The scan stores a *reference* to this `SharedCancellationHandle` (see
  // the `Generator` in `CompressedRelationReader::lazyScan`), not a copy of it.
  // It is therefore not enough that the `CancellationHandle` stays alive, the
  // `shared_ptr` that holds it has to stay alive as well, and at an address
  // that does not change. That is what this extra indirection is for: the
  // `unique_ptr` is moved into the lambda below (which keeps everything alive
  // that the scan borrows), and moving it does not move its pointee. Note that
  // a scan only touches the handle if the permutation has more than one block,
  // so getting this wrong is not caught by a test with a tiny permutation.
  auto cancellationHandle =
      std::make_unique<ad_utility::SharedCancellationHandle>(
          std::make_shared<ad_utility::CancellationHandle<>>());
  // NOTE: Deliberately no structured binding, because the members are captured
  // by the lambda below, which is only valid in C++20.
  auto scanWithReader = permutation.lazyScanWithUnlimitedReader(
      scanSpecAndBlocks, additionalColumns, *cancellationHandle,
      *locatedTriplesState);

  // NOTE: The scan borrows the `reader`, the `locatedTriplesState` and the
  // `cancellationHandle`, so all of them are moved into the transformation
  // below to keep them alive for as long as the returned range is.
  return ad_utility::InputRangeTypeErased{
      ad_utility::CachingTransformInputRange{
          std::move(scanWithReader.blocks_),
          [reader = std::move(scanWithReader.reader_),
           locatedTriplesState = std::move(locatedTriplesState),
           cancellationHandle =
               std::move(cancellationHandle)](IdTable& idTable) {
            for (auto column : idTable.getColumns()) {
              ql::ranges::for_each(column, [](Id& id) { id = convertId(id); });
            }
            return IdTableStatic<0>{std::move(idTable)};
          }}};
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
  // The blocks already are in the correct order (the conversion does not change
  // the order of the `Id`s, see `datatypesOfSourceFormat` above), so the
  // identity is the correct key order here.
  auto [numDistinctCol0, blockMetadata] =
      CompressedRelationWriter::createPermutation({std::move(writer), callback},
                                                  std::move(blocks),
                                                  KeyOrder{0, 1, 2, 3}, {});
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
// content as the permutation that it was converted from (`oldMetaData`). Only
// the number of triples and the first and last triple are compared, which is
// cheap because it only looks at the metadata that is in memory anyway.
void verifyConvertedPermutation(const IndexMetaData& oldMetaData,
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
  check(oldMetaData.totalElements() == newMetaData.totalElements());
  const auto& oldBlocks = oldMetaData.blockData();
  const auto& newBlocks = newMetaData.blockData();
  check(oldBlocks.empty() == newBlocks.empty());
  if (oldBlocks.empty()) {
    return;
  }
  auto convertTriple = [](CompressedBlockMetadata::PermutedTriple triple) {
    return CompressedBlockMetadata::PermutedTriple{
        convertId(triple.col0Id_), convertId(triple.col1Id_),
        convertId(triple.col2Id_), convertId(triple.graphId_)};
  };
  check(convertTriple(oldBlocks.front().firstTriple_) ==
        newBlocks.front().firstTriple_);
  check(convertTriple(oldBlocks.back().lastTriple_) ==
        newBlocks.back().lastTriple_);
}

// Load the given `permutation` of the index with the base name `basename` from
// disk. Return `nullptr` if it does not exist.
std::unique_ptr<Permutation> loadPermutation(const std::string& basename,
                                             Permutation::Enum permutationEnum,
                                             bool isInternal) {
  auto permutation = std::make_unique<Permutation>(
      permutationEnum, ad_utility::makeUnlimitedAllocator<Id>());
  if (!fs::exists(filenameForPermutation(basename, *permutation, isInternal))) {
    return nullptr;
  }
  permutation->loadFromDisk(basenameForPermutations(basename, isInternal));
  return permutation;
}

// Convert a single permutation of an index and write the result to the index
// with the base name `newBasename`. Return the metadata of the new permutation,
// which still has to be written to disk, see `writePermutation` above.
IndexMetaData convertPermutation(const Permutation& oldPermutation,
                                 const std::string& newBasename,
                                 bool isInternal,
                                 std::vector<fs::path>& handledFiles) {
  std::string oldFilename =
      absl::StrCat(oldPermutation.onDiskBase(), PERMUTATION_FILE_INFIX,
                   oldPermutation.fileSuffix());
  handledFiles.emplace_back(oldFilename);
  handledFiles.emplace_back(absl::StrCat(oldFilename, META_FILE_SUFFIX));

  AD_LOG_INFO << "Converting the " << oldPermutation.readableName()
              << " permutation ..." << std::endl;
  std::string newFilename =
      filenameForPermutation(newBasename, oldPermutation, isInternal);
  auto newMetaData =
      writePermutation(newFilename, getNumColumns(oldPermutation),
                       scanAndConvertIds(oldPermutation));
  newMetaData.setName(oldPermutation.metaData().getName());
  verifyConvertedPermutation(oldPermutation.metaData(), newMetaData,
                             newFilename);
  return newMetaData;
}

// Convert all permutations of the index with the base name `oldBasename` and
// write them to the index with the base name `newBasename`.
void convertPermutations(const std::string& oldBasename,
                         const std::string& newBasename,
                         std::vector<fs::path>& handledFiles) {
  for (const auto& [permutationEnums, isInternal] : permutationPairs) {
    auto [enumA, enumB] = permutationEnums;
    auto permutationA = loadPermutation(oldBasename, enumA, isInternal);
    auto permutationB = loadPermutation(oldBasename, enumB, isInternal);
    if (permutationA == nullptr && permutationB == nullptr) {
      // The index does not have this pair of permutations at all, which is the
      // case for `SPO`, `SOP`, `OPS`, and `OSP` if the index was built with
      // `--only-pso-and-pos-permutations`.
      continue;
    }
    if (permutationA == nullptr || permutationB == nullptr) {
      throw std::runtime_error{absl::StrCat(
          "The index \"", oldBasename, "\" has only one of the permutations ",
          Permutation::toString(enumA), " and ", Permutation::toString(enumB),
          ", so it is incomplete and cannot be converted")};
    }
    auto newMetaA = convertPermutation(*permutationA, newBasename, isInternal,
                                       handledFiles);
    auto newMetaB = convertPermutation(*permutationB, newBasename, isInternal,
                                       handledFiles);
    // The multiplicities of the last column of a permutation are stored in the
    // metadata of its twin, so they have to be exchanged before the metadata is
    // written.
    newMetaA.exchangeMultiplicities(newMetaB);
    writeMetaData(newMetaA, filenameForPermutation(newBasename, *permutationA,
                                                   isInternal));
    writeMetaData(newMetaB, filenameForPermutation(newBasename, *permutationB,
                                                   isInternal));
  }
}

// Convert the patterns of the index with the base name `oldBasename` (if it has
// them) and write them to the index with the base name `newBasename`.
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
  CompactVectorOfStrings<Id> patterns;
  PatternCreator::readPatternsFromFile(
      oldFilename, statistics.avgNumDistinctSubjectsPerPredicate_,
      statistics.avgNumDistinctPredicatesPerSubject_,
      statistics.numDistinctSubjectPredicatePairs_, patterns);
  PatternCreator::writePatternsToFile(
      absl::StrCat(newBasename, PATTERNS_FILE_SUFFIX),
      patterns.cloneAndRemap(&convertId), statistics);
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
  Permutation oldPermutation{Permutation::SPO,
                             ad_utility::makeUnlimitedAllocator<Id>(), name};
  oldPermutation.loadFromDisk(oldViewBasename, false,
                              Permutation::Type::MATERIALIZED_VIEW);
  std::string newFilename = absl::StrCat(newViewBasename, VIEW_SPO_SUFFIX);
  auto newMetaData =
      writePermutation(newFilename, getNumColumns(oldPermutation),
                       scanAndConvertIds(oldPermutation));
  newMetaData.setName(newViewBasename);
  verifyConvertedPermutation(oldPermutation.metaData(), newMetaData,
                             newFilename);
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
      "Convert an index in the index format (", versionAsString(sourceVersion),
      ") to an index in the index format (", versionAsString(targetVersion),
      "). The only difference between the two formats is the numbering of the "
      "datatypes of the IDs: the datatype for the words of a secondary "
      "vocabulary was inserted in the middle, which renumbered the datatypes "
      "after it. The IDs of the index are therefore rewritten, and nothing "
      "else "
      "changes. The index that is converted is not modified.\n\nNote that "
      "rebuilding the index from its input files is still the recommended way "
      "to move to a new index format, because it also profits from all "
      "improvements to the index building since the index was built.");
}

// _____________________________________________________________________________
Id convertId(Id id) {
  auto datatypeBits = id.getBits() >> ValueId::numDataBits;
  if (datatypeBits >= datatypesOfSourceFormat.size()) {
    throw std::runtime_error{absl::StrCat(
        "Encountered an `Id` with the invalid datatype ", datatypeBits,
        ", the index that is converted is corrupted")};
  }
  auto datatype = datatypesOfSourceFormat.at(datatypeBits);
  if (datatype == Datatype::LocalVocabIndex) {
    throw std::runtime_error{
        "Encountered an `Id` of type `LocalVocabIndex`, which must never be "
        "stored on disk (it holds a pointer into the memory of the process "
        "that created it), so the index that is converted is corrupted"};
  }
  auto valueBits =
      id.getBits() & ad_utility::bitMaskForLowerBits(ValueId::numDataBits);
  return Id::fromBits(
      valueBits | (static_cast<uint64_t>(datatype) << ValueId::numDataBits));
}

// _____________________________________________________________________________
void convertIndexToCurrentFormat(const std::string& oldBasename,
                                 const std::string& newBasename) {
  AD_CONTRACT_CHECK(!oldBasename.empty() && !newBasename.empty(),
                    "The base names of the indexes must not be empty");
  // NOTE: This is a user-facing error and not a requirement violation, because
  // passing the same base name twice is an easy mistake to make on the command
  // line of `qlever-convert-index`.
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

  convertPermutations(oldBasename, newBasename, handledFiles);
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

}  // namespace qlever::indexFormatConverter
