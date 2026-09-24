// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/IndexFormatConverter.h"

#include <absl/base/casts.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <deque>
#include <fstream>
#include <functional>
#include <future>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "backports/filesystem.h"
#include "backports/keywords.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "engine/idTable/IdTable.h"
#include "global/Constants.h"
#include "global/FileSuffixConstants.h"
#include "global/MaterializedViewConstants.h"
#include "index/CompressedRelationWriter.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/DeltaTriples.h"
#include "index/ExternalSortFunctors.h"
#include "index/Index.h"
#include "index/IndexFormatVersion.h"
#include "index/IndexImpl.h"
#include "index/IndexMetaData.h"
#include "index/IndexSwap.h"
#include "index/KeyOrder.h"
#include "index/LocalVocab.h"
#include "index/PatternCreator.h"
#include "index/Permutation.h"
#include "index/ScanSpecification.h"
#include "rdfTypes/GeometryInfo.h"
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
#include "util/ProgressBar.h"
#include "util/json.h"

namespace qlever::indexFormatConverter {

namespace {

namespace fs = ql::filesystem;

// Return a human-readable representation of the given index format `version`.
std::string versionAsString(const IndexFormatVersion& version) {
  return absl::StrCat("PR = ", version.prNumber_,
                      ", Date = ", version.date_.toStringAndType().first);
}

// The number of leading columns of a permutation that form its sort key: the
// three columns of the permuted triple and its graph, see `writePermutation`
// below. The remaining columns (the two pattern columns of `PSO` and `SPO`) are
// payload.
constexpr size_t numKeyColumns = NumColumnsIndexBuilding;
static_assert(numKeyColumns == 4);

// The smallest and the largest `Id` of type `GeoPoint`. All points of a sorted
// column form one contiguous range within these bounds.
const Id minGeoPointId = Id::makeFromGeoPointBits(0);
const Id maxGeoPointId = Id::makeFromGeoPointBits(
    ad_utility::bitMaskForLowerBits(GeoPoint::numDataBits));

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

// Write the `configuration` of the index with the base name `basename`, with
// the index format version set to the target format.
void writeConfigurationWithTargetVersion(nlohmann::json configuration,
                                         const std::string& basename) {
  configuration[indexFormatVersionKey] = targetVersion;
  ad_utility::makeOfstream(absl::StrCat(basename, CONFIGURATION_FILE))
      << configuration.dump(4) << std::endl;
}

// Return the name of the file with the persisted updates of the index with the
// base name `basename`, and whether it exists.
std::string persistedUpdatesFilename(const std::string& basename) {
  return absl::StrCat(basename, UPDATE_TRIPLES_SUFFIX);
}
bool hasPersistedUpdates(const std::string& basename) {
  return fs::exists(persistedUpdatesFilename(basename));
}

// Throw if the index with the base name `basename` has persisted updates. Those
// contain `Id`s as well, but converting them is deliberately not supported (see
// the documentation of `convertIndexToCurrentFormat`).
void throwIfPersistedUpdatesExist(const std::string& basename) {
  if (hasPersistedUpdates(basename)) {
    throw std::runtime_error{absl::StrCat(
        "The index \"", basename, "\" has persisted updates (the file \"",
        persistedUpdatesFilename(basename),
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

// The cancellation handle for the scans of this converter, which never cancels
// anything.
//
// NOTE: A scan stores a *reference* to its `SharedCancellationHandle` (see the
// `Generator` in `CompressedRelationReader::lazyScan`), not a copy of it. It is
// therefore not enough that the `CancellationHandle` stays alive, the
// `shared_ptr` that holds it has to stay alive as well, and at an address that
// does not change. That is what the `unique_ptr` is for: it can be moved into
// a lambda that keeps everything alive that the scan borrows, and moving it
// does not move its pointee. Note that a scan only touches the handle if the
// permutation has more than one block, so getting this wrong is not caught by a
// test with a tiny permutation.
std::unique_ptr<ad_utility::SharedCancellationHandle> makeCancellationHandle() {
  return std::make_unique<ad_utility::SharedCancellationHandle>(
      std::make_shared<ad_utility::CancellationHandle<>>());
}

// Return a lazy full scan of `permutation` in which all `Id`s are converted to
// the current index format. The rows are still in the order of the source
// format, see
// `sortRunsOfGeoPoints` for the re-sorting. The returned range has to be
// consumed before `permutation` is destroyed.
ad_utility::InputRangeTypeErased<IdTableStatic<0>> scanAndConvertIds(
    const Permutation& permutation) {
  auto locatedTriplesState = makeEmptyLocatedTriplesState(permutation);
  auto scanSpecAndBlocks = permutation.getScanSpecAndBlocks(
      ScanSpecification{std::nullopt, std::nullopt, std::nullopt},
      *locatedTriplesState);
  auto additionalColumns = getAdditionalColumns(getNumColumns(permutation));
  auto cancellationHandle = makeCancellationHandle();
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

// The implementation of `sortRunsOfGeoPoints`, see there. The rows that need
// re-sorting form runs of consecutive rows, so this state machine collects the
// rows of the current run, sorts it once a row arrives that does not belong to
// it (or the input ends), and passes every other row through unchanged. A run
// that outgrows its share of the memory is sorted by the external sorter of
// the index builder, and is then returned block by block, which is why the
// output is a queue of tables and sorted runs rather than a single table.
class RunSorter : public ad_utility::InputRangeFromGet<IdTableStatic<0>> {
 private:
  using Block = IdTableStatic<0>;
  using Sorter = ad_utility::CompressedExternalIdTableSorter<SortByColumns, 0>;

  // A run that was sorted on disk. The output refers to the sorter, so the two
  // have to be kept (and destroyed) together.
  struct SortedRun {
    std::unique_ptr<Sorter> sorter_;
    ad_utility::InputRangeTypeErased<Block> output_;
  };

  ad_utility::InputRangeTypeErased<Block> input_;
  bool inputExhausted_ = false;
  std::string sortTempFilename_;
  size_t numSorters_ = 0;
  ad_utility::MemorySize memory_;
  ad_utility::AllocatorWithLimit<Id> allocator_ =
      ad_utility::makeUnlimitedAllocator<Id>();
  SortByColumns comparator_{{0, 1, 2, 3}};
  static_assert(numKeyColumns == 4);

  // Set when the first block has arrived, see `processBlock`.
  size_t numColumns_ = 0;
  size_t maxRowsOfRunInMemory_ = 0;

  // The current run, if one is open: the column of the first `GeoPoint` of its
  // rows, the values of the columns before that column, and its rows (in
  // memory, or in the sorter once there are too many of them).
  std::optional<size_t> runGeoColumn_;
  std::array<Id, numKeyColumns> runPrefix_{};
  IdTable runRows_{allocator_};
  std::unique_ptr<Sorter> runSorter_;

  // What is to be returned, in order.
  std::deque<std::variant<Block, std::unique_ptr<SortedRun>>> pending_;

 public:
  RunSorter(ad_utility::InputRangeTypeErased<Block> input,
            std::string sortTempFilename, ad_utility::MemorySize memory)
      : input_{std::move(input)},
        sortTempFilename_{std::move(sortTempFilename)},
        memory_{memory} {}

  // Return the next block, or `nullopt` once everything has been returned.
  std::optional<Block> get() override {
    while (true) {
      if (!pending_.empty()) {
        if (auto block = takeFromPending()) return block;
        continue;
      }
      if (inputExhausted_) {
        // The last run, if one is still open, ends with the input.
        if (!runGeoColumn_.has_value()) return std::nullopt;
        closeRun();
        continue;
      }
      if (auto block = input_.get()) {
        processBlock(std::move(block.value()));
      } else {
        inputExhausted_ = true;
      }
    }
  }

 private:
  // Return the next block of the first item of `pending_`, or `nullopt` if
  // that item is empty or exhausted (it is then removed).
  std::optional<Block> takeFromPending() {
    auto& front = pending_.front();
    if (auto* block = std::get_if<Block>(&front)) {
      Block result = std::move(*block);
      pending_.pop_front();
      if (result.empty()) return std::nullopt;
      return result;
    }
    auto& run = *std::get<std::unique_ptr<SortedRun>>(front);
    if (auto block = run.output_.get()) return block;
    pending_.pop_front();
    return std::nullopt;
  }

  // The column of the first `GeoPoint` among the key columns of the given
  // `row` of `block`, or `nullopt` if it has none.
  static std::optional<size_t> geoColumnOf(const Block& block, size_t row) {
    for (size_t column = 0; column < numKeyColumns; ++column) {
      if (isGeoPoint(block(row, column))) {
        return column;
      }
    }
    return std::nullopt;
  }

  // Return true iff the two given rows of `block` belong to the same run, that
  // is, agree on all columns before `geoColumn`, the column of the first
  // `GeoPoint` of `row`.
  static bool sameRun(const Block& block, size_t row, size_t otherRow,
                      size_t geoColumn) {
    if (geoColumnOf(block, otherRow) != geoColumn) {
      return false;
    }
    for (size_t column = 0; column < geoColumn; ++column) {
      if (block(row, column) != block(otherRow, column)) {
        return false;
      }
    }
    return true;
  }

  // Return true iff the given `row` of `block` belongs to the open run.
  bool inOpenRun(const Block& block, size_t row) const {
    if (!runGeoColumn_.has_value() ||
        geoColumnOf(block, row) != runGeoColumn_) {
      return false;
    }
    for (size_t column = 0; column < runGeoColumn_.value(); ++column) {
      if (block(row, column) != runPrefix_[column]) {
        return false;
      }
    }
    return true;
  }

  // Sort the runs of `block`, and hand the block and the runs that are too
  // long for it to `pending_`, in order.
  void processBlock(Block block) {
    if (numColumns_ == 0) {
      numColumns_ = block.numColumns();
      AD_CONTRACT_CHECK(numColumns_ >= numKeyColumns);
      runRows_ = IdTable{numColumns_, allocator_};
      // Half of the budget for the rows of a run in memory, the other half for
      // the sorter that takes over when there are more of them.
      maxRowsOfRunInMemory_ = std::max<size_t>(
          1, (memory_ / 2).getBytes() / (numColumns_ * sizeof(Id)));
    }
    AD_CONTRACT_CHECK(block.numColumns() == numColumns_);
    size_t numRows = block.numRows();
    if (numRows == 0) {
      return;
    }

    // The leading rows that continue the open run. If that is the whole block,
    // it belongs to that run and nothing else has to be done.
    size_t numLeading = 0;
    while (numLeading < numRows && inOpenRun(block, numLeading)) {
      ++numLeading;
    }
    if (numLeading == numRows) {
      appendToRun(block, 0, numRows);
      return;
    }
    if (numLeading > 0) {
      appendToRun(block, 0, numLeading);
    }
    if (runGeoColumn_.has_value()) {
      closeRun();
    }

    // A run that ends inside this block is sorted where it is, so that the
    // many short runs cost no copy at all. Only the last run of the block may
    // continue in the next one, so only that one has to be taken out.
    size_t trailingRunStart = numRows;
    for (size_t row = numLeading; row < numRows;) {
      auto geoColumn = geoColumnOf(block, row);
      if (!geoColumn.has_value()) {
        ++row;
        continue;
      }
      size_t end = row + 1;
      while (end < numRows && sameRun(block, row, end, geoColumn.value())) {
        ++end;
      }
      if (end == numRows) {
        trailingRunStart = row;
        break;
      }
      if (end - row > 1) {
        ql::ranges::sort(block.begin() + row, block.begin() + end, comparator_);
      }
      row = end;
    }

    // Take the rows of the trailing run out of the block and truncate it,
    // which moves no row. Only the rows of a leading run, which have already
    // been appended to that run, have to be dropped with a copy, and a block
    // rarely starts inside a run.
    if (trailingRunStart < numRows) {
      runGeoColumn_ = geoColumnOf(block, trailingRunStart);
      for (size_t column = 0; column < runGeoColumn_.value(); ++column) {
        runPrefix_[column] = block(trailingRunStart, column);
      }
      appendToRun(block, trailingRunStart, numRows);
      block.resize(trailingRunStart);
    }
    if (numLeading == 0) {
      pending_.emplace_back(std::move(block));
      return;
    }
    if (block.numRows() > numLeading) {
      IdTable rest{numColumns_, allocator_};
      rest.insertAtEnd(block, numLeading, block.numRows());
      pending_.emplace_back(Block{std::move(rest)});
    }
  }

  // Append the rows `[begin, end)` of `block` to the open run.
  void appendToRun(const Block& block, size_t begin, size_t end) {
    if (runSorter_ != nullptr) {
      if (begin == 0 && end == block.numRows()) {
        runSorter_->pushBlock(block);
      } else {
        IdTable rows{numColumns_, allocator_};
        rows.insertAtEnd(block, begin, end);
        runSorter_->pushBlock(rows);
      }
      return;
    }
    runRows_.insertAtEnd(block, begin, end);
    if (runRows_.numRows() > maxRowsOfRunInMemory_) {
      // The run does not fit into memory, so the sorter takes over. It gets
      // the whole budget, because the rows it is given here are released.
      //
      // NOTE: The lower bound only matters for the tiny budgets with which the
      // unit tests force the external sorting; with too little memory, the
      // sorter writes more blocks than it can merge again.
      constexpr ad_utility::MemorySize minMemoryForSorter = 1_MB;
      AD_LOG_INFO << "The points of \"" << sortTempFilename_
                  << "\" do not fit into memory, sorting them on disk ..."
                  << std::endl;
      runSorter_ = std::make_unique<Sorter>(
          absl::StrCat(sortTempFilename_, ".", numSorters_++), numColumns_,
          std::max(memory_, minMemoryForSorter), allocator_,
          ad_utility::DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE, comparator_);
      runSorter_->pushBlock(runRows_);
      runRows_ = IdTable{numColumns_, allocator_};
    }
  }

  // Sort the open run and hand it to `pending_`.
  void closeRun() {
    AD_CORRECTNESS_CHECK(runGeoColumn_.has_value());
    if (runSorter_ != nullptr) {
      auto run = std::make_unique<SortedRun>();
      run->sorter_ = std::move(runSorter_);
      run->output_ = run->sorter_->getSortedOutput(std::nullopt);
      pending_.emplace_back(std::move(run));
    } else {
      ad_utility::BlockSorter<SortByColumns>{comparator_}(runRows_);
      pending_.emplace_back(Block{std::move(runRows_)});
      runRows_ = IdTable{numColumns_, allocator_};
    }
    runGeoColumn_.reset();
  }
};

// Write the given `blocks` as a single permutation to the file `filename`, and
// return its metadata. The metadata is not yet written to disk, because it is
// only complete once the multiplicities have been exchanged with the twin
// permutation (see `permutationPairs` above). The `blocks` have to be sorted by
// their first `numKeyColumns` columns.
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
  // The blocks already are in the correct order, so the identity is the correct
  // key order here.
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

// Return true iff the given `triple` has a `GeoPoint` in any of its columns.
bool hasGeoPoint(const CompressedBlockMetadata::PermutedTriple& triple) {
  return isGeoPoint(triple.col0Id_) || isGeoPoint(triple.col1Id_) ||
         isGeoPoint(triple.col2Id_) || isGeoPoint(triple.graphId_);
}

// Check that the permutation that was written (`newMetaData`) has the same
// content as the permutation that it was converted from (`oldMetaData`). Only
// the number of triples and the first and last triple are compared, which is
// cheap because it only looks at the metadata that is in memory anyway. A first
// or last triple with a `GeoPoint` may have moved by the re-sorting, so it is
// only compared if it has none.
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
  const auto& oldFirst = oldBlocks.front().firstTriple_;
  const auto& oldLast = oldBlocks.back().lastTriple_;
  if (!hasGeoPoint(oldFirst)) {
    check(oldFirst == newBlocks.front().firstTriple_);
  }
  if (!hasGeoPoint(oldLast)) {
    check(oldLast == newBlocks.back().lastTriple_);
  }
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

// Load the given `permutation` of the index with the base name `basename` from
// disk. Return `nullptr` if it does not exist.
std::unique_ptr<Permutation> loadPermutation(const std::string& basename,
                                             Permutation::Enum permutationEnum,
                                             bool isInternal) {
  if (!hasPermutation(basename, permutationEnum, isInternal)) {
    return nullptr;
  }
  auto permutation = std::make_unique<Permutation>(
      permutationEnum, ad_utility::makeUnlimitedAllocator<Id>());

  // NOTE: The "Registered ... permutation" message that `loadFromDisk` logs by
  // default is suppressed here, because it would interrupt the progress bar of
  // `convertPermutations` below. The statistics of each permutation are in the
  // log of the index build, and those of the upgraded index are logged when it
  // is checked (see `checkUpgradedIndex`).
  permutation->loadFromDisk(basenameForPermutations(basename, isInternal),
                            false, Permutation::Type::NORMAL, {},
                            /* logRegistration = */ false);
  return permutation;
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

// The column of the given `permutation` that holds the object of the triple
// (for example, column 0 for `OSP` and column 1 for `POS`).
size_t objectColumnOf(const Permutation& permutation) {
  const auto& keys = permutation.keyOrder().keys();
  for (size_t column = 0; column < 3; ++column) {
    if (keys.at(column) == 2) {
      return column;
    }
  }
  AD_FAIL();
}

// Read the block with the given `blockIndex` of `permutation` and return true
// iff its `column` contains a `GeoPoint`.
bool blockContainsGeoPoint(const Permutation& permutation, size_t blockIndex,
                           size_t column) {
  auto locatedTriplesState = makeEmptyLocatedTriplesState(permutation);
  BlockMetadataSpan allBlocks{permutation.metaData().blockData()};
  BlockMetadataRanges singleBlock{BlockMetadataRange{
      allBlocks.begin() + blockIndex, allBlocks.begin() + blockIndex + 1}};
  CompressedRelationReader::ScanSpecAndBlocks scanSpecAndBlocks{
      ScanSpecification{std::nullopt, std::nullopt, std::nullopt}, singleBlock};
  auto cancellationHandle = makeCancellationHandle();
  IdTable block = permutation.scan(scanSpecAndBlocks, {}, *cancellationHandle,
                                   *locatedTriplesState);
  return ql::ranges::any_of(block.getColumn(column), &isGeoPoint);
}

// Return true iff the object column of `permutation` contains a `GeoPoint`.
// Each block is classified by its metadata: if its first and its last triple
// agree on all columns before the object column, the block's objects are
// sorted and lie between the objects of these two triples, so the block has a
// point iff that range intersects the range of all points; if that range
// contains the whole range of all points, or if the block spans several values
// of the columns before the object column, the block is read.
bool permutationContainsGeoPoints(const Permutation& permutation,
                                  std::string_view name) {
  size_t objectColumn = objectColumnOf(permutation);
  const auto& blocks = permutation.metaData().blockData();
  for (size_t blockIndex = 0; blockIndex < blocks.size(); ++blockIndex) {
    const auto& block = blocks.at(blockIndex);
    auto columnOf = [](const CompressedBlockMetadata::PermutedTriple& triple,
                       size_t column) {
      return column == 0   ? triple.col0Id_
             : column == 1 ? triple.col1Id_
                           : triple.col2Id_;
    };
    Id firstObject = columnOf(block.firstTriple_, objectColumn);
    Id lastObject = columnOf(block.lastTriple_, objectColumn);
    if (isGeoPoint(firstObject) || isGeoPoint(lastObject)) {
      AD_LOG_INFO << "The " << name
                  << " permutation contains geo points (block " << blockIndex
                  << " starts or ends with one)" << std::endl;
      return true;
    }
    bool prefixIsConstant = true;
    for (size_t column = 0; column < objectColumn; ++column) {
      if (columnOf(block.firstTriple_, column) !=
          columnOf(block.lastTriple_, column)) {
        prefixIsConstant = false;
      }
    }
    if (prefixIsConstant &&
        (lastObject < minGeoPointId || firstObject > maxGeoPointId)) {
      continue;
    }
    AD_LOG_INFO << "Reading block " << blockIndex << " of the " << name
                << " permutation to check whether it contains geo points ("
                << firstObject << " .. " << lastObject << ")" << std::endl;
    if (blockContainsGeoPoint(permutation, blockIndex, objectColumn)) {
      AD_LOG_INFO << "The " << name
                  << " permutation contains geo points (block " << blockIndex
                  << ")" << std::endl;
      return true;
    }
  }
  AD_LOG_INFO << "The " << name << " permutation contains no geo points"
              << std::endl;
  return false;
}

// Convert a single permutation of an index and write the result to the index
// with the base name `newBasename`. Return the metadata of the new permutation,
// which still has to be written to disk, see `writePermutation` above.
//
// NOTE: The two permutations of a pair are converted concurrently (see
// `convertPermutations` below), so this must not touch any state that is
// shared between them. That is why the files of the old permutation are
// recorded by the caller and not here, why the progress is reported to a
// threadsafe `progress` callback instead of being logged here, and why the
// temporary file of the external sorter is named after the permutation.
IndexMetaData convertPermutation(const Permutation& oldPermutation,
                                 const std::string& newBasename,
                                 bool isInternal,
                                 const std::function<void(size_t)>& progress) {
  std::string newFilename =
      filenameForPermutation(newBasename, oldPermutation, isInternal);
  auto rows = sortRunsOfGeoPoints(scanAndConvertIds(oldPermutation),
                                  absl::StrCat(newFilename, ".sort-tmp"),
                                  memoryForSorting() / 2);
  // NOTE: The progress is reported on the rows that are written, not on the
  // rows that are read. The two differ while the points of a permutation are
  // sorted externally: the rows of that run are all read before the first of
  // them is written, so a progress bar over the rows that are read would race
  // ahead and then stand still for as long as the sorting takes.
  auto rowsWithProgress =
      ad_utility::InputRangeTypeErased{ad_utility::CachingTransformInputRange{
          std::move(rows), [&progress](IdTableStatic<0>& block) {
            progress(block.numRows());
            return std::move(block);
          }}};
  auto newMetaData = writePermutation(
      newFilename, getNumColumns(oldPermutation), std::move(rowsWithProgress));
  newMetaData.setName(oldPermutation.metaData().getName());
  verifyConvertedPermutation(oldPermutation.metaData(), newMetaData,
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
    // Record the files of the two old permutations (see
    // `checkAllFilesWereHandled`). This happens here and not in
    // `convertPermutation`, because `handledFiles` is shared between the two
    // conversions below, which run concurrently.
    auto recordOldFiles = [&handledFiles, &oldBasename,
                           isInternal](const Permutation& permutation) {
      std::string oldFilename =
          filenameForPermutation(oldBasename, permutation, isInternal);
      handledFiles.emplace_back(oldFilename);
      handledFiles.emplace_back(absl::StrCat(oldFilename, META_FILE_SUFFIX));
    };
    recordOldFiles(*permutationA);
    recordOldFiles(*permutationB);

    // Convert the two permutations of the pair concurrently. They are
    // independent of each other (each has its own reader, its own writer, its
    // own sorter, and its own metadata), and a single conversion uses only few
    // threads (`lazy-index-scan-num-threads` for reading and
    // `permutation-writer-num-threads` for writing), so there are cores to
    // spare. One of the two conversions runs on this thread, so that only one
    // additional thread is needed.
    //
    // NOTE: If the conversion on this thread throws, the destructor of
    // `futureB` waits for the other conversion to finish before the exception
    // leaves this function. That is exactly what we want: no thread must still
    // be writing to the incomplete index when the caller handles the error.
    auto convert = [&newBasename, isInternal,
                    &progress](const Permutation& permutation) {
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
    writeMetaData(newMetaA, filenameForPermutation(newBasename, *permutationA,
                                                   isInternal));
    writeMetaData(newMetaB, filenameForPermutation(newBasename, *permutationB,
                                                   isInternal));
  }
  progressBar.logFinalProgressString();
}

// Convert the patterns of the index with the base name `oldBasename` (if it has
// them) and write them to the index with the base name `newBasename`. NOTE: The
// patterns hold only the `Id`s of predicates, for which the conversion is the
// identity, but this is not relied upon.
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

// Return the names of the materialized views of the index with the base name
// `basename`. Each view has exactly one info file, so the names of the views
// are exactly the infixes of those files (`<basename>.view.<name><suffix>`).
std::vector<std::string> namesOfMaterializedViews(const std::string& basename) {
  auto viewFiles =
      qlever::util::filesWithBaseNameAndSuffix(basename, VIEW_FILE_INFIX);
  std::string prefix = materializedViewFilenameBase(basename, "");
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
  return names;
}

// Warn that the materialized views of the index with the base name `basename`
// (if it has any) are not converted, see the documentation of
// `convertIndexToCurrentFormat`. Return their names.
std::vector<std::string> warnAboutMaterializedViews(
    const std::string& basename) {
  auto names = namesOfMaterializedViews(basename);
  if (!names.empty()) {
    AD_LOG_WARN << "The index has " << names.size()
                << " materialized view(s), which this converter does not "
                   "convert: "
                << absl::StrJoin(names, ", ")
                << ". They have to be created again for the converted index."
                << std::endl;
  }
  return names;
}

// The layout of the `.geoinfo` file of a geometry vocabulary (see
// `GeoVocabulary`): a header that holds the `GEOMETRY_INFO_VERSION`, followed
// by one record of `sizeof(GeometryInfo)` bytes per word of that vocabulary,
// which is all-zero for a word that is not a valid geometry. Every valid record
// holds three encoded points (the two corners of the bounding box and the
// centroid), which are what the conversion rewrites.
using GeoInfoRecord = std::array<uint8_t, sizeof(ad_utility::GeometryInfo)>;
constexpr size_t geoInfoHeaderSize = sizeof(ad_utility::GEOMETRY_INFO_VERSION);
constexpr GeoInfoRecord invalidGeoInfoRecord{};
constexpr std::string_view geoInfoSuffix = ".geoinfo";

// Return the `.geoinfo` files of the vocabularies of the index with the base
// name `basename` (one per geometry vocabulary, typically at most one).
std::vector<fs::path> geoInfoFiles(const std::string& basename) {
  std::vector<fs::path> files;
  for (const auto& file :
       qlever::util::filesWithBaseNameAndSuffix(basename, VOCAB_SUFFIX)) {
    if (ql::ends_with(file.native(), geoInfoSuffix)) {
      files.push_back(file);
    }
  }
  return files;
}

// Open the `.geoinfo` file `file` for reading, check its header, and return
// the stream (positioned after the header) together with the number of
// records.
std::pair<std::ifstream, size_t> openGeoInfoFile(const fs::path& file) {
  auto fileSize = fs::file_size(file);
  if (fileSize < geoInfoHeaderSize ||
      (fileSize - geoInfoHeaderSize) % sizeof(GeoInfoRecord) != 0) {
    throw std::runtime_error{absl::StrCat(
        "The file \"", file.native(), "\" has ", fileSize,
        " bytes, which is not a valid size for a file with geometry "
        "information, so the index is corrupted")};
  }
  std::ifstream in{file, std::ios::binary};
  AD_CONTRACT_CHECK(in.is_open());
  uint64_t version = 0;
  in.read(reinterpret_cast<char*>(&version), geoInfoHeaderSize);
  if (version != ad_utility::GEOMETRY_INFO_VERSION) {
    throw std::runtime_error{absl::StrCat(
        "The file \"", file.native(), "\" has the geometry info version ",
        version, ", but this converter only handles the version ",
        ad_utility::GEOMETRY_INFO_VERSION, ". Please rebuild the index.")};
  }
  return {std::move(in),
          (fileSize - geoInfoHeaderSize) / sizeof(GeoInfoRecord)};
}

// Read the records of the `.geoinfo` file `file` in chunks and call
// `handleChunk` for each of them, which may modify the records and returns
// false to stop the reading.
void forEachChunkOfGeoInfoRecords(
    const fs::path& file,
    const std::function<bool(ql::span<GeoInfoRecord>)>& handleChunk) {
  constexpr size_t recordsPerChunk = 1 << 16;
  auto [in, numRecords] = openGeoInfoFile(file);
  std::vector<GeoInfoRecord> chunk(recordsPerChunk);
  for (size_t done = 0; done < numRecords;) {
    size_t numInChunk = std::min(recordsPerChunk, numRecords - done);
    in.read(reinterpret_cast<char*>(chunk.data()),
            numInChunk * sizeof(GeoInfoRecord));
    AD_CORRECTNESS_CHECK(in.good());
    if (!handleChunk(ql::span<GeoInfoRecord>{chunk.data(), numInChunk})) {
      return;
    }
    done += numInChunk;
  }
}

// Return true iff any `.geoinfo` file of the index with the base name
// `basename` has a valid record, that is, a geometry with encoded points.
bool geoInfoFilesContainGeometries(const std::string& basename) {
  return ql::ranges::any_of(geoInfoFiles(basename), [](const fs::path& file) {
    bool found = false;
    forEachChunkOfGeoInfoRecords(file, [&found](ql::span<GeoInfoRecord> chunk) {
      found = ql::ranges::any_of(chunk, [](const GeoInfoRecord& record) {
        return record != invalidGeoInfoRecord;
      });
      return !found;
    });
    return found;
  });
}

// Convert the `.geoinfo` file `oldFile` and write the result to `newFile`: the
// header and the invalid records are copied, the encoded points of every valid
// record are rewritten (see `convertGeoPointBits`).
void convertGeoInfoFile(const fs::path& oldFile, const fs::path& newFile) {
  size_t numRecords =
      (fs::file_size(oldFile) - geoInfoHeaderSize) / sizeof(GeoInfoRecord);
  AD_LOG_INFO << "Converting the geometry information of "
              << ad_utility::withThousandSeparators(numRecords)
              << " geometries in \"" << oldFile.native() << "\" ..."
              << std::endl;
  std::ofstream out{newFile, std::ios::binary};
  AD_CORRECTNESS_CHECK(out.is_open());
  out.write(reinterpret_cast<const char*>(&ad_utility::GEOMETRY_INFO_VERSION),
            geoInfoHeaderSize);
  ad_utility::ConcurrentProgressBar progressBar{
      "Geometries converted: ", numRecords, batchSizeFor(numRecords)};
  auto progress = progressCallbackFor(progressBar);

  forEachChunkOfGeoInfoRecords(
      oldFile, [&out, &progress](ql::span<GeoInfoRecord> chunk) {
        for (GeoInfoRecord& record : chunk) {
          if (record == invalidGeoInfoRecord) {
            continue;
          }
          auto info = absl::bit_cast<ad_utility::GeometryInfo>(record);
          record = absl::bit_cast<GeoInfoRecord>(
              info.withPointBitsMappedBy(&convertGeoPointBits));
          // A valid record never becomes all-zero, because the geometry type
          // and the number of geometries are not touched and are never zero.
          AD_CORRECTNESS_CHECK(record != invalidGeoInfoRecord);
        }
        out.write(reinterpret_cast<const char*>(chunk.data()),
                  chunk.size() * sizeof(GeoInfoRecord));
        AD_CORRECTNESS_CHECK(out.good());
        progress(chunk.size());
        return true;
      });
  progressBar.logFinalProgressString();
}

// Convert all `.geoinfo` files of the index with the base name `oldBasename`
// and write them to the index with the base name `newBasename`.
void convertGeoInfoFiles(const std::string& oldBasename,
                         const std::string& newBasename,
                         std::vector<fs::path>& handledFiles) {
  for (const auto& oldFile : geoInfoFiles(oldBasename)) {
    std::string_view filename = oldFile.native();
    AD_CORRECTNESS_CHECK(ql::starts_with(filename, oldBasename));
    filename.remove_prefix(oldBasename.size());
    handledFiles.emplace_back(oldFile);
    convertGeoInfoFile(oldFile, absl::StrCat(newBasename, filename));
  }
}

// Copy all files of the index with the base name `oldBasename` that need no
// conversion to the index with the base name `newBasename`. These are the files
// that contain no `Id`s and no encoded points at all: the vocabulary (except
// for its `.geoinfo` files, see `convertGeoInfoFiles`), the text index (which
// stores plain integers and reconstructs its `Id`s when it is read), the
// settings, and the state of the graph-name allocation.
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
  for (const auto& file :
       qlever::util::filesWithBaseNameAndSuffix(oldBasename, VOCAB_SUFFIX)) {
    if (!ql::ends_with(file.native(), geoInfoSuffix)) {
      copy(file);
    }
  }
}

// Copy all files of the index with the base name `oldBasename` unchanged to
// the index with the base name `newBasename`, including its materialized
// views. This is the conversion of an index that needs no conversion (see
// `indexNeedsNoConversion`), except for the configuration file, which the
// caller writes with the target version.
void copyAllFiles(const std::string& oldBasename,
                  const std::string& newBasename) {
  AD_LOG_INFO << "Copying all files of the index unchanged ..." << std::endl;
  auto copy = [&oldBasename, &newBasename](const fs::path& oldFile) {
    std::string_view filename = oldFile.native();
    AD_CORRECTNESS_CHECK(ql::starts_with(filename, oldBasename));
    filename.remove_prefix(oldBasename.size());
    fs::copy_file(oldFile, absl::StrCat(newBasename, filename));
  };
  for (const auto& file : IndexImpl::allIndexFiles(oldBasename)) {
    if (file.native() == absl::StrCat(oldBasename, CONFIGURATION_FILE)) {
      continue;
    }
    copy(file);
  }
  ql::ranges::for_each(
      qlever::util::filesWithBaseNameAndSuffix(oldBasename, VIEW_FILE_INFIX),
      copy);
}

// Check that the filesystem of the converted index has enough free space, and
// throw if it certainly does not. The conversion writes a full second copy of
// everything but the materialized views, and while a permutation whose points
// do not fit into memory is converted, the external sorter holds that
// permutation once more (compressed, like the permutation itself); two
// permutations are converted at a time. The free space is therefore certainly
// insufficient if it is below the size of the files that are written, and it
// is probably insufficient if it is below that plus the two largest
// permutations.
void checkFreeSpace(const std::string& oldBasename,
                    const std::string& newBasename) {
  uint64_t bytesToWrite = 0;
  std::vector<uint64_t> permutationSizes;
  for (const auto& file : IndexImpl::allIndexFiles(oldBasename)) {
    auto size = fs::file_size(file);
    bytesToWrite += size;
    if (file.native().find(PERMUTATION_FILE_INFIX) != std::string::npos &&
        !ql::ends_with(file.native(), META_FILE_SUFFIX)) {
      permutationSizes.push_back(size);
    }
  }
  ql::ranges::sort(permutationSizes, std::greater<>{});
  permutationSizes.resize(std::min<size_t>(permutationSizes.size(), 2));
  uint64_t bytesForSorting = std::accumulate(
      permutationSizes.begin(), permutationSizes.end(), uint64_t{0});
  uint64_t bytesFree = freeSpaceForTesting().value_or(
      fs::space(fs::path{newBasename}.parent_path()).available);
  auto asString = [](uint64_t bytes) {
    return ad_utility::MemorySize::bytes(bytes).asString();
  };
  if (bytesFree < bytesToWrite) {
    throw std::runtime_error{absl::StrCat(
        "The conversion writes a second copy of the index (everything but its "
        "materialized views), which needs at least ",
        asString(bytesToWrite), ", but only ", asString(bytesFree),
        " are free where the converted index is written. Please free up space, "
        "or rebuild the index instead of converting it.")};
  }
  if (bytesFree < bytesToWrite + bytesForSorting) {
    AD_LOG_WARN << "The conversion writes " << asString(bytesToWrite)
                << " and may need up to " << asString(bytesForSorting)
                << " more while it sorts the points of a permutation, but only "
                << asString(bytesFree)
                << " are free where the converted index is written. The "
                   "conversion may run out of space."
                << std::endl;
  }
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
      ") in place. The only difference between the two formats is the bit "
      "representation of a geo point (a WKT `POINT`) in an ID: the two "
      "coordinates are now bit-interleaved instead of stored one after the "
      "other, so that all points in a rectangle have IDs in a few contiguous "
      "ranges. This changes the order of the points in the permutations, so "
      "the "
      "IDs of all points are rewritten and the affected parts of the "
      "permutations are sorted again. Nothing else changes.\n\nAn index "
      "without "
      "geo points needs no conversion at all (and can also be used as is, the "
      "server detects this); for such an index, only the format version in the "
      "meta data is updated.\n\nFor an index with geo points, the upgraded "
      "index is first written to the subdirectory `",
      stagingDirPrefix,
      "<current datetime>.tmp` and checked. Only then, the index in the old "
      "format is moved to the subdirectory `",
      retiredDirPrefix,
      "<datetime of its build>` and the upgraded index takes its place. The "
      "materialized views of such an index are not converted, but moved to "
      "that "
      "subdirectory as well, so they have to be created again for the upgraded "
      "index.\n\nNote that rebuilding the index from its input files is still "
      "the recommended way to move to a new index format, because it also "
      "profits from all improvements to the index building since the index "
      "was built.");
}

// _____________________________________________________________________________
GeoPoint::T convertGeoPointBits(GeoPoint::T bits) {
  // The source format stores the quantized latitude in the upper and the
  // quantized longitude in the lower `numDataBitsCoordinate` bits.
  constexpr auto numCoordinateBits = GeoPoint::numDataBitsCoordinate;
  constexpr auto coordinateMask =
      ad_utility::bitMaskForLowerBits(numCoordinateBits);
  AD_CONTRACT_CHECK((bits & GeoPoint::coordinateMaskFreeBits) == 0);
  GeoPoint::T lat = (bits >> numCoordinateBits) & coordinateMask;
  GeoPoint::T lng = bits & coordinateMask;
  return GeoPoint::interleaveCoordinates(lat, lng);
}

// _____________________________________________________________________________
Id convertId(Id id) {
  if (id.getDatatype() == Datatype::LocalVocabIndex) {
    throw std::runtime_error{
        "Encountered an `Id` of type `LocalVocabIndex`, which must never be "
        "stored on disk (it holds a pointer into the memory of the process "
        "that created it), so the index that is converted is corrupted"};
  }
  if (!isGeoPoint(id)) {
    return id;
  }
  auto valueBits =
      id.getBits() & ad_utility::bitMaskForLowerBits(ValueId::numDataBits);
  return Id::makeFromGeoPointBits(convertGeoPointBits(valueBits));
}

// _____________________________________________________________________________
ad_utility::InputRangeTypeErased<IdTableStatic<0>> sortRunsOfGeoPoints(
    ad_utility::InputRangeTypeErased<IdTableStatic<0>> blocks,
    std::string sortTempFilename, ad_utility::MemorySize memory) {
  return ad_utility::InputRangeTypeErased<IdTableStatic<0>>{
      std::make_unique<RunSorter>(std::move(blocks),
                                  std::move(sortTempFilename), memory)};
}

// _____________________________________________________________________________
bool indexContainsGeoPoints(const std::string& basename) {
  // The geometry information of a geometry vocabulary holds encoded points for
  // every valid geometry, points or not.
  if (geoInfoFilesContainGeometries(basename)) {
    AD_LOG_INFO << "The geometry vocabulary of the index \"" << basename
                << "\" contains geometries, whose bounding boxes and "
                   "centroids are encoded geo points"
                << std::endl;
    return true;
  }
  // Check a permutation with the object as its first column if the index has
  // one, else `POS`; and the internal `POS` in the same way.
  auto check = [&basename](Permutation::Enum permutationEnum,
                           bool isInternal) -> std::optional<bool> {
    auto permutation = loadPermutation(basename, permutationEnum, isInternal);
    if (permutation == nullptr) {
      return std::nullopt;
    }
    return permutationContainsGeoPoints(
        *permutation, absl::StrCat(isInternal ? "internal " : "",
                                   Permutation::toString(permutationEnum)));
  };
  auto normal = check(Permutation::OSP, false);
  if (!normal.has_value()) {
    normal = check(Permutation::POS, false);
  }
  auto internal = check(Permutation::POS, true);
  if (!normal.has_value() || !internal.has_value()) {
    AD_LOG_INFO << "The index \"" << basename
                << "\" has no permutation that could be checked for geo "
                   "points, so it is assumed to have some"
                << std::endl;
    return true;
  }
  return normal.value() || internal.value();
}

// _____________________________________________________________________________
bool indexNeedsNoConversion(const std::string& basename) {
  return !hasPersistedUpdates(basename) && !indexContainsGeoPoints(basename);
}

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

  checkFreeSpace(oldBasename, newBasename);

  AD_LOG_INFO << "Converting the index \"" << oldBasename << "\" to \""
              << newBasename << "\" ..." << std::endl;

  if (!indexContainsGeoPoints(oldBasename)) {
    AD_LOG_INFO << "The index contains no encoded geo points, so it needs no "
                   "conversion, only its format version has to be updated"
                << std::endl;
    copyAllFiles(oldBasename, newBasename);
  } else {
    // The files of the old index that have been converted or copied, see
    // `checkAllFilesWereHandled`. The configuration file is handled at the very
    // end, but is already listed here.
    std::vector<fs::path> handledFiles{
        absl::StrCat(oldBasename, CONFIGURATION_FILE)};
    convertPermutations(oldBasename, newBasename,
                        static_cast<Index::NumNormalAndInternal>(
                            configuration.at("num-triples")),
                        handledFiles);
    convertPatterns(oldBasename, newBasename, handledFiles);
    convertGeoInfoFiles(oldBasename, newBasename, handledFiles);
    copyFilesThatNeedNoConversion(oldBasename, newBasename, handledFiles);
    checkAllFilesWereHandled(oldBasename, handledFiles);
    warnAboutMaterializedViews(oldBasename);
  }

  // Write the configuration last, with the version of the target format. An
  // index without its configuration file cannot be loaded at all, so if the
  // conversion is interrupted, the incomplete index is not mistaken for a
  // complete one.
  writeConfigurationWithTargetVersion(std::move(configuration), newBasename);

  AD_LOG_INFO << "Conversion of the index completed, the converted index is \""
              << newBasename << "\"" << std::endl;
}

namespace {
// Check that every permutation of the upgraded index with the base name
// `newBasename` can be read and has the number of triples that the
// configuration of the index that was upgraded says (that configuration is
// copied unchanged by the conversion, so comparing configurations would check
// nothing). A conversion that loses or duplicates rows of a permutation is
// caught here, before the upgraded index replaces the original one.
//
// NOTE: The permutations are opened one by one, instead of loading the whole
// index, so that this only depends on what the conversion writes. An index can
// use a feature that this binary does not know (a geo cell grid scheme that
// only exists on a branch, say), which loading it would refuse, although it has
// nothing to do with the conversion.
void checkUpgradedIndex(const std::string& newBasename,
                        const Index::NumNormalAndInternal& expectedNumTriples) {
  AD_LOG_INFO << "Checking the permutations of the upgraded index ..."
              << std::endl;
  for (const auto& permutationPair : permutationPairs) {
    auto [enumA, enumB] = permutationPair.first;
    bool isInternal = permutationPair.second;
    for (auto permutationEnum : {enumA, enumB}) {
      auto permutation =
          loadPermutation(newBasename, permutationEnum, isInternal);
      if (permutation == nullptr) {
        continue;
      }
      size_t expected =
          isInternal ? expectedNumTriples.internal : expectedNumTriples.normal;
      size_t actual = permutation->numTriples();
      if (actual != expected) {
        throw std::runtime_error{absl::StrCat(
            "The ", permutation->readableName(),
            " permutation of the upgraded index has ", actual,
            " triples, but the index that was upgraded has ", expected,
            " according to its configuration. This is a bug in the conversion; "
            "the original index was not modified.")};
      }
    }
  }
}
}  // namespace

// _____________________________________________________________________________
void upgradeIndexInPlace(const std::string& basename) {
  checkThatTheSupportedFormatsAreUpToDate();
  // Read the configuration of the index that is to be upgraded; this also
  // checks that the index exists and is exactly in the source format.
  auto configuration = readAndCheckConfiguration(basename);
  throwIfPersistedUpdatesExist(basename);

  // An index without geo points is already in the target format except for
  // the version in its configuration.
  if (!indexContainsGeoPoints(basename)) {
    writeConfigurationWithTargetVersion(std::move(configuration), basename);
    AD_LOG_INFO << "The index \"" << basename
                << "\" contains no geo points, so it needs no conversion. "
                   "Only its format version was updated, from ("
                << versionAsString(sourceVersion) << ") to ("
                << versionAsString(targetVersion) << ")" << std::endl;
    return;
  }

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
  auto viewNames = namesOfMaterializedViews(basename);
  moveIndexIntoPlace(config);
  AD_LOG_INFO << "The upgrade was successful: the upgraded index is at \""
              << config.newIndexTarget() << "\"" << std::endl;
  std::string retiredDirectory =
      fs::path{config.oldIndexTarget()}.parent_path().string();
  AD_LOG_INFO << "The index in the old format was moved to the directory \""
              << retiredDirectory << "\"" << std::endl;
  if (!viewNames.empty()) {
    AD_LOG_WARN << "The materialized view(s) " << absl::StrJoin(viewNames, ", ")
                << " were not converted, but moved to that directory as well. "
                   "They have to be created again for the upgraded index (for "
                   "example, with `qlever materialized-view` of the qlever "
                   "CLI)."
                << std::endl;
  }
}

}  // namespace qlever::indexFormatConverter
