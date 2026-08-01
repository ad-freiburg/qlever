//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include "index/IndexRebuilder.h"

#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <array>
#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <cstdint>
#include <fstream>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include "backports/algorithm.h"
#include "engine/idTable/IdTable.h"
#include "global/FileSuffixConstants.h"
#include "global/Id.h"
#include "global/RuntimeParameters.h"
#include "index/IndexImpl.h"
#include "index/IndexRebuilderImpl.h"
#include "index/LocalVocabEntry.h"
#include "index/Permutation.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/HashMap.h"
#include "util/InputRangeUtils.h"
#include "util/Log.h"
#include "util/ProgressBar.h"
#include "util/StringUtils.h"
#include "util/Timer.h"

namespace qlever::indexRebuilder {

namespace {

// The number of processed steps (e.g. written words) that are accumulated
// locally before they are reported to a progress callback. Each report is a
// mutex-protected addition on a shared counter (see
// `ad_utility::ConcurrentProgressBar`), so reporting every single step would be
// needlessly expensive. The exact value is not important; it only has to be
// large enough to amortize the callback and small enough for smooth progress
// output.
constexpr size_t PROGRESS_REPORTING_BATCH_SIZE = 65536;

// Helper struct that stores where a local vocab entry should be inserted into
// the original vocab and what the original `Id` of the local vocab entry was
// (so that we can create the mapping from old.
struct InsertionInfo {
  // The position indicates the gap between the actual values, so 0 means that
  // the local vocab entry should be inserted before the first entry of the
  // original vocab, 1 means that it should be inserted between the first and
  // second entry of the original vocab, etc.
  VocabIndex insertionPosition_;
  std::string_view word_;
  Id originalId_;
};

// Merge the local vocab entries with the original vocab and write a new
// vocabulary. Returns a mapping from the old local vocab `Id`s bit
// representation (for cheaper hash functions) to new `Id`s.
LocalVocabMapping mergeVocabs(const std::string& vocabularyName,
                              const Index::Vocab& vocab,
                              const std::vector<InsertionInfo>& insertInfo,
                              const std::function<void(size_t)>& progress) {
  auto vocabWriter = vocab.makeWordWriterPtr(vocabularyName);
  LocalVocabMapping localVocabMapping;
  // Report the number of written words to `progress` in batches, see
  // `PROGRESS_REPORTING_BATCH_SIZE`.
  size_t wordsSinceLastProgress = 0;
  auto noteWord = [&progress, &wordsSinceLastProgress]() {
    if (!progress) {
      return;
    }
    if (++wordsSinceLastProgress == PROGRESS_REPORTING_BATCH_SIZE) {
      progress(wordsSinceLastProgress);
      wordsSinceLastProgress = 0;
    }
  };
  auto writeWordFromVocab = [&vocab, &vocabWriter,
                             &noteWord](const IndexAndWord& indexAndWord) {
    const auto& [_, word] = indexAndWord;
    (*vocabWriter)(word, vocab.shouldBeExternalized(word));
    noteWord();
  };
  auto writeWordFromLocalVocab = [&vocab, &vocabWriter, &localVocabMapping,
                                  &noteWord](const InsertionInfo& info) {
    const auto& [_, word, originalId] = info;
    auto newIndex = (*vocabWriter)(word, vocab.shouldBeExternalized(word));
    localVocabMapping.emplace(
        originalId.getBits(),
        Id::makeFromVocabIndex(VocabIndex::make(newIndex)));
    noteWord();
  };
  ad_utility::OverloadCallOperator writer{std::move(writeWordFromVocab),
                                          std::move(writeWordFromLocalVocab)};
  ql::ranges::merge(
      vocab.scanAll(), insertInfo,
      ad_utility::IteratorForAssigmentOperator{writer}, {},
      // The tags ensure that the local vocab entries are sorted before all the
      // original vocab entries, even if they share the same vocab index as
      // insertion position.
      [tag = 1](const IndexAndWord& indexAndWord) {
        return std::tie(indexAndWord.index_, tag);
      },
      [tag = 0](const InsertionInfo& info) {
        return std::tie(info.insertionPosition_.get(), tag);
      });
  if (progress && wordsSinceLastProgress > 0) {
    progress(wordsSinceLastProgress);
  }
  return localVocabMapping;
}
}  // namespace

// _____________________________________________________________________________
std::tuple<InsertionPositions, LocalVocabMapping> materializeLocalVocab(
    const std::vector<LocalVocabIndex>& entries, const Index::Vocab& vocab,
    const std::string& newIndexName,
    const std::function<void(size_t)>& progress) {
  std::vector<InsertionInfo> insertInfo;
  insertInfo.reserve(entries.size());

  for (auto* entry : entries) {
    const auto& [lower, upper] = entry->positionInVocab();
    AD_CORRECTNESS_CHECK(lower == upper);
    Id id = Id::fromBits(upper.get());
    AD_CORRECTNESS_CHECK(id.getDatatype() == Datatype::VocabIndex);
    insertInfo.emplace_back(id.getVocabIndex(),
                            entry->asLiteralOrIri().toStringRepresentation(),
                            Id::makeFromLocalVocabIndex(entry));
  }
  // Sort by insertion position, then by the original `Id`. It would probably
  // suffice to just sort by `Id`, but it is faster to check the two numbers
  // first that we already computed.
  ql::ranges::sort(insertInfo, {}, [](const InsertionInfo& info) {
    return std::tie(info.insertionPosition_, info.originalId_);
  });

  LocalVocabMapping localVocabMapping =
      mergeVocabs(newIndexName + VOCAB_SUFFIX, vocab, insertInfo, progress);
  auto denseInfo = insertInfo |
                   ql::views::transform(&InsertionInfo::insertionPosition_) |
                   ::ranges::to<std::vector>;
  return std::make_tuple(std::move(denseInfo), std::move(localVocabMapping));
}

// _____________________________________________________________________________
BlankNodeBlocks flattenBlankNodeBlocks(const OwnedBlocks& ownedBlocks) {
  auto result = ownedBlocks |
                ql::views::transform(&OwnedBlocksEntry::blockIndices_) |
                ql::views::join | ::ranges::to<std::vector>;
  ql::ranges::sort(result);
  return result;
}

// _____________________________________________________________________________
namespace {
// Compute by what offset `value` needs to be increased to fit in the new index.
size_t computeIndexOffset(VocabIndex value,
                          const InsertionPositions& insertionPositions) {
  return ql::ranges::distance(
      insertionPositions.begin(),
      ql::ranges::upper_bound(insertionPositions, value, std::less{}));
}

// Apply `offset` to `value` and return the new `Id` resulting from this.
Id applyOffset(VocabIndex value, size_t offset) {
  return Id::makeFromVocabIndex(VocabIndex::make(value.get() + offset));
}
}  // namespace

// _____________________________________________________________________________
Id remapVocabId(Id original, const InsertionPositions& insertionPositions) {
  AD_EXPENSIVE_CHECK(
      original.getDatatype() == Datatype::VocabIndex,
      "Only ids resembling a vocab index can be remapped with this function.");
  auto value = original.getVocabIndex();
  return applyOffset(value, computeIndexOffset(value, insertionPositions));
}

// _____________________________________________________________________________
Id remapVocabId(Id original, const InsertionPositions& insertionPositions,
                size_t& hint) {
  AD_EXPENSIVE_CHECK(
      original.getDatatype() == Datatype::VocabIndex,
      "Only ids resembling a vocab index can be remapped with this function.");
  AD_EXPENSIVE_CHECK(hint <= insertionPositions.size(),
                     "Hint must be a valid index into the insertion positions "
                     "or equal to its size.");
  auto value = original.getVocabIndex();
  auto isUpperBound = [value, &insertionPositions](size_t candidate) {
    return candidate == insertionPositions.size() ||
           insertionPositions[candidate] > value;
  };

  // Update `hint` to the correct upper bound for `value`. Avoid writing `hint`
  // in cases where that's not necessary.
  [&hint, &isUpperBound, &value, &insertionPositions]() {
    // Check if the cached hint is still the upper bound for `value`.
    if (isUpperBound(hint)) [[likely]] {
      // `hint` is an upper bound, so check if `hint - 1` is not an upper bound.
      if (hint == 0 || !isUpperBound(hint - 1)) [[likely]] {
        // `hint` still is the correct upper bound, so there is nothing to do.
        return;
      }
    } else {
      // Check if `hint + 1` is an upper bound. This is the case when we just
      // move the hint forward by one position.
      size_t next = hint + 1;
      if (isUpperBound(next)) [[likely]] {
        hint = next;
        return;
      }
    }

    // Fallback and write the hint for the next iteration.
    hint = computeIndexOffset(value, insertionPositions);
  }();

  return applyOffset(value, hint);
}

// _____________________________________________________________________________
std::optional<Id> tryRemapBlankNodeId(Id original,
                                      const BlankNodeBlocks& blankNodeBlocks,
                                      uint64_t minBlankNodeIndex) {
  AD_EXPENSIVE_CHECK(
      original.getDatatype() == Datatype::BlankNodeIndex,
      "Only ids resembling a blank node index can be remapped with this "
      "function.");
  auto rawId = original.getBlankNodeIndex().get();
  if (rawId < minBlankNodeIndex) {
    return original;
  }
  auto normalizedId = rawId - minBlankNodeIndex;
  auto blockIndex = normalizedId / ad_utility::BlankNodeManager::blockSize_;
  auto it = ql::ranges::lower_bound(blankNodeBlocks, blockIndex);
  if (it == blankNodeBlocks.end() || *it != blockIndex) {
    return std::nullopt;
  }
  auto relativeId = normalizedId % ad_utility::BlankNodeManager::blockSize_;
  auto blockOffset = ql::ranges::distance(blankNodeBlocks.begin(), it) *
                     ad_utility::BlankNodeManager::blockSize_;
  return Id::makeFromBlankNodeIndex(
      BlankNodeIndex::make(relativeId + blockOffset + minBlankNodeIndex));
}

// _____________________________________________________________________________
Id remapBlankNodeId(Id original, const BlankNodeBlocks& blankNodeBlocks,
                    uint64_t minBlankNodeIndex) {
  auto value =
      tryRemapBlankNodeId(original, blankNodeBlocks, minBlankNodeIndex);
  AD_CORRECTNESS_CHECK(value.has_value(),
                       "Could not find block index of blank node.");
  return value.value();
}

// _____________________________________________________________________________
ad_utility::InputRangeTypeErased<IdTableStatic<0>> readIndexAndRemap(
    const Permutation& permutation,
    const BlockMetadataRanges& blockMetadataRanges,
    const LocatedTriplesSharedState& locatedTriplesSharedState,
    const LocalVocabMapping& localVocabMapping,
    const InsertionPositions& insertionPositions,
    const BlankNodeBlocks& blankNodeBlocks, uint64_t minBlankNodeIndex,
    const ad_utility::SharedCancellationHandle& cancellationHandle,
    ql::span<const ColumnIndex> additionalColumns) {
  AD_CORRECTNESS_CHECK(ql::ranges::is_sorted(insertionPositions));
  AD_CORRECTNESS_CHECK(ql::ranges::is_sorted(blankNodeBlocks));
  Permutation::ScanSpecAndBlocks scanSpecAndBlocks{
      ScanSpecification{std::nullopt, std::nullopt, std::nullopt},
      blockMetadataRanges};
  // A value of 0 means "fall back to `lazy-index-scan-num-threads`" (the same
  // thread count as query scans); a positive value throttles the rebuild's
  // read/decompress parallelism only, reducing its peak CPU without touching
  // queries.
  auto rebuildScanThreads =
      getRuntimeParameter<&RuntimeParameters::rebuildIndexScanNumThreads_>();
  std::optional<size_t> numThreadsOverride =
      rebuildScanThreads == 0 ? std::nullopt
                              : std::optional<size_t>{rebuildScanThreads};
  auto [reader, fullScan] = permutation.lazyScanWithUnlimitedReader(
      scanSpecAndBlocks, additionalColumns, cancellationHandle,
      *locatedTriplesSharedState, numThreadsOverride);

  auto remapId = [&insertionPositions, &localVocabMapping, &blankNodeBlocks,
                  minBlankNodeIndex, lastId = Id::makeUndefined(),
                  mappedId = Id::makeUndefined(),
                  vocabHint = size_t{0}](Id& id) mutable {
    if (lastId.getBits() == id.getBits()) {
      id = mappedId;
      return;
    }
    lastId = id;
    using enum Datatype;
    auto datatype = id.getDatatype();
    if (datatype == VocabIndex) [[likely]] {
      id = remapVocabId(id, insertionPositions, vocabHint);
    } else if (datatype == LocalVocabIndex) {
      id = localVocabMapping.at(id.getBits());
    } else if (datatype == BlankNodeIndex) {
      id = remapBlankNodeId(id, blankNodeBlocks, minBlankNodeIndex);
    }
    mappedId = id;
  };

  return ad_utility::InputRangeTypeErased{
      ad_utility::CachingTransformInputRange{
          std::move(fullScan), [remapId = std::move(remapId),
                                reader = std::move(reader)](IdTable& idTable) {
            auto allCols = idTable.getColumns();
            // Extra columns beyond the graph column only contain integers (or
            // undefined for triples added via UPDATE) and thus don't need to be
            // remapped.
            constexpr size_t REGULAR_COLUMNS = 4;
            for (auto col : allCols | ::ranges::views::take(REGULAR_COLUMNS)) {
              ql::ranges::for_each(col, remapId);
            }
            AD_EXPENSIVE_CHECK(ql::ranges::all_of(
                allCols | ::ranges::views::drop(REGULAR_COLUMNS), [](auto col) {
                  return ql::ranges::all_of(col, [](Id id) {
                    return id.getDatatype() == Datatype::Int ||
                           id.isUndefined();
                  });
                }));
            return IdTableStatic<0>{std::move(idTable)};
          }}};
}

// _____________________________________________________________________________
size_t getNumColumns(const BlockMetadataRanges& blockMetadataRanges) {
  if (!blockMetadataRanges.empty()) {
    const auto& first = blockMetadataRanges.at(0);
    if (!first.empty()) {
      const auto& offsets = first[0].offsetsAndCompressedSize_;
      if (offsets.has_value()) {
        return offsets.value().size();
      }
    }
  }
  return 4;
}

// _____________________________________________________________________________
std::pair<size_t, std::vector<ColumnIndex>>
getNumberOfColumnsAndAdditionalColumns(
    const BlockMetadataRanges& blockMetadataRanges) {
  size_t numColumns = getNumColumns(blockMetadataRanges);
  std::vector<ColumnIndex> additionalColumns;
  additionalColumns.push_back(ADDITIONAL_COLUMN_GRAPH_ID);
  for (ColumnIndex col : {ADDITIONAL_COLUMN_INDEX_SUBJECT_PATTERN,
                          ADDITIONAL_COLUMN_INDEX_OBJECT_PATTERN}) {
    if (additionalColumns.size() >= numColumns - 3) {
      break;
    }
    additionalColumns.push_back(col);
  }
  AD_CORRECTNESS_CHECK(additionalColumns.size() == numColumns - 3);
  return std::make_pair(numColumns, additionalColumns);
}

namespace {
// Run the synchronous `func` as a distinct task on the current executor. The
// initial `post` is essential and easy to overlook: `co_spawn` (used by the
// `&&` operator below) starts a child coroutine *inline* via `dispatch()` on
// the spawning thread. Because `func` is fully synchronous and never suspends,
// without this reschedule the first sibling task would run to completion before
// the second is even started, serializing work that is meant to run in
// parallel. Posting first yields the thread immediately, so the siblings are
// queued onto the pool and actually spread across its threads.
template <typename Func>
boost::asio::awaitable<std::invoke_result_t<Func>> asCoroutine(Func func) {
  namespace net = boost::asio;
  co_await net::post(co_await net::this_coro::executor, net::use_awaitable);
  co_return std::invoke(func);
}
}  // namespace

// _____________________________________________________________________________
boost::asio::awaitable<void> createPermutationWriterTask(
    IndexImpl& newIndex, const Permutation& permutationA,
    const Permutation& permutationB, bool isInternal,
    const LocatedTriplesSharedState& locatedTriplesSharedState,
    const LocalVocabMapping& localVocabMapping,
    const InsertionPositions& insertionPositions,
    const BlankNodeBlocks& blankNodeBlocks, uint64_t minBlankNodeIndex,
    const ad_utility::SharedCancellationHandle& cancellationHandle,
    std::function<void(size_t)> progress) {
  namespace net = boost::asio;
  using namespace net::experimental::awaitable_operators;
  auto makeTaskForPermutation = [&](const Permutation& permutation) {
    return [&newIndex, &permutation, isInternal, &locatedTriplesSharedState,
            &localVocabMapping, &insertionPositions, &blankNodeBlocks,
            minBlankNodeIndex, &cancellationHandle, progress]() {
      auto blockMetadataRanges = permutation.getAugmentedMetadataForPermutation(
          *locatedTriplesSharedState);
      auto [numColumns, additionalColumns] =
          getNumberOfColumnsAndAdditionalColumns(blockMetadataRanges);
      // Wrap the input range so that the number of processed triples is
      // reported to `progress` per block.
      auto countingStream = ad_utility::InputRangeTypeErased<IdTableStatic<0>>{
          ad_utility::CachingTransformInputRange{
              readIndexAndRemap(
                  permutation, blockMetadataRanges, locatedTriplesSharedState,
                  localVocabMapping, insertionPositions, blankNodeBlocks,
                  minBlankNodeIndex, cancellationHandle, additionalColumns),
              [progress](IdTableStatic<0>& table) {
                if (progress) {
                  progress(table.numRows());
                }
                return std::move(table);
              }}};
      return newIndex.createPermutationWithoutMetadata(
          numColumns, std::move(countingStream), permutation, isInternal);
    };
  };
  // Workaround for a GCC 15/16 bug: the hidden object of a by-value
  // structured binding is not destroyed when the coroutine frame is
  // destroyed while suspended (gcc.gnu.org bug 124584).
  auto results = co_await (asCoroutine(makeTaskForPermutation(permutationA)) &&
                           asCoroutine(makeTaskForPermutation(permutationB)));
  auto& [resultA, resultB] = results;
  auto& [_, metaA] = resultA;
  auto& [__, metaB] = resultB;
  metaA.exchangeMultiplicities(metaB);

  auto makeFinalizerTasks = [&newIndex, isInternal](
                                IndexMetaData& meta,
                                const Permutation& permutation) {
    return [&newIndex, &meta, &permutation, isInternal]() {
      return newIndex.finalizePermutation(meta, permutation, isInternal);
    };
  };
  co_await (asCoroutine(makeFinalizerTasks(metaA, permutationA)) &&
            asCoroutine(makeFinalizerTasks(metaB, permutationB)));
}
}  // namespace qlever::indexRebuilder

// _____________________________________________________________________________
namespace qlever {
indexRebuilder::IndexRebuildMapping materializeToIndex(
    const IndexImpl& index, const std::string& newIndexName,
    const LocatedTriplesSharedState& locatedTriplesSharedState,
    const std::vector<LocalVocabIndex>& entries,
    const indexRebuilder::OwnedBlocks& ownedBlocks,
    const ad_utility::SharedCancellationHandle& cancellationHandle,
    const std::string& logFileName) {
  using namespace indexRebuilder;
  AD_CONTRACT_CHECK(!logFileName.empty(), "Log file name must not be empty");

  // The rebuilt index gets its own build date, namely the time when the
  // rebuild started (the statistics below are derived from the configuration
  // of the old index and hence contain the old date).
  auto dateOfIndexBuild = IndexImpl::formatIndexBuildTime(absl::Now());

  auto logFile = ad_utility::makeOfstream(logFileName);

  // Macro for rebuild-specific logging with the same syntax as AD_LOG_INFO
#define REBUILD_LOG_INFO \
  logFile << ad_utility::Log::getTimeStamp() << " - INFO: "

  REBUILD_LOG_INFO << "Rebuilding index from current data (including updates)"
                   << std::endl;

  // The phase headers say in parentheses what exactly is being processed, so
  // that the totals of the progress lines below are self-explanatory.
  REBUILD_LOG_INFO << "Writing new vocabulary (merging existing and new "
                      "words) ..."
                   << std::endl;

  auto blankNodeBlocks = flattenBlankNodeBlocks(ownedBlocks);
  ad_utility::ConcurrentProgressBar vocabProgress{
      logFile, "Words written: ", index.getVocab().size() + entries.size()};
  auto [insertionPositions, localVocabMapping] = materializeLocalVocab(
      entries, index.getVocab(), newIndexName,
      [&vocabProgress](size_t numWords) { vocabProgress.add(numWords); });
  vocabProgress.finish();

  REBUILD_LOG_INFO << "Recomputing statistics (from "
                   << (index.hasAllPermutations()
                           ? "4 permutations, 3 normal and 1 internal"
                           : "2 permutations, 1 normal and 1 internal")
                   << ") ..." << std::endl;

  // The totals for the progress reports below are taken from the statistics
  // of the old index; they are exact up to the delta triples, which is good
  // enough for a percentage. The statistics phase scans the PSO permutation,
  // its internal counterpart, and (if present) the SPO and OSP permutations.
  auto numTriplesOld = index.numTriples();
  size_t statsTotal =
      (index.hasAllPermutations() ? 3 : 1) * numTriplesOld.normal +
      numTriplesOld.internal;
  ad_utility::ConcurrentProgressBar statsProgress{logFile,
                                               "Triples counted: ", statsTotal};
  auto newStats = index.recomputeStatistics(
      locatedTriplesSharedState,
      [&statsProgress](size_t numRows) { statsProgress.add(numRows); });
  statsProgress.finish();
  newStats[DATE_OF_INDEX_BUILD_KEY] = dateOfIndexBuild;

  auto minBlankNodeIndex = index.getBlankNodeManager()->minIndex_;

  // Set newer lower bound for dynamic blank node indices.
  newStats["num-blank-nodes-total"] =
      minBlankNodeIndex +
      blankNodeBlocks.size() * ad_utility::BlankNodeManager::blockSize_;

  // Pass a 0-byte allocator as a sanity check: nothing below allocates
  // through `newIndex`'s allocator, and if a future change ever does, this
  // will throw immediately rather than silently using whatever allocator
  // the source index happens to have.
  IndexImpl newIndex{ad_utility::makeAllocatorWithLimit<Id>(0_B)};
  newIndex.loadConfigFromOldIndex(newIndexName, index, newStats);

  REBUILD_LOG_INFO << "Writing new index ("
                   << (index.hasAllPermutations()
                           ? "8 permutations, 6 normal and 2 internal"
                           : "4 permutations, 2 normal and 2 internal")
                   << ") ..." << std::endl;

  // Each of the (up to 6) normal permutations writes all normal triples,
  // each of the two internal permutations all internal triples.
  size_t permutationsTotal =
      (index.hasAllPermutations() ? 6 : 2) * numTriplesOld.normal +
      2 * numTriplesOld.internal;
  ad_utility::ConcurrentProgressBar permutationsProgress{
      logFile, "Triples written: ", permutationsTotal};
  auto permutationsProgressCallback = [&permutationsProgress](size_t numRows) {
    permutationsProgress.add(numRows);
  };

  auto patternThreads = static_cast<size_t>(index.usePatterns());
  size_t numberOfPermutations = index.hasAllPermutations() ? 8 : 4;
  namespace net = boost::asio;
  net::thread_pool threadPool{patternThreads + numberOfPermutations};

  // Collect the first exception thrown by any worker so it can be rethrown to
  // the caller after `threadPool.join()`. Without this, exceptions escaping a
  // `net::post` handler call `std::terminate` and exceptions from a detached
  // `co_spawn` are silently swallowed. NOTE: `exceptionCollector` must outlive
  // `threadPool`, since the worker callables capture a pointer to it via
  // `wrap()` / `std::ref`; the declaration order here guarantees that.
  ad_utility::ExceptionCollector exceptionCollector;

  if (index.usePatterns()) {
    net::post(threadPool, exceptionCollector.wrap([&newIndex, &index,
                                                   &insertionPositions]() {
      newIndex.getPatterns() = index.getPatterns().cloneAndRemap(
          [&insertionPositions](const Id& oldId) {
            return remapVocabId(oldId, insertionPositions);
          });
      newIndex.writePatternsToFile();
    }));
  }

  using enum Permutation::Enum;

  // List of permutation pairs, with the information whether the attached
  // internal permutation should be used.
  std::vector<std::pair<std::pair<Permutation::Enum, Permutation::Enum>, bool>>
      permutationSettings{{{PSO, POS}, false}, {{PSO, POS}, true}};

  if (index.hasAllPermutations()) {
    permutationSettings.push_back({{SPO, SOP}, false});
    permutationSettings.push_back({{OPS, OSP}, false});
  }

  for (const auto& [permutationEnums, isInternal] : permutationSettings) {
    auto [a, b] = permutationEnums;
    auto getPermutation =
        [&index, isInternal](Permutation::Enum permEnum) -> const Permutation& {
      const auto& perm = index.getPermutation(permEnum);
      return isInternal ? perm.internalPermutation() : perm;
    };

    net::co_spawn(threadPool,
                  createPermutationWriterTask(
                      newIndex, getPermutation(a), getPermutation(b),
                      isInternal, locatedTriplesSharedState, localVocabMapping,
                      insertionPositions, blankNodeBlocks, minBlankNodeIndex,
                      cancellationHandle, permutationsProgressCallback),
                  std::ref(exceptionCollector));
  }

  threadPool.join();
  exceptionCollector.rethrowIfException();
  permutationsProgress.finish();

  REBUILD_LOG_INFO << "Index rebuild completed" << std::endl;

#undef REBUILD_LOG_INFO
  return {std::move(insertionPositions), std::move(localVocabMapping),
          std::move(blankNodeBlocks), minBlankNodeIndex};
}

}  // namespace qlever

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
