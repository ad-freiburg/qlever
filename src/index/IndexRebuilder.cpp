//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include "index/IndexRebuilder.h"

#include <array>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <cstdint>
#include <fstream>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include "backports/algorithm.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "index/IndexImpl.h"
#include "index/IndexRebuilderImpl.h"
#include "index/LocalVocabEntry.h"
#include "index/Permutation.h"
#include "util/BPlusTree.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/HashMap.h"
#include "util/InputRangeUtils.h"
#include "util/Log.h"

namespace qlever::indexRebuilder {

namespace {

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
                              const std::vector<InsertionInfo>& insertInfo) {
  auto vocabWriter = vocab.makeWordWriterPtr(vocabularyName);
  LocalVocabMapping localVocabMapping;
  auto writeWordFromVocab = [&vocab, &vocabWriter](VocabIndex vocabIndex) {
    auto word = vocab[vocabIndex];
    (*vocabWriter)(word, vocab.shouldBeExternalized(word));
  };
  auto writeWordFromLocalVocab =
      [&vocab, &vocabWriter, &localVocabMapping](const InsertionInfo& info) {
        const auto& [_, word, originalId] = info;
        auto newIndex = (*vocabWriter)(word, vocab.shouldBeExternalized(word));
        localVocabMapping.emplace(
            originalId.getBits(),
            Id::makeFromVocabIndex(VocabIndex::make(newIndex)));
      };
  ad_utility::OverloadCallOperator writer{std::move(writeWordFromVocab),
                                          std::move(writeWordFromLocalVocab)};
  ql::ranges::merge(
      ad_utility::integerRange(vocab.size()) |
          ql::views::transform(&VocabIndex::make),
      insertInfo, ad_utility::IteratorForAssigmentOperator{writer}, {},
      // The tags ensure that the local vocab entries are sorted before all the
      // original vocab entries, even if they share the same vocab index as
      // insertion position.
      [tag = 1](const VocabIndex& index) { return std::tie(index, tag); },
      [tag = 0](const InsertionInfo& info) {
        return std::tie(info.insertionPosition_, tag);
      });
  return localVocabMapping;
}
}  // namespace

// _____________________________________________________________________________
std::tuple<InsertionPositions, LocalVocabMapping> materializeLocalVocab(
    const std::vector<LocalVocabIndex>& entries, const Index::Vocab& vocab,
    const std::string& newIndexName) {
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
      mergeVocabs(newIndexName + VOCAB_SUFFIX, vocab, insertInfo);
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
AD_ALWAYS_INLINE Id
remapVocabId(Id original, const InsertionPositionsTree& insertionPositions) {
  AD_EXPENSIVE_CHECK(
      original.getDatatype() == Datatype::VocabIndex,
      "Only ids resembling a vocab index can be remapped with this function.");
  size_t offset =
      insertionPositions.upperBound<true>(original.getVocabIndex().get())
          .second;
  return Id::makeFromVocabIndex(
      VocabIndex::make(original.getVocabIndex().get() + offset));
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

namespace {

// Scratch buffers reused across `IdTable` blocks and chunks to avoid repeated
// heap allocation inside the column-remapping loop.  `vocabBuf` accumulates
// raw `VocabIndex` values for the batch B+ tree lookup; `vocabResults` holds
// the corresponding remapped `Id` values computed inside the
// `multiUpperBound` callback; `nonVocabBuf` holds eagerly remapped
// non-`VocabIndex` IDs; `bits` records, for each position in the chunk,
// whether the original ID was a `VocabIndex` (1) or not (0), so the scatter
// pass can reconstruct the original order without branching on the type.
struct VocabRemapBuffers {
  std::vector<uint64_t> vocabBuf;
  std::vector<Id> vocabResults;
  std::vector<Id> nonVocabBuf;
  std::vector<uint8_t> bits;
};

// Remaps a non-`VocabIndex` `Id` using the provided mappings.  Returns the
// `Id` unchanged for datatypes other than `LocalVocabIndex` and
// `BlankNodeIndex`.
AD_ALWAYS_INLINE Id remapNonVocabId(Id id, const LocalVocabMapping& lvm,
                                    const BlankNodeBlocks& bnb,
                                    uint64_t minBni) {
  using enum Datatype;
  auto dt = id.getDatatype();
  if (dt == LocalVocabIndex) return lvm.at(id.getBits());
  if (dt == BlankNodeIndex) return remapBlankNodeId(id, bnb, minBni);
  return id;
}

// Remaps any `Id` using the B+ tree for `VocabIndex` and the appropriate
// mapping for other datatypes.  Used in the all-same fast path where a single
// lookup result is broadcast across the whole chunk.
AD_ALWAYS_INLINE Id remapSingleId(Id id, const InsertionPositionsTree& tree,
                                  const LocalVocabMapping& lvm,
                                  const BlankNodeBlocks& bnb, uint64_t minBni) {
  if (id.getDatatype() == Datatype::VocabIndex) return remapVocabId(id, tree);
  return remapNonVocabId(id, lvm, bnb, minBni);
}

// Remaps all IDs in `col` in-place, using `buf` as scratch storage to avoid
// repeated heap allocation.  The column is processed in chunks of
// `INDEX_REBUILD_VOCAB_CHUNK_SIZE` rows; within each chunk the following
// three-pass strategy is used:
//   1. Collect: classify each ID as `VocabIndex` or non-vocab; push raw
//      `uint64_t` values to `buf.vocabBuf` and eagerly remapped non-vocab
//      results to `buf.nonVocabBuf`; record the classification in `buf.bits`.
//   2. Batch lookup: call
//      `multiUpperBound<INDEX_REBUILD_VOCAB_PREFETCH_BATCH>` on
//      `buf.vocabBuf`, computing the remapped `Id` for each entry inside the
//      callback and appending it to `buf.vocabResults`.
//   3. Scatter: write remapped values back to `col` using `buf.bits` to
//      select between `buf.vocabResults` and `buf.nonVocabBuf`.
// If `INDEX_REBUILD_ENABLE_ALLSAME_OPT` is `true`, each chunk is first
// checked for the all-same case (all IDs identical); if so, a single lookup
// fills the whole chunk without going through the three-pass path.
void remapColumnChunked(ql::span<Id> col, VocabRemapBuffers& buf,
                        const InsertionPositionsTree& tree,
                        const LocalVocabMapping& lvm,
                        const BlankNodeBlocks& bnb, uint64_t minBni) {
  constexpr std::size_t CHUNK = INDEX_REBUILD_VOCAB_CHUNK_SIZE;
  constexpr std::size_t BATCH = INDEX_REBUILD_VOCAB_PREFETCH_BATCH;
  for (std::size_t chunkStart = 0; chunkStart < col.size();
       chunkStart += CHUNK) {
    const std::size_t chunkSize = std::min(CHUNK, col.size() - chunkStart);
    auto chunk = col.subspan(chunkStart, chunkSize);
    if constexpr (INDEX_REBUILD_ENABLE_ALLSAME_OPT) {
      Id first = chunk[0];
      if (ql::ranges::adjacent_find(chunk, [](Id a, Id b) {
            return a.getBits() != b.getBits();
          }) == chunk.end()) {
        ql::ranges::fill(chunk, remapSingleId(first, tree, lvm, bnb, minBni));
        continue;
      }
    }
    // Collect pass.
    buf.vocabBuf.clear();
    buf.vocabResults.clear();
    buf.nonVocabBuf.clear();
    buf.bits.clear();
    buf.vocabBuf.reserve(chunkSize);
    buf.vocabResults.reserve(chunkSize);
    buf.nonVocabBuf.reserve(chunkSize);
    buf.bits.reserve(chunkSize);
    for (Id id : chunk) {
      uint8_t b = (id.getDatatype() == Datatype::VocabIndex) ? 1 : 0;
      buf.bits.push_back(b);
      if (b) {
        buf.vocabBuf.push_back(id.getVocabIndex().get());
      } else {
        buf.nonVocabBuf.push_back(remapNonVocabId(id, lvm, bnb, minBni));
      }
    }
    // Batch upper bound.
    std::size_t vi = 0;
    tree.multiUpperBound<BATCH>(buf.vocabBuf, [&buf, &vi](std::size_t rank) {
      buf.vocabResults.push_back(
          Id::makeFromVocabIndex(VocabIndex::make(buf.vocabBuf[vi] + rank)));
      ++vi;
    });
    // Scatter pass: the ternary on `b` compiles to a branch-free CMOV.
    std::size_t vj = 0, nj = 0;
    for (std::size_t k = 0; k < chunkSize; ++k) {
      uint8_t b = buf.bits[k];
      chunk[k] = b ? buf.vocabResults[vj] : buf.nonVocabBuf[nj];
      vj += b;
      nj += static_cast<std::size_t>(!b);
    }
  }
}

}  // namespace

// _____________________________________________________________________________
ad_utility::InputRangeTypeErased<IdTableStatic<0>> readIndexAndRemap(
    const Permutation& permutation,
    const BlockMetadataRanges& blockMetadataRanges,
    const LocatedTriplesSharedState& locatedTriplesSharedState,
    const LocalVocabMapping& localVocabMapping,
    const InsertionPositionsTree& insertionPositions,
    const BlankNodeBlocks& blankNodeBlocks, uint64_t minBlankNodeIndex,
    const ad_utility::SharedCancellationHandle& cancellationHandle,
    ql::span<const ColumnIndex> additionalColumns) {
  AD_CORRECTNESS_CHECK(ql::ranges::is_sorted(blankNodeBlocks));
  Permutation::ScanSpecAndBlocks scanSpecAndBlocks{
      ScanSpecification{std::nullopt, std::nullopt, std::nullopt},
      blockMetadataRanges};
  auto [reader, fullScan] = permutation.lazyScanWithUnlimitedReader(
      scanSpecAndBlocks, additionalColumns, cancellationHandle,
      *locatedTriplesSharedState);

  return ad_utility::InputRangeTypeErased{
      ad_utility::CachingTransformInputRange{
          std::move(fullScan),
          [buffers = VocabRemapBuffers{}, reader = std::move(reader),
           &insertionPositions, &localVocabMapping, &blankNodeBlocks,
           minBlankNodeIndex](IdTable& idTable) mutable {
            auto allCols = idTable.getColumns();
            // Extra columns beyond the graph column only contain integers (or
            // undefined for triples added via UPDATE) and thus don't need to be
            // remapped.
            constexpr size_t REGULAR_COLUMNS = 4;
            for (auto col : allCols | ::ranges::views::take(REGULAR_COLUMNS)) {
              remapColumnChunked(col, buffers, insertionPositions,
                                 localVocabMapping, blankNodeBlocks,
                                 minBlankNodeIndex);
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
template <typename Func>
boost::asio::awaitable<std::invoke_result_t<Func>> asCoroutine(Func func) {
  co_return std::invoke(func);
}
}  // namespace

// _____________________________________________________________________________
boost::asio::awaitable<void> createPermutationWriterTask(
    IndexImpl& newIndex, const Permutation& permutationA,
    const Permutation& permutationB, bool isInternal,
    const LocatedTriplesSharedState& locatedTriplesSharedState,
    const LocalVocabMapping& localVocabMapping,
    const InsertionPositionsTree& insertionPositions,
    const BlankNodeBlocks& blankNodeBlocks, uint64_t minBlankNodeIndex,
    const ad_utility::SharedCancellationHandle& cancellationHandle) {
  namespace net = boost::asio;
  auto ex = co_await net::this_coro::executor;
  auto makeTaskForPermutation = [&](const Permutation& permutation) {
    return [&newIndex, &permutation, isInternal, &locatedTriplesSharedState,
            &localVocabMapping, &insertionPositions, &blankNodeBlocks,
            minBlankNodeIndex, &cancellationHandle]() {
      auto blockMetadataRanges = permutation.getAugmentedMetadataForPermutation(
          *locatedTriplesSharedState);
      auto [numColumns, additionalColumns] =
          getNumberOfColumnsAndAdditionalColumns(blockMetadataRanges);
      return newIndex.createPermutationWithoutMetadata(
          numColumns,
          readIndexAndRemap(
              permutation, blockMetadataRanges, locatedTriplesSharedState,
              localVocabMapping, insertionPositions, blankNodeBlocks,
              minBlankNodeIndex, cancellationHandle, additionalColumns),
          permutation, isInternal);
    };
  };
  auto taskA =
      net::co_spawn(ex, asCoroutine(makeTaskForPermutation(permutationA)),
                    net::use_awaitable);
  auto taskB =
      net::co_spawn(ex, asCoroutine(makeTaskForPermutation(permutationB)),
                    net::use_awaitable);

  auto [_, metaA] = co_await std::move(taskA);
  auto [__, metaB] = co_await std::move(taskB);
  metaA.exchangeMultiplicities(metaB);

  auto makeFinalizerTasks =
      [&newIndex, isInternal](
          IndexImpl::IndexMetaDataMmapDispatcher::WriteType& meta,
          const Permutation& permutation) {
        return [&newIndex, &meta, &permutation, isInternal]() {
          return newIndex.finalizePermutation(meta, permutation, isInternal);
        };
      };
  auto taskC =
      net::co_spawn(ex, asCoroutine(makeFinalizerTasks(metaA, permutationA)),
                    net::use_awaitable);
  auto taskD =
      net::co_spawn(ex, asCoroutine(makeFinalizerTasks(metaB, permutationB)),
                    net::use_awaitable);

  co_await std::move(taskC);
  co_await std::move(taskD);
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

  auto logFile = ad_utility::makeOfstream(logFileName);

  // Macro for rebuild-specific logging with the same syntax as AD_LOG_INFO
#define REBUILD_LOG_INFO \
  logFile << ad_utility::Log::getTimeStamp() << " - INFO: "

  REBUILD_LOG_INFO << "Rebuilding index from current data (including updates)"
                   << std::endl;

  REBUILD_LOG_INFO << "Writing new vocabulary ..." << std::endl;

  auto blankNodeBlocks = flattenBlankNodeBlocks(ownedBlocks);
  auto [rawInsertionPositions, localVocabMapping] =
      materializeLocalVocab(entries, index.getVocab(), newIndexName);

  AD_CORRECTNESS_CHECK(ql::ranges::is_sorted(rawInsertionPositions));
  InsertionPositionsTree insertionPositions(
      rawInsertionPositions |
      ql::views::transform([](const VocabIndex& v) { return v.get(); }));

  REBUILD_LOG_INFO << "Recomputing statistics ..." << std::endl;

  auto newStats = index.recomputeStatistics(locatedTriplesSharedState);

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

  REBUILD_LOG_INFO << "Writing new permutations ..." << std::endl;

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

    net::co_spawn(
        threadPool,
        createPermutationWriterTask(
            newIndex, getPermutation(a), getPermutation(b), isInternal,
            locatedTriplesSharedState, localVocabMapping, insertionPositions,
            blankNodeBlocks, minBlankNodeIndex, cancellationHandle),
        std::ref(exceptionCollector));
  }

  threadPool.join();
  exceptionCollector.rethrowIfException();

  REBUILD_LOG_INFO << "Index rebuild completed" << std::endl;

#undef REBUILD_LOG_INFO
  return {std::move(insertionPositions), std::move(localVocabMapping),
          std::move(blankNodeBlocks), minBlankNodeIndex};
}

}  // namespace qlever

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
