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
#include <boost/asio/detached.hpp>
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
#include "util/CancellationHandle.h"
#include "util/Exception.h"
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
AD_ALWAYS_INLINE Id remapVocabId(Id original,
                                 const InsertionPositions& insertionPositions) {
  AD_EXPENSIVE_CHECK(
      original.getDatatype() == Datatype::VocabIndex,
      "Only ids resembling a vocab index can be remapped with this function.");
  size_t offset = ql::ranges::distance(
      insertionPositions.begin(),
      ql::ranges::upper_bound(insertionPositions, original.getVocabIndex(),
                              std::less{}));
  return Id::makeFromVocabIndex(
      VocabIndex::make(original.getVocabIndex().get() + offset));
}

// _____________________________________________________________________________
Id remapBlankNodeId(Id original, const BlankNodeBlocks& blankNodeBlocks,
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
  AD_EXPENSIVE_CHECK(it != blankNodeBlocks.end() && *it == blockIndex,
                     "Could not find block index of blank node.");
  auto relativeId = normalizedId % ad_utility::BlankNodeManager::blockSize_;
  auto blockOffset = ql::ranges::distance(blankNodeBlocks.begin(), it) *
                     ad_utility::BlankNodeManager::blockSize_;
  return Id::makeFromBlankNodeIndex(
      BlankNodeIndex::make(relativeId + blockOffset + minBlankNodeIndex));
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
  auto fullScan = permutation.lazyScan(
      scanSpecAndBlocks, std::nullopt, additionalColumns, cancellationHandle,
      *locatedTriplesSharedState, LimitOffsetClause{});

  auto remapId = [&insertionPositions, &localVocabMapping, &blankNodeBlocks,
                  minBlankNodeIndex](Id& id) {
    // TODO<RobinTF> Experiment with caching the last remapped id
    // and reusing it if the same id appears again. See if that
    // improves performance or if it makes it worse.
    using enum Datatype;
    auto datatype = id.getDatatype();
    if (datatype == VocabIndex) [[likely]] {
      id = remapVocabId(id, insertionPositions);
    } else if (datatype == LocalVocabIndex) {
      id = localVocabMapping.at(id.getBits());
    } else if (datatype == BlankNodeIndex) {
      id = remapBlankNodeId(id, blankNodeBlocks, minBlankNodeIndex);
    }
  };

  return ad_utility::InputRangeTypeErased{
      ad_utility::CachingTransformInputRange{
          std::move(fullScan),
          [remapId = std::move(remapId)](IdTable& idTable) {
            // TODO<RobinTF> process columns in parallel.
            auto allCols = idTable.getColumns();
            // Extra columns beyond the graph column only contain integers (or
            // undefined for triples added via UPDATE) and thus don't need to be
            // remapped.
            constexpr size_t REGULAR_COLUMNS = 4;
            for (auto col : allCols | ::ranges::views::take(REGULAR_COLUMNS)) {
              ql::ranges::for_each(col, remapId);
            }
            AD_CORRECTNESS_CHECK(ql::ranges::all_of(
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
    const InsertionPositions& insertionPositions,
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
void materializeToIndex(
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
  const auto& [insertionPositions, localVocabMapping] =
      materializeLocalVocab(entries, index.getVocab(), newIndexName);

  REBUILD_LOG_INFO << "Recomputing statistics ..." << std::endl;

  auto newStats = index.recomputeStatistics(locatedTriplesSharedState);

  auto minBlankNodeIndex = index.getBlankNodeManager()->minIndex_;

  // Set newer lower bound for dynamic blank node indices.
  newStats["num-blank-nodes-total"] =
      minBlankNodeIndex +
      blankNodeBlocks.size() * ad_utility::BlankNodeManager::blockSize_;

  IndexImpl newIndex{index.allocator(), false};
  newIndex.loadConfigFromOldIndex(newIndexName, index, newStats);

  REBUILD_LOG_INFO << "Writing new permutations ..." << std::endl;

  auto patternThreads = static_cast<size_t>(index.usePatterns());
  size_t numberOfPermutations = index.hasAllPermutations() ? 8 : 4;
  namespace net = boost::asio;
  net::thread_pool threadPool{patternThreads + numberOfPermutations};

  if (index.usePatterns()) {
    net::post(threadPool, [&newIndex, &index, &insertionPositions]() {
      newIndex.getPatterns() = index.getPatterns().cloneAndRemap(
          [&insertionPositions](const Id& oldId) {
            return remapVocabId(oldId, insertionPositions);
          });
      newIndex.writePatternsToFile();
    });
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
        net::detached);
  }

  threadPool.join();

  REBUILD_LOG_INFO << "Index rebuild completed" << std::endl;

#undef REBUILD_LOG_INFO
}

}  // namespace qlever

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
