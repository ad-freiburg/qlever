// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "index/IndexRebuilder.h"

#include <array>
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
#include "index/LocalVocabEntry.h"
#include "index/Permutation.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/InputRangeUtils.h"
#include "util/Log.h"

namespace {
using CancellationHandle = ad_utility::SharedCancellationHandle;

// Write a new vocabulary that contains all words from `vocab` plus all
// entries in `entries`. Returns a pair consisting of a vector of tuples
// containing information about the inserted entries (the `VocabIndex` of their
// position in the old `vocab`, the string representation of the newly added
// value, and the original `Id`) and a mapping from old local vocab `Id`s to
// new vocab `Id`s.
std::pair<std::vector<std::tuple<VocabIndex, std::string_view, Id>>,
          ad_utility::HashMap<Id, Id>>
materializeLocalVocab(const std::vector<LocalVocabIndex>& entries,
                      const Index::Vocab& vocab,
                      const std::string& newIndexName) {
  size_t newWordCount = 0;
  std::vector<std::tuple<VocabIndex, std::string_view, Id>> insertInfo;
  insertInfo.reserve(entries.size());

  ad_utility::HashMap<Id, Id> localVocabMapping;

  for (auto* entry : entries) {
    const auto& [lower, upper] = entry->positionInVocab();
    AD_CORRECTNESS_CHECK(lower == upper);
    Id id = Id::fromBits(upper.get());
    AD_CORRECTNESS_CHECK(id.getDatatype() == Datatype::VocabIndex);
    insertInfo.emplace_back(id.getVocabIndex(),
                            entry->asLiteralOrIri().toStringRepresentation(),
                            Id::makeFromLocalVocabIndex(entry));
  }
  ql::ranges::sort(insertInfo, [](const auto& tupleA, const auto& tupleB) {
    return std::tie(std::get<VocabIndex>(tupleA).get(), std::get<Id>(tupleA)) <
           std::tie(std::get<VocabIndex>(tupleB).get(), std::get<Id>(tupleB));
  });

  auto vocabWriter = vocab.makeWordWriterPtr(newIndexName + ".vocabulary");
  for (size_t vocabIndex = 0; vocabIndex < vocab.size(); ++vocabIndex) {
    auto actualIndex = VocabIndex::make(vocabIndex);
    while (insertInfo.size() > newWordCount &&
           std::get<VocabIndex>(insertInfo.at(newWordCount)) == actualIndex) {
      AD_CORRECTNESS_CHECK(std::get<Id>(insertInfo.at(newWordCount)) <
                           Id::makeFromVocabIndex(actualIndex));
      auto word = std::get<std::string_view>(insertInfo.at(newWordCount));
      auto newIndex = (*vocabWriter)(word, vocab.shouldBeExternalized(word));
      localVocabMapping.emplace(
          std::get<Id>(insertInfo.at(newWordCount)),
          Id::makeFromVocabIndex(VocabIndex::make(newIndex)));
      newWordCount++;
    }
    auto word = vocab[actualIndex];
    auto newIndex = (*vocabWriter)(word, vocab.shouldBeExternalized(word));
    AD_CORRECTNESS_CHECK(newIndex == vocabIndex + newWordCount);
  }

  for (const auto& [_, word, id] : insertInfo | ql::views::drop(newWordCount)) {
    auto newIndex = (*vocabWriter)(word, vocab.shouldBeExternalized(word));
    localVocabMapping.emplace(
        id, Id::makeFromVocabIndex(VocabIndex::make(newIndex)));
  }
  return std::make_pair(std::move(insertInfo), std::move(localVocabMapping));
}

// Map old vocab `Id`s to new vocab `Id`s according to the given `insertInfo`.
Id remapVocabId(Id original,
                const std::vector<std::tuple<VocabIndex, std::string_view, Id>>&
                    insertInfo) {
  AD_CONTRACT_CHECK(original.getDatatype() == Datatype::VocabIndex);
  size_t offset = ql::ranges::distance(
      insertInfo.begin(),
      ql::ranges::upper_bound(
          insertInfo, original.getVocabIndex(), std::less{},
          [](const auto& tuple) { return std::get<0>(tuple); }));
  return Id::makeFromVocabIndex(
      VocabIndex::make(original.getVocabIndex().get() + offset));
}

// Create a copy of the given `permutation` scanned according to `scanSpec`,
// where all local vocab `Id`s are remapped according to `localVocabMapping`
// and all vocab `Id`s are remapped according to `insertInfo` to create a new
// index where all of these values are all vocab `Id`s in the new vocabulary.
ad_utility::InputRangeTypeErased<IdTableStatic<0>> readIndexAndRemap(
    const Permutation& permutation, ScanSpecification scanSpec,
    const BlockMetadataRanges& blockMetadataRanges,
    const LocatedTriplesSharedState& locatedTriplesSharedState,
    const ad_utility::HashMap<Id, Id>& localVocabMapping,
    const std::vector<std::tuple<VocabIndex, std::string_view, Id>>& insertInfo,
    const ad_utility::SharedCancellationHandle& cancellationHandle,
    ql::span<const ColumnIndex> additionalColumns) {
  Permutation::ScanSpecAndBlocks scanSpecAndBlocks{std::move(scanSpec),
                                                   blockMetadataRanges};
  auto fullScan = permutation.lazyScan(
      scanSpecAndBlocks, std::nullopt, additionalColumns, cancellationHandle,
      *locatedTriplesSharedState, LimitOffsetClause{});

  return ad_utility::InputRangeTypeErased{
      ad_utility::CachingTransformInputRange{
          std::move(fullScan),
          [&localVocabMapping, &insertInfo](IdTable& idTable) {
            // TODO<RobinTF> process columns in parallel.
            auto allCols = idTable.getColumns();
            // Extra columns beyond the graph column only contain integers (or
            // undefined for triples added via UPDATE) and thus don't need to be
            // remapped.
            constexpr size_t REGULAR_COLUMNS = 4;
            for (auto col : allCols | ::ranges::views::take(REGULAR_COLUMNS)) {
              ql::ranges::for_each(
                  col, [&localVocabMapping, &insertInfo](Id& id) {
                    if (id.getDatatype() == Datatype::LocalVocabIndex) {
                      id = localVocabMapping.at(id);
                    } else if (id.getDatatype() == Datatype::VocabIndex) {
                      id = remapVocabId(id, insertInfo);
                    }
                  });
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

// Overload that automatically retrieves the block metadata ranges for the given
// `permutation` and passes the graph ID as an additional column to be read.
ad_utility::InputRangeTypeErased<IdTableStatic<0>> readIndexAndRemap(
    const Permutation& permutation, ScanSpecification scanSpec,
    const LocatedTriplesSharedState& locatedTriplesSharedState,
    const ad_utility::HashMap<Id, Id>& localVocabMapping,
    const std::vector<std::tuple<VocabIndex, std::string_view, Id>>& insertInfo,
    const ad_utility::SharedCancellationHandle& cancellationHandle) {
  return readIndexAndRemap(
      permutation, std::move(scanSpec),
      permutation.getAugmentedMetadataForPermutation(
          *locatedTriplesSharedState),
      locatedTriplesSharedState, localVocabMapping, insertInfo,
      cancellationHandle,
      std::array{static_cast<ColumnIndex>(ADDITIONAL_COLUMN_GRAPH_ID)});
}

// Get the number of columns in the given `blockMetadataRanges`. If this cannot
// be determined, return 4 as a safe default.
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
}  // namespace

// _____________________________________________________________________________
namespace qlever {
void materializeToIndex(
    const IndexImpl& index, const std::string& newIndexName,
    const std::vector<LocalVocabIndex>& entries,
    const LocatedTriplesSharedState& locatedTriplesSharedState,
    const CancellationHandle& cancellationHandle,
    const std::string& logFileName) {
  AD_CONTRACT_CHECK(!logFileName.empty(), "Log file name must not be empty");

  // Set up logging to file
  std::ofstream logFile{logFileName};
  AD_CORRECTNESS_CHECK(logFile.is_open(),
                       "Failed to open log file: " + logFileName);

  // Macro for rebuild-specific logging with the same syntax as AD_LOG_INFO
#define REBUILD_LOG_INFO \
  logFile << ad_utility::Log::getTimeStamp() << " - INFO: "

  REBUILD_LOG_INFO << "Rebuilding index from current data (including updates)"
                   << std::endl;

  REBUILD_LOG_INFO << "Writing new vocabulary ..." << std::endl;

  const auto& [insertInfo, localVocabMapping] =
      materializeLocalVocab(entries, index.getVocab(), newIndexName);

  REBUILD_LOG_INFO << "Recomputing statistics ..." << std::endl;

  auto newStats = index.recomputeStatistics(locatedTriplesSharedState);

  ScanSpecification scanSpec{std::nullopt, std::nullopt, std::nullopt};
  IndexImpl newIndex{index.allocator(), false};
  newIndex.loadConfigFromOldIndex(newIndexName, index, newStats);

  REBUILD_LOG_INFO << "Writing new permutations ..." << std::endl;

  // TODO<RobinTF> Make sure any exceptions are properly handled and propagated.
  std::vector<ad_utility::JThread> tasks;

  if (index.usePatterns()) {
    tasks.push_back(ad_utility::JThread{[&newIndex, &index, &insertInfo]() {
      newIndex.getPatterns() =
          index.getPatterns().cloneAndRemap([&insertInfo](const Id& oldId) {
            return remapVocabId(oldId, insertInfo);
          });
      newIndex.writePatternsToFile();
    }});
  }

  if (index.hasAllPermutations()) {
    using enum Permutation::Enum;
    for (auto permutation : {SPO, SOP, OPS, OSP}) {
      const auto& actualPermutation = index.getPermutation(permutation);
      tasks.push_back(ad_utility::JThread{
          [&newIndex, &actualPermutation, &scanSpec, &locatedTriplesSharedState,
           &localVocabMapping, &insertInfo, &cancellationHandle]() {
            newIndex.createPermutation(
                4,
                readIndexAndRemap(actualPermutation, scanSpec,
                                  locatedTriplesSharedState, localVocabMapping,
                                  insertInfo, cancellationHandle),
                actualPermutation);
          }});
    }
  }

  for (auto permutation : Permutation::INTERNAL) {
    const auto& actualPermutation = index.getPermutation(permutation);
    const auto& internalPermutation = actualPermutation.internalPermutation();
    tasks.push_back(ad_utility::JThread{
        [&newIndex, &internalPermutation, &scanSpec, &locatedTriplesSharedState,
         &localVocabMapping, &insertInfo, &cancellationHandle]() {
          newIndex.createPermutation(
              4,
              readIndexAndRemap(internalPermutation, scanSpec,
                                locatedTriplesSharedState, localVocabMapping,
                                insertInfo, cancellationHandle),
              internalPermutation, true);
        }});

    auto blockMetadataRanges =
        actualPermutation.getAugmentedMetadataForPermutation(
            *locatedTriplesSharedState);
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
    tasks.push_back(ad_utility::JThread{
        [&newIndex, &actualPermutation, &scanSpec, &locatedTriplesSharedState,
         &localVocabMapping, &insertInfo, &cancellationHandle, numColumns,
         blockMetadataRanges = std::move(blockMetadataRanges),
         additionalColumns = std::move(additionalColumns)]() {
          newIndex.createPermutation(
              numColumns,
              readIndexAndRemap(actualPermutation, scanSpec,
                                blockMetadataRanges, locatedTriplesSharedState,
                                localVocabMapping, insertInfo,
                                cancellationHandle, additionalColumns),
              actualPermutation);
        }});
  }

  // Explicitly wait for all threads to complete before logging completion
  tasks.clear();

  REBUILD_LOG_INFO << "Index rebuild completed" << std::endl;

#undef REBUILD_LOG_INFO
}

}  // namespace qlever
