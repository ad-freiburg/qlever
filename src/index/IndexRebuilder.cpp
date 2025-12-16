// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "index/IndexRebuilder.h"

#include <array>
#include <cstdint>
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

namespace {
using CancellationHandle = ad_utility::SharedCancellationHandle;

// _____________________________________________________________________________
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
  return std::pair{std::move(insertInfo), std::move(localVocabMapping)};
}

// _____________________________________________________________________________
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

// _____________________________________________________________________________
ad_utility::InputRangeTypeErased<IdTableStatic<0>> readIndexAndRemap(
    const Permutation& permutation, ScanSpecification scanSpec,
    const BlockMetadataRanges& blockMetadataRanges,
    const LocatedTriplesSnapshot& snapshot,
    const ad_utility::HashMap<Id, Id>& localVocabMapping,
    const std::vector<std::tuple<VocabIndex, std::string_view, Id>>& insertInfo,
    const ad_utility::SharedCancellationHandle& cancellationHandle,
    ql::span<const ColumnIndex> additionalColumns) {
  Permutation::ScanSpecAndBlocks scanSpecAndBlocks{std::move(scanSpec),
                                                   blockMetadataRanges};
  auto fullScan =
      permutation.lazyScan(scanSpecAndBlocks, std::nullopt, additionalColumns,
                           cancellationHandle, snapshot, LimitOffsetClause{});
  auto keyOrder = Permutation::toKeyOrder(permutation.permutation());
  std::vector<ColumnIndex> columnIndices{keyOrder.keys().begin(),
                                         keyOrder.keys().end()};
  while (columnIndices.size() < additionalColumns.size() + 3) {
    columnIndices.emplace_back(columnIndices.size());
  }
  return ad_utility::InputRangeTypeErased{
      ad_utility::CachingTransformInputRange{
          std::move(fullScan),
          [columnIndices = std::move(columnIndices), &localVocabMapping,
           &insertInfo](IdTable& idTable) {
            AD_CORRECTNESS_CHECK(idTable.numColumns() == columnIndices.size());
            idTable.setColumnSubset(columnIndices);
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

ad_utility::InputRangeTypeErased<IdTableStatic<0>> readIndexAndRemap(
    const Permutation& permutation, ScanSpecification scanSpec,
    const LocatedTriplesSnapshot& snapshot,
    const ad_utility::HashMap<Id, Id>& localVocabMapping,
    const std::vector<std::tuple<VocabIndex, std::string_view, Id>>& insertInfo,
    const ad_utility::SharedCancellationHandle& cancellationHandle) {
  return readIndexAndRemap(
      permutation, std::move(scanSpec),
      permutation.getAugmentedMetadataForPermutation(snapshot), snapshot,
      localVocabMapping, insertInfo, cancellationHandle,
      std::array{static_cast<ColumnIndex>(ADDITIONAL_COLUMN_GRAPH_ID)});
}

size_t getNumColumns(const BlockMetadataRanges& blockMetadataRanges) {
  if (!blockMetadataRanges.empty()) {
    const auto& first = blockMetadataRanges.at(0);
    if (!first.empty()) {
      return first[0].offsetsAndCompressedSize_.size();
    }
  }
  return 4;
}
}  // namespace

// _____________________________________________________________________________
namespace qlever {
void materializeToIndex(const IndexImpl& index, const std::string& newIndexName,
                        const std::vector<LocalVocabIndex>& entries,
                        const SharedLocatedTriplesSnapshot& snapshot,
                        const CancellationHandle& cancellationHandle) {
  const auto& [insertInfo, localVocabMapping] =
      materializeLocalVocab(entries, index.getVocab(), newIndexName);

  ScanSpecification scanSpec{std::nullopt, std::nullopt, std::nullopt};
  IndexImpl newIndex{index.allocator(), false};
  newIndex.loadConfigFromOldIndex(newIndexName, index);

  if (index.usePatterns()) {
    newIndex.getPatterns() =
        index.getPatterns().cloneAndRemap([&insertInfo](const Id& oldId) {
          return remapVocabId(oldId, insertInfo);
        });
    newIndex.writePatternsToFile();
  }

  if (index.hasAllPermutations()) {
    newIndex.createSPOAndSOPPublic(
        4, readIndexAndRemap(index.getPermutation(Permutation::Enum::SPO),
                             scanSpec, *snapshot, localVocabMapping, insertInfo,
                             cancellationHandle));
    // TODO<RobinTF> Find out why we can't use createOSPAndOPSPublic here.
    newIndex.createPermutationPairPublic(
        4,
        readIndexAndRemap(index.getPermutation(Permutation::Enum::OPS),
                          scanSpec, *snapshot, localVocabMapping, insertInfo,
                          cancellationHandle),
        newIndex.getPermutation(Permutation::Enum::OPS),
        newIndex.getPermutation(Permutation::Enum::OSP));
  }

  auto [numTriplesInternal, numPredicatesInternal] =
      newIndex.createInternalPSOandPOSFromRange(readIndexAndRemap(
          index.getPermutation(Permutation::Enum::PSO).internalPermutation(),
          scanSpec, *snapshot, localVocabMapping, insertInfo,
          cancellationHandle));

  const auto& psoPermutation = index.getPermutation(Permutation::Enum::PSO);
  auto blockMetadataRanges =
      psoPermutation.getAugmentedMetadataForPermutation(*snapshot);
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
  newIndex.createPSOAndPOSImplPublic(
      numColumns,
      readIndexAndRemap(psoPermutation, scanSpec, blockMetadataRanges,
                        *snapshot, localVocabMapping, insertInfo,
                        cancellationHandle, additionalColumns));

  // TODO<RobinTF> explicitly set these two
  // newIndex.configurationJson_["has-all-permutations"] =
  // index_.hasAllPermutations();
  // newIndex.configurationJson_["num-blank-nodes-total"] = TBD;
  newIndex.addInternalStatisticsToConfiguration(numTriplesInternal,
                                                numPredicatesInternal);
}

}  // namespace qlever
