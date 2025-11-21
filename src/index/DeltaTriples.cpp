// Copyright 2023 - 2025 The QLever Authors, in particular:
//
// 2023 - 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2024 - 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
// 2024 - 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "index/DeltaTriples.h"

#include <absl/strings/str_cat.h>

#include "backports/algorithm.h"
#include "engine/ExecuteUpdate.h"
#include "index/Index.h"
#include "index/IndexImpl.h"
#include "index/LocatedTriples.h"
#include "util/Serializer/TripleSerializer.h"

// ____________________________________________________________________________
LocatedTriples::iterator& DeltaTriples::LocatedTripleHandles::forPermutation(
    Permutation::Enum permutation) {
  return handles_[static_cast<size_t>(permutation)];
}

// ____________________________________________________________________________
void DeltaTriples::clear() {
  triplesInserted_.clear();
  triplesDeleted_.clear();
  ql::ranges::for_each(locatedTriples(), &LocatedTriplesPerBlock::clear);
}

// ____________________________________________________________________________
std::vector<DeltaTriples::LocatedTripleHandles>
DeltaTriples::locateAndAddTriples(CancellationHandle cancellationHandle,
                                  ql::span<const IdTriple<0>> triples,
                                  bool insertOrDelete,
                                  ad_utility::timer::TimeTracer& tracer) {
  std::array<std::vector<LocatedTriples::iterator>, Permutation::ALL.size()>
      intermediateHandles;
  for (auto permutation : Permutation::ALL) {
    tracer.beginTrace(std::string{Permutation::toString(permutation)});
    tracer.beginTrace("locateTriples");
    auto& perm = index_.getPermutation(permutation);
    auto locatedTriples = LocatedTriple::locateTriplesInPermutation(
        triples, perm.metaData().blockData(), perm.keyOrder(), insertOrDelete,
        cancellationHandle);
    cancellationHandle->throwIfCancelled();
    tracer.endTrace("locateTriples");
    tracer.beginTrace("addToLocatedTriples");
    intermediateHandles[static_cast<size_t>(permutation)] =
        this->locatedTriples()[static_cast<size_t>(permutation)].add(
            locatedTriples, tracer);
    cancellationHandle->throwIfCancelled();
    tracer.endTrace("addToLocatedTriples");
    tracer.endTrace(Permutation::toString(permutation));
  }
  tracer.beginTrace("transformHandles");
  std::vector<DeltaTriples::LocatedTripleHandles> handles{triples.size()};
  for (auto permutation : Permutation::ALL) {
    for (size_t i = 0; i < triples.size(); i++) {
      handles[i].forPermutation(permutation) =
          intermediateHandles[static_cast<size_t>(permutation)][i];
    }
  }
  tracer.endTrace("transformHandles");
  return handles;
}

// ____________________________________________________________________________
void DeltaTriples::eraseTripleInAllPermutations(LocatedTripleHandles& handles) {
  // Erase for all permutations.
  for (auto permutation : Permutation::ALL) {
    auto ltIter = handles.forPermutation(permutation);
    locatedTriples()[static_cast<int>(permutation)].erase(ltIter->blockIndex_,
                                                          ltIter);
  }
}

// ____________________________________________________________________________
DeltaTriplesCount DeltaTriples::getCounts() const {
  return {numInserted(), numDeleted()};
}

// ____________________________________________________________________________
void DeltaTriples::insertTriples(CancellationHandle cancellationHandle,
                                 Triples triples,
                                 ad_utility::timer::TimeTracer& tracer) {
  AD_LOG_DEBUG << "Inserting"
               << " " << triples.size()
               << " triples (including idempotent triples)." << std::endl;
  modifyTriplesImpl(std::move(cancellationHandle), std::move(triples), true,
                    triplesInserted_, triplesDeleted_, tracer);
}

// ____________________________________________________________________________
void DeltaTriples::deleteTriples(CancellationHandle cancellationHandle,
                                 Triples triples,
                                 ad_utility::timer::TimeTracer& tracer) {
  AD_LOG_DEBUG << "Deleting"
               << " " << triples.size()
               << " triples (including idempotent triples)." << std::endl;
  modifyTriplesImpl(std::move(cancellationHandle), std::move(triples), false,
                    triplesDeleted_, triplesInserted_, tracer);
}

// ____________________________________________________________________________
void DeltaTriples::rewriteLocalVocabEntriesAndBlankNodes(Triples& triples) {
  // Remember which original blank node (from the parsing of an insert
  // operation) is mapped to which blank node managed by the `localVocab_` of
  // this class.
  ad_utility::HashMap<Id, Id> blankNodeMap;
  // For the given original blank node `id`, check if it has already been
  // mapped. If not, map it to a new blank node managed by the `localVocab_` of
  // this class. Either way, return the (already existing or newly created)
  // value.
  auto getLocalBlankNode = [this, &blankNodeMap](Id id) {
    AD_CORRECTNESS_CHECK(id.getDatatype() == Datatype::BlankNodeIndex);
    // The following code handles both cases (already mapped or not) with a
    // single lookup in the map. Note that the value of the `try_emplace` call
    // is irrelevant.
    auto [it, newElement] = blankNodeMap.try_emplace(id, Id::makeUndefined());
    if (newElement) {
      it->second = Id::makeFromBlankNodeIndex(
          localVocab_.getBlankNodeIndex(index_.getBlankNodeManager()));
    }
    return it->second;
  };

  // Return true iff `blankNodeIndex` is a blank node index from the original
  // index.
  auto isGlobalBlankNode = [minLocalBlankNode =
                                index_.getBlankNodeManager()->minIndex_](
                               BlankNodeIndex blankNodeIndex) {
    return blankNodeIndex.get() < minLocalBlankNode;
  };

  // Helper lambda that converts a single local vocab or blank node `id` as
  // described in the comment for this function. All other types are left
  // unchanged.
  auto convertId = [this, isGlobalBlankNode, &getLocalBlankNode](Id& id) {
    if (id.getDatatype() == Datatype::LocalVocabIndex) {
      id = Id::makeFromLocalVocabIndex(
          localVocab_.getIndexAndAddIfNotContained(*id.getLocalVocabIndex()));
    } else if (id.getDatatype() == Datatype::BlankNodeIndex) {
      auto idx = id.getBlankNodeIndex();
      if (isGlobalBlankNode(idx) ||
          localVocab_.isBlankNodeIndexContained(idx)) {
        return;
      }
      id = getLocalBlankNode(id);
    }
  };

  // Convert all local vocab and blank node `Id`s in all `triples`.
  ql::ranges::for_each(triples, [&convertId](IdTriple<0>& triple) {
    ql::ranges::for_each(triple.ids(), convertId);
    ql::ranges::for_each(triple.payload(), convertId);
  });
}

// ____________________________________________________________________________
void DeltaTriples::modifyTriplesImpl(CancellationHandle cancellationHandle,
                                     Triples triples, bool insertOrDelete,
                                     TriplesToHandlesMap& targetMap,
                                     TriplesToHandlesMap& inverseMap,
                                     ad_utility::timer::TimeTracer& tracer) {
  tracer.beginTrace("rewriteLocalVocabEntries");
  rewriteLocalVocabEntriesAndBlankNodes(triples);
  tracer.endTrace("rewriteLocalVocabEntries");
  AD_EXPENSIVE_CHECK(ql::ranges::is_sorted(triples));
  AD_EXPENSIVE_CHECK(std::unique(triples.begin(), triples.end()) ==
                     triples.end());
  tracer.beginTrace("removeExistingTriples");
  ql::erase_if(triples, [&targetMap](const IdTriple<0>& triple) {
    return targetMap.contains(triple);
  });
  tracer.endTrace("removeExistingTriples");
  tracer.beginTrace("removeInverseTriples");
  ql::ranges::for_each(triples, [this, &inverseMap](const IdTriple<0>& triple) {
    auto handle = inverseMap.find(triple);
    if (handle != inverseMap.end()) {
      eraseTripleInAllPermutations(handle->second);
      inverseMap.erase(triple);
    }
  });
  tracer.endTrace("removeInverseTriples");
  tracer.beginTrace("locatedAndAdd");

  std::vector<LocatedTripleHandles> handles = locateAndAddTriples(
      std::move(cancellationHandle), triples, insertOrDelete, tracer);
  tracer.endTrace("locatedAndAdd");
  tracer.beginTrace("markTriples");

  AD_CORRECTNESS_CHECK(triples.size() == handles.size());
  // TODO<qup42>: replace with ql::views::zip in C++23
  for (size_t i = 0; i < triples.size(); i++) {
    targetMap.insert({triples[i], handles[i]});
  }
  tracer.endTrace("markTriples");
}

// ____________________________________________________________________________
const LocatedTriplesPerBlock&
LocatedTriplesSnapshot::getLocatedTriplesForPermutation(
    Permutation::Enum permutation) const {
  return locatedTriplesPerBlock_[static_cast<int>(permutation)];
}

// ____________________________________________________________________________
SharedLocatedTriplesSnapshot DeltaTriples::getSnapshot() {
  // NOTE: Both members of the `LocatedTriplesSnapshot` are copied, but the
  // `localVocab_` has no copy constructor (in order to avoid accidental
  // copies), hence the explicit `clone`.
  auto snapshotIndex = nextSnapshotIndex_;
  ++nextSnapshotIndex_;
  return SharedLocatedTriplesSnapshot{std::make_shared<LocatedTriplesSnapshot>(
      locatedTriples(), localVocab_.getLifetimeExtender(), snapshotIndex)};
}

// ____________________________________________________________________________
void to_json(nlohmann::json& j, const DeltaTriplesCount& count) {
  j = nlohmann::json{{"inserted", count.triplesInserted_},
                     {"deleted", count.triplesDeleted_},
                     {"total", count.triplesInserted_ + count.triplesDeleted_}};
}

// ____________________________________________________________________________
DeltaTriplesCount operator-(const DeltaTriplesCount& lhs,
                            const DeltaTriplesCount& rhs) {
  return {lhs.triplesInserted_ - rhs.triplesInserted_,
          lhs.triplesDeleted_ - rhs.triplesDeleted_};
}

// ____________________________________________________________________________
DeltaTriples::DeltaTriples(const Index& index)
    : DeltaTriples(index.getImpl()) {}

// ____________________________________________________________________________
DeltaTriplesManager::DeltaTriplesManager(const IndexImpl& index)
    : deltaTriples_{index},
      currentLocatedTriplesSnapshot_{deltaTriples_.wlock()->getSnapshot()} {}

// _____________________________________________________________________________
template <typename ReturnType>
ReturnType DeltaTriplesManager::modify(
    const std::function<ReturnType(DeltaTriples&)>& function,
    bool writeToDiskAfterRequest, bool updateMetadataAfterRequest,
    ad_utility::timer::TimeTracer& tracer) {
  // While holding the lock for the underlying `DeltaTriples`, perform the
  // actual `function` (typically some combination of insert and delete
  // operations) and (while still holding the lock) update the
  // `currentLocatedTriplesSnapshot_`.
  tracer.beginTrace("acquiringDeltaTriplesWriteLock");
  return deltaTriples_.withWriteLock([this, &function, writeToDiskAfterRequest,
                                      updateMetadataAfterRequest,
                                      &tracer](DeltaTriples& deltaTriples) {
    auto updateSnapshot = [this, &deltaTriples] {
      auto newSnapshot = deltaTriples.getSnapshot();
      currentLocatedTriplesSnapshot_.withWriteLock(
          [&newSnapshot](auto& currentSnapshot) {
            currentSnapshot = std::move(newSnapshot);
          });
    };
    auto writeAndUpdateSnapshot = [&updateSnapshot, &deltaTriples, &tracer,
                                   writeToDiskAfterRequest]() {
      if (writeToDiskAfterRequest) {
        tracer.beginTrace("diskWriteback");
        deltaTriples.writeToDisk();
        tracer.endTrace("diskWriteback");
      }
      tracer.beginTrace("snapshotCreation");
      updateSnapshot();
      tracer.endTrace("snapshotCreation");
    };
    auto updateMetadata = [&tracer, &deltaTriples,
                           updateMetadataAfterRequest]() {
      if (updateMetadataAfterRequest) {
        tracer.beginTrace("updateMetadata");
        deltaTriples.updateAugmentedMetadata();
        tracer.endTrace("updateMetadata");
      }
    };

    tracer.endTrace("acquiringDeltaTriplesWriteLock");
    if constexpr (std::is_void_v<ReturnType>) {
      function(deltaTriples);
      updateMetadata();
      writeAndUpdateSnapshot();
    } else {
      ReturnType returnValue = function(deltaTriples);
      updateMetadata();
      writeAndUpdateSnapshot();
      return returnValue;
    }
  });
}
// Explicit instantiations
template void DeltaTriplesManager::modify<void>(
    std::function<void(DeltaTriples&)> const&, bool writeToDiskAfterRequest,
    bool updateMetadataAfterRequest, ad_utility::timer::TimeTracer&);
template UpdateMetadata DeltaTriplesManager::modify<UpdateMetadata>(
    const std::function<UpdateMetadata(DeltaTriples&)>&,
    bool writeToDiskAfterRequest, bool updateMetadataAfterRequest,
    ad_utility::timer::TimeTracer&);
template DeltaTriplesCount DeltaTriplesManager::modify<DeltaTriplesCount>(
    const std::function<DeltaTriplesCount(DeltaTriples&)>&,
    bool writeToDiskAfterRequest, bool updateMetadataAfterRequest,
    ad_utility::timer::TimeTracer&);

// _____________________________________________________________________________
void DeltaTriplesManager::clear() { modify<void>(&DeltaTriples::clear); }

// _____________________________________________________________________________
SharedLocatedTriplesSnapshot DeltaTriplesManager::getCurrentSnapshot() const {
  return *currentLocatedTriplesSnapshot_.rlock();
}

// _____________________________________________________________________________
void DeltaTriples::setOriginalMetadata(
    Permutation::Enum permutation,
    std::shared_ptr<const std::vector<CompressedBlockMetadata>> metadata) {
  locatedTriples()
      .at(static_cast<size_t>(permutation))
      .setOriginalMetadata(std::move(metadata));
}

// _____________________________________________________________________________
void DeltaTriples::updateAugmentedMetadata() {
  ql::ranges::for_each(locatedTriples(),
                       &LocatedTriplesPerBlock::updateAugmentedMetadata);
}

// _____________________________________________________________________________
void DeltaTriples::writeToDisk() const {
  if (!filenameForPersisting_.has_value()) {
    return;
  }
  auto toRange = [](const TriplesToHandlesMap& map) {
    return map | ql::views::keys |
           ql::views::transform(
               [](const IdTriple<0>& triple) -> const std::array<Id, 4>& {
                 return triple.ids();
               }) |
           ql::views::join;
  };
  std::filesystem::path tempPath = filenameForPersisting_.value();
  tempPath += ".tmp";
  ad_utility::serializeIds(
      tempPath, localVocab_,
      std::array{toRange(triplesDeleted_), toRange(triplesInserted_)});
  std::filesystem::rename(tempPath, filenameForPersisting_.value());
}

// _____________________________________________________________________________
void DeltaTriples::readFromDisk() {
  if (!filenameForPersisting_.has_value()) {
    return;
  }
  AD_CONTRACT_CHECK(localVocab_.empty());
  auto [vocab, idRanges] = ad_utility::deserializeIds(
      filenameForPersisting_.value(), index_.getBlankNodeManager());
  if (idRanges.empty()) {
    return;
  }
  AD_CORRECTNESS_CHECK(idRanges.size() == 2);
  auto toTriples = [](const std::vector<Id>& ids) {
    Triples triples;
    static_assert(Triples::value_type::PayloadSize == 0);
    constexpr size_t cols = Triples::value_type::NumCols;
    AD_CORRECTNESS_CHECK(ids.size() % cols == 0);
    triples.reserve(ids.size() / cols);
    for (size_t i = 0; i < ids.size(); i += cols) {
      triples.emplace_back(
          std::array{ids[i], ids[i + 1], ids[i + 2], ids[i + 3]});
    }
    return triples;
  };
  auto cancellationHandle =
      std::make_shared<CancellationHandle::element_type>();
  insertTriples(cancellationHandle, toTriples(idRanges.at(1)));
  deleteTriples(cancellationHandle, toTriples(idRanges.at(0)));
  AD_LOG_INFO << "Done, #inserted triples = " << idRanges.at(1).size()
              << ", #deleted triples = " << idRanges.at(0).size() << std::endl;
}

// _____________________________________________________________________________
void DeltaTriples::setPersists(std::optional<std::string> filename) {
  filenameForPersisting_ = std::move(filename);
}

// _____________________________________________________________________________
void DeltaTriplesManager::setFilenameForPersistentUpdatesAndReadFromDisk(
    std::string filename) {
  modify<void>(
      [&filename](DeltaTriples& deltaTriples) {
        deltaTriples.setPersists(std::move(filename));
        deltaTriples.readFromDisk();
      },
      false);
}

std::pair<std::vector<std::tuple<VocabIndex, std::string_view, Id>>,
          ad_utility::HashMap<Id, Id>>
DeltaTriples::materializeLocalVocab() const {
  auto& vocab = index_.getVocab();

  size_t newWordCount = 0;
  std::vector<std::tuple<VocabIndex, std::string_view, Id>> insertInfo;
  insertInfo.reserve(localVocab_.size());

  ad_utility::HashMap<Id, Id> localVocabMapping;

  for (const LocalVocabEntry& entry : localVocab_.primaryWordSet()) {
    const auto& [lower, upper] = entry.positionInVocab();
    AD_CORRECTNESS_CHECK(lower == upper);
    Id id = Id::fromBits(upper.get());
    AD_CORRECTNESS_CHECK(id.getDatatype() == Datatype::VocabIndex);
    insertInfo.emplace_back(id.getVocabIndex(),
                            entry.asLiteralOrIri().toStringRepresentation(),
                            Id::makeFromLocalVocabIndex(&entry));
  }
  ql::ranges::sort(insertInfo, [](const auto& tupleA, const auto& tupleB) {
    return std::tie(std::get<VocabIndex>(tupleA).get(), std::get<Id>(tupleA)) <
           std::tie(std::get<VocabIndex>(tupleB).get(), std::get<Id>(tupleB));
  });

  auto vocabWriter = vocab.makeWordWriterPtr("tmp_index.vocabulary");
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

namespace {
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

ad_utility::InputRangeTypeErased<IdTableStatic<0>> readIndexAndRemap(
    const Permutation& permutation, ScanSpecification scanSpec,
    BlockMetadataRanges blockMetadataRanges,
    const LocatedTriplesSnapshot& snapshot,
    const ad_utility::HashMap<Id, Id>& localVocabMapping,
    const std::vector<std::tuple<VocabIndex, std::string_view, Id>>& insertInfo,
    const ad_utility::SharedCancellationHandle& cancellationHandle,
    ql::span<const ColumnIndex> additionalColumns) {
  Permutation::ScanSpecAndBlocks scanSpecAndBlocks{
      std::move(scanSpec), std::move(blockMetadataRanges)};
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
void DeltaTriples::materializeToIndex(
    const CancellationHandle& cancellationHandle) {
  const auto& [insertInfo, localVocabMapping] = materializeLocalVocab();

  // TODO<RobinTF> Move much of this logic to `IndexImpl`. This way the "public"
  // wrappers can be avoided.
  ScanSpecification scanSpec{std::nullopt, std::nullopt, std::nullopt};
  auto snapshot = getSnapshot();
  IndexImpl newIndex{index_.allocator(), false};
  newIndex.loadConfigFromOldIndex("tmp_index", index_);

  if (index_.usePatterns()) {
    newIndex.getPatterns() =
        index_.getPatterns().cloneAndRemap([&insertInfo](const Id& oldId) {
          return remapVocabId(oldId, insertInfo);
        });
    newIndex.writePatternsToFile();
  }

  if (index_.hasAllPermutations()) {
    newIndex.createSPOAndSOPPublic(
        4, readIndexAndRemap(index_.getPermutation(Permutation::Enum::SPO),
                             scanSpec, *snapshot, localVocabMapping, insertInfo,
                             cancellationHandle));
    // TODO<RobinTF> Find out why we can't use createOSPAndOPSPublic here.
    newIndex.createPermutationPairPublic(
        4,
        readIndexAndRemap(index_.getPermutation(Permutation::Enum::OPS),
                          scanSpec, *snapshot, localVocabMapping, insertInfo,
                          cancellationHandle),
        newIndex.getPermutation(Permutation::Enum::OPS),
        newIndex.getPermutation(Permutation::Enum::OSP));
  }

  auto [numTriplesInternal, numPredicatesInternal] =
      newIndex.createInternalPSOandPOSFromRange(readIndexAndRemap(
          index_.getPermutation(Permutation::Enum::PSO).internalPermutation(),
          scanSpec, *snapshot, localVocabMapping, insertInfo,
          cancellationHandle));

  const auto& psoPermutation = index_.getPermutation(Permutation::Enum::PSO);
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
      numColumns, readIndexAndRemap(psoPermutation, scanSpec,
                                    std::move(blockMetadataRanges), *snapshot,
                                    localVocabMapping, insertInfo,
                                    cancellationHandle, additionalColumns));

  // TODO<RobinTF> explicitly set these two
  // newIndex.configurationJson_["has-all-permutations"] =
  // index_.hasAllPermutations();
  // newIndex.configurationJson_["num-blank-nodes-total"] = TBD;
  newIndex.addInternalStatisticsToConfiguration(numTriplesInternal,
                                                numPredicatesInternal);
}
