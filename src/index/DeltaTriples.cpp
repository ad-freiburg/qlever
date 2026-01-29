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
#include "engine/ExportQueryExecutionTrees.h"
#include "index/DeltaTriplesPaths.h"
#include "index/DeltaTriplesWriter.h"
#include "index/Index.h"
#include "index/IndexImpl.h"
#include "index/LocatedTriples.h"
#include "util/LruCache.h"
#include "util/Serializer/TripleSerializer.h"

using namespace ad_utility::delta_triples;

// ____________________________________________________________________________
template <bool isInternal>
const LocatedTriplesPerBlock&
LocatedTriplesState::getLocatedTriplesForPermutation(
    Permutation::Enum permutation) const {
  if constexpr (isInternal) {
    AD_CONTRACT_CHECK(permutation == Permutation::PSO ||
                      permutation == Permutation::POS);
  }
  return getLocatedTriples<isInternal>().at(static_cast<int>(permutation));
}

template const LocatedTriplesPerBlock&
LocatedTriplesState::getLocatedTriplesForPermutation<true>(
    Permutation::Enum permutation) const;
template const LocatedTriplesPerBlock&
LocatedTriplesState::getLocatedTriplesForPermutation<false>(
    Permutation::Enum permutation) const;

// ____________________________________________________________________________
template <bool isInternal>
LocatedTriplesPerBlock& LocatedTriplesState::getLocatedTriplesForPermutation(
    Permutation::Enum permutation) {
  return const_cast<LocatedTriplesPerBlock&>(
      std::as_const(*this).getLocatedTriplesForPermutation<isInternal>(
          permutation));
}

template LocatedTriplesPerBlock&
LocatedTriplesState::getLocatedTriplesForPermutation<true>(
    Permutation::Enum permutation);
template LocatedTriplesPerBlock&
LocatedTriplesState::getLocatedTriplesForPermutation<false>(
    Permutation::Enum permutation);

// ____________________________________________________________________________
template <bool isInternal>
LocatedTriplesPerBlockAllPermutations<isInternal>&
LocatedTriplesState::getLocatedTriples() {
  return const_cast<LocatedTriplesPerBlockAllPermutations<isInternal>&>(
      std::as_const(*this).getLocatedTriples<isInternal>());
}

// ____________________________________________________________________________
template <bool isInternal>
const LocatedTriplesPerBlockAllPermutations<isInternal>&
LocatedTriplesState::getLocatedTriples() const {
  if constexpr (isInternal) {
    return internalLocatedTriplesPerBlock_;
  } else {
    return locatedTriplesPerBlock_;
  }
}

// ____________________________________________________________________________
template <bool isInternal>
LocatedTriples::iterator& DeltaTriples::TriplesToHandles<isInternal>::
    LocatedTripleHandles::forPermutation(Permutation::Enum permutation) {
  return handles_[static_cast<size_t>(permutation)];
}

// ____________________________________________________________________________
void DeltaTriples::clear() {
  auto clearImpl = [](auto& state, auto& locatedTriples) {
    state.triplesInserted_.clear();
    state.triplesDeleted_.clear();
    ql::ranges::for_each(locatedTriples, &LocatedTriplesPerBlock::clear);
  };
  clearImpl(triplesToHandlesNormal_,
            locatedTriples_->getLocatedTriples<false>());
  clearImpl(triplesToHandlesInternal_,
            locatedTriples_->getLocatedTriples<true>());
}

// ____________________________________________________________________________
template <bool isInternal>
DeltaTriples::TriplesToHandles<isInternal>& DeltaTriples::getState() {
  if constexpr (isInternal) {
    return triplesToHandlesInternal_;
  } else {
    return triplesToHandlesNormal_;
  }
}

// ____________________________________________________________________________
template <bool isInternal>
std::vector<
    typename DeltaTriples::TriplesToHandles<isInternal>::LocatedTripleHandles>
DeltaTriples::locateAndAddTriples(CancellationHandle cancellationHandle,
                                  ql::span<const IdTriple<0>> triples,
                                  bool insertOrDelete,
                                  ad_utility::timer::TimeTracer& tracer) {
  constexpr const auto& allPermutations = Permutation::all<isInternal>();
  auto& lt = locatedTriples_->getLocatedTriples<isInternal>();
  std::array<std::vector<LocatedTriples::iterator>, allPermutations.size()>
      intermediateHandles;
  for (auto permutation : allPermutations) {
    tracer.beginTrace(std::string{Permutation::toString(permutation)});
    tracer.beginTrace("locateTriples");
    auto& basePerm = index_.getPermutation(permutation);
    auto& perm = isInternal ? basePerm.internalPermutation() : basePerm;
    auto locatedTriples = LocatedTriple::locateTriplesInPermutation(
        triples, perm.metaData().blockData(), perm.keyOrder(), insertOrDelete,
        cancellationHandle);
    cancellationHandle->throwIfCancelled();
    tracer.endTrace("locateTriples");
    tracer.beginTrace("addToLocatedTriples");
    intermediateHandles[static_cast<size_t>(permutation)] =
        lt[static_cast<size_t>(permutation)].add(locatedTriples, tracer);
    cancellationHandle->throwIfCancelled();
    tracer.endTrace("addToLocatedTriples");
    tracer.endTrace(Permutation::toString(permutation));
  }
  tracer.beginTrace("transformHandles");
  std::vector<typename TriplesToHandles<isInternal>::LocatedTripleHandles>
      handles{triples.size()};
  for (auto permutation : allPermutations) {
    for (size_t i = 0; i < triples.size(); i++) {
      handles[i].forPermutation(permutation) =
          intermediateHandles[static_cast<size_t>(permutation)][i];
    }
  }
  tracer.endTrace("transformHandles");
  return handles;
}

// ____________________________________________________________________________
template <bool isInternal>
void DeltaTriples::eraseTripleInAllPermutations(
    typename TriplesToHandles<isInternal>::LocatedTripleHandles& handles) {
  auto& lt = locatedTriples_->getLocatedTriples<isInternal>();
  // Erase for all permutations.
  for (auto permutation : Permutation::all<isInternal>()) {
    auto ltIter = handles.forPermutation(permutation);
    lt[static_cast<int>(permutation)].erase(ltIter->blockIndex_, ltIter);
  }
}

// ____________________________________________________________________________
DeltaTriplesCount DeltaTriples::getCounts() const {
  return {numInserted(), numDeleted()};
}

// _____________________________________________________________________________
DeltaTriples::Triples DeltaTriples::makeInternalTriples(
    const Triples& triples) {
  // NOTE: If this logic is ever changed, you need to also change the code
  // in `IndexBuilderTypes.h`, the function `getIdMapLambdas` specifically,
  // which adds the same extra triples for language tags to the internal triples
  // on the initial index build.
  Triples internalTriples;
  constexpr size_t predicateCacheSize = 50;
  ad_utility::util::LRUCache<Id::T, ad_utility::triple_component::Iri>
      predicateCache{predicateCacheSize};
  for (const auto& triple : triples) {
    const auto& ids = triple.ids();
    Id objectId = ids.at(2);
    auto optionalLiteralOrIri = ExportQueryExecutionTrees::idToLiteralOrIri(
        index_, objectId, localVocab_, true);
    if (!optionalLiteralOrIri.has_value() ||
        !optionalLiteralOrIri.value().isLiteral() ||
        !optionalLiteralOrIri.value().hasLanguageTag()) {
      continue;
    }
    const auto& predicate =
        predicateCache.getOrCompute(ids.at(1).getBits(), [this](Id::T bits) {
          auto optionalPredicate = ExportQueryExecutionTrees::idToLiteralOrIri(
              index_, Id::fromBits(bits), localVocab_, true);
          AD_CORRECTNESS_CHECK(optionalPredicate.has_value());
          AD_CORRECTNESS_CHECK(optionalPredicate.value().isIri());
          return std::move(optionalPredicate.value().getIri());
        });
    auto specialPredicate = ad_utility::convertToLanguageTaggedPredicate(
        predicate,
        asStringViewUnsafe(optionalLiteralOrIri.value().getLanguageTag()));
    Id specialId = TripleComponent{std::move(specialPredicate)}.toValueId(
        index_.getVocab(), localVocab_, index_.encodedIriManager());
    // NOTE: We currently only add one of the language triples, specifically
    // `<subject> @language@<predicate> "object"@language` because it is
    // directly tied to a regular triple, so insertion and removal work exactly
    // the same. `<object> ql:langtag <@language>` on the other hand needs
    // either some sort of reference counting or we have to keep it
    // indefinitely, even if the object is removed. This means that some queries
    // will return no results for entries with language tags that were inserted
    // via an UPDATE operation.
    internalTriples.push_back(
        IdTriple<0>{std::array{ids.at(0), specialId, ids.at(2), ids.at(3)}});
  }
  // Because of the special predicates, we need to re-sort the triples.
  ql::ranges::sort(internalTriples);
  return internalTriples;
}

// ____________________________________________________________________________
void DeltaTriples::insertTriples(CancellationHandle cancellationHandle,
                                 Triples triples,
                                 ad_utility::timer::TimeTracer& tracer) {
  tracer.beginTrace("makeInternalTriples");
  auto internalTriples = makeInternalTriples(triples);
  tracer.endTrace("makeInternalTriples");
  tracer.beginTrace("externalPermutation");
  modifyTriplesImpl<false, true>(cancellationHandle, std::move(triples),
                                 tracer);
  tracer.endTrace("externalPermutation");
  tracer.beginTrace("internalPermutation");
  modifyTriplesImpl<true, true>(std::move(cancellationHandle),
                                std::move(internalTriples), tracer);
  tracer.endTrace("internalPermutation");
  // Update the index of the located triples to mark that they have changed.
  locatedTriples_->index_++;
}

// ____________________________________________________________________________
void DeltaTriples::deleteTriples(CancellationHandle cancellationHandle,
                                 Triples triples,
                                 ad_utility::timer::TimeTracer& tracer) {
  tracer.beginTrace("makeInternalTriples");
  auto internalTriples = makeInternalTriples(triples);
  tracer.endTrace("makeInternalTriples");
  tracer.beginTrace("externalPermutation");
  modifyTriplesImpl<false, false>(cancellationHandle, std::move(triples),
                                  tracer);
  tracer.endTrace("externalPermutation");
  tracer.beginTrace("internalPermutation");
  modifyTriplesImpl<true, false>(std::move(cancellationHandle),
                                 std::move(internalTriples), tracer);
  tracer.endTrace("internalPermutation");
  // Update the index of the located triples to mark that they have changed.
  locatedTriples_->index_++;
}

// ____________________________________________________________________________
void DeltaTriples::insertInternalTriplesForTesting(
    CancellationHandle cancellationHandle, Triples triples,
    ad_utility::timer::TimeTracer& tracer) {
  modifyTriplesImpl<true, true>(std::move(cancellationHandle),
                                std::move(triples), tracer);
}

// ____________________________________________________________________________
void DeltaTriples::deleteInternalTriplesForTesting(
    CancellationHandle cancellationHandle, Triples triples,
    ad_utility::timer::TimeTracer& tracer) {
  modifyTriplesImpl<true, false>(std::move(cancellationHandle),
                                 std::move(triples), tracer);
}

// ____________________________________________________________________________
void DeltaTriples::rewriteLocalVocabEntriesAndBlankNodes(Triples& triples) {
  // Remember which original blank node (from the parsing of an insert
  // operation) is mapped to which blank node managed by the `localVocab_` of
  // this class.
  ad_utility::HashMap<Id, Id> blankNodeMap;
  // For the given original blank node `id`, check if it has already been
  // mapped. If not, map it to a new blank node managed by the `localVocab_`
  // of this class. Either way, return the (already existing or newly created)
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
template <bool isInternal, bool insertOrDelete>
void DeltaTriples::modifyTriplesImpl(CancellationHandle cancellationHandle,
                                     Triples triples,
                                     ad_utility::timer::TimeTracer& tracer) {
  AD_LOG_DEBUG << (insertOrDelete ? "Inserting" : "Deleting") << " "
               << triples.size() << (isInternal ? " internal" : "")
               << " triples (including idempotent triples)." << std::endl;
  auto [targetMap, inverseMap] = [this]() {
    auto& state = getState<isInternal>();
    if constexpr (insertOrDelete) {
      return std::tie(state.triplesInserted_, state.triplesDeleted_);
    } else {
      return std::tie(state.triplesDeleted_, state.triplesInserted_);
    }
  }();
  tracer.beginTrace("rewriteLocalVocabEntries");
  rewriteLocalVocabEntriesAndBlankNodes(triples);
  tracer.endTrace("rewriteLocalVocabEntries");
  // TODO<joka921> Once the migration is finished, check whether we can remove
  // the `ifndef` here again.
#ifndef QLEVER_CPP_17
  AD_EXPENSIVE_CHECK(ql::ranges::is_sorted(triples));
#endif
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
      eraseTripleInAllPermutations<isInternal>(handle->second);
      inverseMap.erase(triple);
    }
  });
  tracer.endTrace("removeInverseTriples");
  tracer.beginTrace("locatedAndAdd");

  auto handles = locateAndAddTriples<isInternal>(
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
LocatedTriplesSharedState DeltaTriples::getLocatedTriplesSharedStateCopy()
    const {
  // Create a copy of the `LocatedTriplesState` for use as a constant
  // snapshot.
  auto state = std::make_shared<LocatedTriplesState>(
      locatedTriples_->locatedTriplesPerBlock_,
      locatedTriples_->internalLocatedTriplesPerBlock_,
      localVocab_.getLifetimeExtender(), locatedTriples_->index_);
  state->onDiskDeltas_ = getOnDiskDeltas();
  return LocatedTriplesSharedState{std::move(state)};
}

// ____________________________________________________________________________
LocatedTriplesSharedState DeltaTriples::getLocatedTriplesSharedStateReference()
    const {
  // Creating a `shared_ptr<const LocatedTriplesState>` from a
  // `shared_ptr<LocatedTriplesState>` is cheap.
  // Ensure the on-disk deltas pointer is up to date.
  locatedTriples_->onDiskDeltas_ = getOnDiskDeltas();
  return LocatedTriplesSharedState{locatedTriples_};
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
      currentLocatedTriplesSharedState_{
          deltaTriples_.wlock()->getLocatedTriplesSharedStateCopy()} {}

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
      auto newSnapshot = deltaTriples.getLocatedTriplesSharedStateCopy();
      currentLocatedTriplesSharedState_.withWriteLock(
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
        tracer.beginTrace("metadataUpdateForSnapshot");
        deltaTriples.updateAugmentedMetadata();
        tracer.endTrace("metadataUpdateForSnapshot");
      }
    };

    // Check if we should spill to disk after the operation.
    auto checkAndSpillToDisk = [&tracer, &deltaTriples]() {
      if (deltaTriples.shouldSpillToDisk()) {
        deltaTriples.spillToDisk(tracer);
      }
    };

    tracer.endTrace("acquiringDeltaTriplesWriteLock");
    if constexpr (std::is_void_v<ReturnType>) {
      tracer.beginTrace("operations");
      function(deltaTriples);
      tracer.endTrace("operations");
      checkAndSpillToDisk();
      updateMetadata();
      writeAndUpdateSnapshot();
    } else {
      tracer.beginTrace("operations");
      ReturnType returnValue = function(deltaTriples);
      tracer.endTrace("operations");
      checkAndSpillToDisk();
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
template nlohmann::json DeltaTriplesManager::modify<nlohmann::json>(
    const std::function<nlohmann::json(DeltaTriples&)>&,
    bool writeToDiskAfterRequest, bool updateMetadataAfterRequest,
    ad_utility::timer::TimeTracer&);

// _____________________________________________________________________________
void DeltaTriplesManager::clear() { modify<void>(&DeltaTriples::clear); }

// _____________________________________________________________________________
LocatedTriplesSharedState
DeltaTriplesManager::getCurrentLocatedTriplesSharedState() const {
  return *currentLocatedTriplesSharedState_.rlock();
}

// _____________________________________________________________________________
void DeltaTriples::setOriginalMetadata(
    Permutation::Enum permutation,
    std::shared_ptr<const std::vector<CompressedBlockMetadata>> metadata,
    bool setInternalMetadata) {
  auto& locatedTriplesPerBlock =
      setInternalMetadata
          ? locatedTriples_->getLocatedTriplesForPermutation<true>(permutation)
          : locatedTriples_->getLocatedTriplesForPermutation<false>(
                permutation);
  locatedTriplesPerBlock.setOriginalMetadata(std::move(metadata));
}

// _____________________________________________________________________________
void DeltaTriples::updateAugmentedMetadata() {
  auto update = [](auto& lt) {
    ql::ranges::for_each(lt, &LocatedTriplesPerBlock::updateAugmentedMetadata);
  };
  update(locatedTriples_->getLocatedTriples<false>());
  update(locatedTriples_->getLocatedTriples<true>());
}

// _____________________________________________________________________________
void DeltaTriples::writeToDisk() const {
  if (!filenameForPersisting_.has_value()) {
    return;
  }
  // TODO<RobinTF> Currently this only writes non-internal delta triples to
  // disk. The internal triples will be regenerated when importing the rest
  // again. In the future we might to also want to explicitly store the
  // internal triples.
  auto toRange = [](const TriplesToHandles<false>::TriplesToHandlesMap& map) {
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
      std::array{toRange(triplesToHandlesNormal_.triplesDeleted_),
                 toRange(triplesToHandlesNormal_.triplesInserted_)});
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

// _____________________________________________________________________________
void DeltaTriples::setBaseDirForOnDiskDeltas(std::string baseDir) {
  baseDirForOnDiskDeltas_ = std::move(baseDir);

  // Check if on-disk delta files exist. If so, load them.
  OnDiskDeltaTriples tempDeltas{baseDirForOnDiskDeltas_};
  try {
    tempDeltas.loadFromDisk();
    // If loadFromDisk succeeded and found deltas, store them.
    if (tempDeltas.hasOnDiskDeltas()) {
      onDiskDeltas_ = std::move(tempDeltas);
    }
  } catch (const std::exception& e) {
    // If loading fails (e.g., files don't exist), that's fine - we just
    // don't have any on-disk deltas yet.
  }
}

// _____________________________________________________________________________
bool DeltaTriples::shouldSpillToDisk() const {
  size_t totalTriples = numInserted() + numDeleted() + numInternalInserted() +
                        numInternalDeleted();
  return totalTriples >= SPILL_THRESHOLD_NUM_TRIPLES;
}

// _____________________________________________________________________________
void DeltaTriples::spillToDisk(ad_utility::timer::TimeTracer& tracer) {
  tracer.beginTrace("spillToDisk");

  if (baseDirForOnDiskDeltas_.empty()) {
    AD_LOG_WARN << "Cannot spill delta triples to disk: base directory not set."
                << std::endl;
    tracer.endTrace("spillToDisk");
    return;
  }

  AD_LOG_INFO << "Spilling " << (numInserted() + numDeleted())
              << " delta triples to disk (threshold: "
              << SPILL_THRESHOLD_NUM_TRIPLES << ")" << std::endl;

  // If on-disk deltas already exist, we need to rebuild (merge old + new).
  if (onDiskDeltas_.has_value() && onDiskDeltas_->hasOnDiskDeltas()) {
    AD_LOG_INFO << "On-disk deltas already exist, performing rebuild instead."
                << std::endl;
    rebuildOnDiskDeltas(tracer);
    tracer.endTrace("spillToDisk");
    return;
  }

  // Write current in-memory deltas to disk.
  tracer.beginTrace("writeAllPermutations");
  DeltaTriplesWriter writer(index_, baseDirForOnDiskDeltas_);
  writer.writeAllPermutations(locatedTriples_->getLocatedTriples<false>(),
                              locatedTriples_->getLocatedTriples<true>(),
                              false);  // useTemporary = false
  tracer.endTrace("writeAllPermutations");

  // Load the newly written delta files.
  tracer.beginTrace("loadFromDisk");
  onDiskDeltas_.emplace(baseDirForOnDiskDeltas_);
  onDiskDeltas_->loadFromDisk();
  tracer.endTrace("loadFromDisk");

  // Clear in-memory deltas (except LocalVocab which is preserved).
  tracer.beginTrace("clearInMemory");
  clear();
  tracer.endTrace("clearInMemory");

  AD_LOG_INFO << "Spilled delta triples to disk successfully." << std::endl;
  tracer.endTrace("spillToDisk");
}

// _____________________________________________________________________________
void DeltaTriples::rebuildOnDiskDeltas(ad_utility::timer::TimeTracer& tracer) {
  tracer.beginTrace("rebuildOnDiskDeltas");

  AD_LOG_INFO << "Rebuilding on-disk delta files by merging with new "
                 "in-memory deltas."
              << std::endl;

  AD_CORRECTNESS_CHECK(onDiskDeltas_.has_value());
  AD_CORRECTNESS_CHECK(onDiskDeltas_->hasOnDiskDeltas());

  DeltaTriplesWriter writer(index_, baseDirForOnDiskDeltas_);
  auto allocator = ad_utility::makeUnlimitedAllocator<Id>();

  // Helper to merge and write for a single permutation and type
  // (insert/delete).
  auto mergeAndWritePermutation =
      [&](Permutation::Enum permutation, bool isInsert, bool isInternal,
          const LocatedTriplesPerBlock& locatedTriples) {
        tracer.beginTrace(absl::StrCat(Permutation::toString(permutation),
                                       isInsert ? "-inserts" : "-deletes"));

        // 1. Read old on-disk triples.
        tracer.beginTrace("readOldOnDisk");
        IdTable oldTriples = onDiskDeltas_->readAllTriples(
            static_cast<PermutationEnum>(permutation), isInsert, allocator);
        tracer.endTrace("readOldOnDisk");

        // 2. Extract new in-memory triples.
        tracer.beginTrace("extractNewInMemory");
        auto keyOrder = Permutation::toKeyOrder(permutation);
        IdTable newTriples =
            writer.extractAndSortTriples(locatedTriples, keyOrder, isInsert);
        tracer.endTrace("extractNewInMemory");

        // 3. Merge and write to temporary file.
        tracer.beginTrace("mergeAndWrite");
        std::vector<IdTable> tablesToMerge;
        if (oldTriples.numRows() > 0) {
          tablesToMerge.push_back(std::move(oldTriples));
        }
        if (newTriples.numRows() > 0) {
          tablesToMerge.push_back(std::move(newTriples));
        }

        std::string path =
            isInsert
                ? getDeltaTempInsertsPath(baseDirForOnDiskDeltas_, permutation)
                : getDeltaTempDeletesPath(baseDirForOnDiskDeltas_, permutation);

        if (!tablesToMerge.empty()) {
          writer.mergeAndWriteTriples(std::move(tablesToMerge), path);
        } else {
          // Write empty file to maintain consistency.
          writer.mergeAndWriteTriples({}, path);
        }
        tracer.endTrace("mergeAndWrite");

        tracer.endTrace(absl::StrCat(Permutation::toString(permutation),
                                     isInsert ? "-inserts" : "-deletes"));
      };

  // Merge all regular permutations.
  tracer.beginTrace("regularPermutations");
  for (auto permutation : Permutation::ALL) {
    const auto& locatedTriples =
        locatedTriples_
            ->getLocatedTriples<false>()[static_cast<size_t>(permutation)];
    mergeAndWritePermutation(permutation, true, false, locatedTriples);
    mergeAndWritePermutation(permutation, false, false, locatedTriples);
  }
  tracer.endTrace("regularPermutations");

  // Merge all internal permutations.
  tracer.beginTrace("internalPermutations");
  for (auto permutation : Permutation::INTERNAL) {
    const auto& locatedTriples =
        locatedTriples_
            ->getLocatedTriples<true>()[static_cast<size_t>(permutation)];
    mergeAndWritePermutation(permutation, true, true, locatedTriples);
    mergeAndWritePermutation(permutation, false, true, locatedTriples);
  }
  tracer.endTrace("internalPermutations");

  // Commit temporary files (atomic rename).
  tracer.beginTrace("commitTemporaryFiles");
  writer.commitTemporaryFiles();
  tracer.endTrace("commitTemporaryFiles");

  // Reload from disk.
  tracer.beginTrace("reloadFromDisk");
  onDiskDeltas_->loadFromDisk();
  tracer.endTrace("reloadFromDisk");

  // Clear in-memory deltas.
  tracer.beginTrace("clearInMemory");
  clear();
  tracer.endTrace("clearInMemory");

  AD_LOG_INFO << "Rebuilt on-disk delta files successfully." << std::endl;
  tracer.endTrace("rebuildOnDiskDeltas");
}
