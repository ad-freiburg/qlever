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
#include "index/Index.h"
#include "index/IndexImpl.h"
#include "index/LocatedTriples.h"
#include "util/LruCache.h"
#include "util/Serializer/TripleSerializer.h"

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
nlohmann::json DeltaTriples::vacuum() {
  nlohmann::json result = nlohmann::json::object();

  // Phase 1: Process external permutations (6 permutations)
  auto externalResult = identifyTriplesToVacuum<false>();
  removeIdentifiedTriples<false>(externalResult.deletionsToRemove,
                                 externalResult.insertionsToRemove);
  result["external"] = externalResult.stats;

  // Phase 2: Process internal permutations (2 permutations)
  auto internalResult = identifyTriplesToVacuum<true>();
  removeIdentifiedTriples<true>(internalResult.deletionsToRemove,
                                internalResult.insertionsToRemove);
  result["internal"] = internalResult.stats;

  return result;
}

namespace {
IdTriple<0> convertPSOToSPO(const IdTriple<0>& psoTriple) {
  const auto& ids = psoTriple.ids();
  // PSO: [P, S, O, G] → SPO: [S, P, O, G]
  return IdTriple<0>{std::array{ids[1], ids[0], ids[2], ids[3]}};
}

// Helper to create a tie from an IdTable row (S, P, O, G)
auto tieIdTableRow(auto& row) {
  return std::tie(row[0], row[1], row[2], row[3]);
}

// Helper to create a tie from a located triple (S, P, O, G)
auto tieLocatedTriple(auto& lt) {
  auto& ids = lt->triple_.ids();
  return std::tie(ids[0], ids[1], ids[2], ids[3]);
}
}  // namespace

// ____________________________________________________________________________
template <bool isInternal>
void DeltaTriples::removeIdentifiedTriples(
    const std::vector<IdTriple<0>>& deletionsToRemove,
    const std::vector<IdTriple<0>>& insertionsToRemove) {
  auto& state = getState<isInternal>();

  auto removeTriples = [this](const std::vector<IdTriple<0>>& triples,
                              auto& triplesToHandlesMap) {
    for (const auto& triple : triples) {
      auto it = triplesToHandlesMap.find(triple);
      AD_CORRECTNESS_CHECK(it != triplesToHandlesMap.end());
      this->eraseTripleInAllPermutations<isInternal>(it->second);
      triplesToHandlesMap.erase(it);
    }
  };

  removeTriples(deletionsToRemove, state.triplesDeleted_);
  removeTriples(insertionsToRemove, state.triplesInserted_);
}

// Explicit template instantiations
template void DeltaTriples::removeIdentifiedTriples<true>(
    const std::vector<IdTriple<0>>&, const std::vector<IdTriple<0>>&);
template void DeltaTriples::removeIdentifiedTriples<false>(
    const std::vector<IdTriple<0>>&, const std::vector<IdTriple<0>>&);

// ____________________________________________________________________________
template <bool isInternal>
DeltaTriples::TriplesToVacuum DeltaTriples::identifyTriplesToRemoveFromBlock(
    size_t blockIndex, const IdTable& block) {
  // Get the located triples for this block from PSO permutation
  const auto& locatedTriplesPerBlock =
      locatedTriples_->getLocatedTriplesForPermutation<isInternal>(
          Permutation::PSO);

  // Check if this block has any located triples
  auto optUpdates = locatedTriplesPerBlock.getUpdatesIfPresent(blockIndex);
  if (!optUpdates.has_value()) {
    return {std::vector<IdTriple<0>>{}, std::vector<IdTriple<0>>{},
            VacuumStatistics{0, 0, 0, 0}};
  }

  const auto& locatedTriples = optUpdates.value();
  std::vector<IdTriple<0>> deletionsToRemove;
  std::vector<IdTriple<0>> insertionsToRemove;
  VacuumStatistics stats{0, 0, 0, 0};

  auto lessThan = [](const auto& lt, const auto& row) {
    return tieLocatedTriple(lt) < tieIdTableRow(row);
  };
  auto equal = [](const auto& lt, const auto& row) {
    return tieLocatedTriple(lt) == tieIdTableRow(row);
  };

  auto rowIt = block.begin();
  auto updateIt = locatedTriples.begin();

  // Two-pointer merge to identify redundant updates
  while (updateIt != locatedTriples.end() && rowIt != block.end()) {
    if (lessThan(updateIt, *rowIt)) {
      // Update is for a triple not in the original dataset.
      if (updateIt->insertOrDelete_) {
        // Valid insertion of a new triple - keep it
        stats.numInsertionsKept_++;
      } else {
        // Redundant deletion of a non-existent triple - remove it
        deletionsToRemove.push_back(convertPSOToSPO(updateIt->triple_));
        stats.numDeletionsRemoved_++;
      }
      updateIt++;
    } else if (equal(updateIt, *rowIt)) {
      // Update matches an existing triple in the dataset.
      if (updateIt->insertOrDelete_) {
        // Redundant insertion of an existing triple - remove it
        insertionsToRemove.push_back(convertPSOToSPO(updateIt->triple_));
        stats.numInsertionsRemoved_++;
      } else {
        // Valid deletion of an existing triple - keep it
        stats.numDeletionsKept_++;
      }
      updateIt++;
    } else {
      // Row is less than update, advance row iterator.
      rowIt++;
    }
  }

  // Handle remaining updates after block is exhausted
  while (updateIt != locatedTriples.end()) {
    if (updateIt->insertOrDelete_) {
      // Valid insertion (triple not in dataset) - keep it
      stats.numInsertionsKept_++;
    } else {
      // Redundant deletion (triple not in dataset) - remove it
      deletionsToRemove.push_back(convertPSOToSPO(updateIt->triple_));
      stats.numDeletionsRemoved_++;
    }
    updateIt++;
  }

  return TriplesToVacuum{std::move(deletionsToRemove),
                         std::move(insertionsToRemove), stats};
}

// Explicit template instantiations
template DeltaTriples::TriplesToVacuum
DeltaTriples::identifyTriplesToRemoveFromBlock<true>(size_t, const IdTable&);
template DeltaTriples::TriplesToVacuum
DeltaTriples::identifyTriplesToRemoveFromBlock<false>(size_t, const IdTable&);

// ____________________________________________________________________________
template <bool isInternal>
DeltaTriples::TriplesToVacuum DeltaTriples::identifyTriplesToVacuum() {
  VacuumStatistics totalStats{0, 0, 0, 0};
  std::vector<IdTriple<0>> allDeletionsToRemove;
  std::vector<IdTriple<0>> allInsertionsToRemove;

  // Threshold for vacuuming a block
  constexpr size_t VACUUM_THRESHOLD = 10000;

  // Step 1: Get PSO permutation data
  const auto& locatedTriplesPerBlock =
      locatedTriples_->getLocatedTriplesForPermutation<isInternal>(
          Permutation::PSO);
  auto& basePerm = index_.getPermutation(Permutation::PSO);
  const auto& perm = isInternal ? basePerm.internalPermutation() : basePerm;

  // Step 2: Collect blocks needing vacuum
  std::vector<size_t> blocksToVacuum;
  // TODO: here we require the LocatedTriplesPerBlock -> this is especially easy to remove
  for (const auto& [blockIndex, locatedTriples] : locatedTriplesPerBlock.map_) {
    if (locatedTriples.size() > VACUUM_THRESHOLD) {
      blocksToVacuum.push_back(blockIndex);
    }
  }

  // Early exit if nothing to vacuum
  if (blocksToVacuum.empty()) {
    return {std::move(allDeletionsToRemove), std::move(allInsertionsToRemove),
            totalStats};
  }

  // Step 3: Get reader and metadata
  const auto& reader = perm.reader();
  const auto& blockMetadata = perm.metaData().blockData();

  // Step 4: Process each block
  for (size_t blockIndex : blocksToVacuum) {
    // 4a: Validate
    AD_CORRECTNESS_CHECK(blockIndex <= blockMetadata.size());
    if (blockIndex == blockMetadata.size()) {
      // return identifyTriplesToRemoveFromBlock<isInternal>(blockIndex,
      // IdTable(4, allocator_);
      continue;
    }
    const auto& blockMeta = blockMetadata[blockIndex];
    AD_CORRECTNESS_CHECK(blockMeta.offsetsAndCompressedSize_.has_value());
    AD_CORRECTNESS_CHECK(blockMeta.blockIndex_ == blockIndex);

    // 4b: Determine columns
    std::vector<ColumnIndex> columnIndices;
    for (size_t col = 0; col < 4; ++col) {
      const auto& colData = blockMeta.offsetsAndCompressedSize_.value()[col];
      AD_CORRECTNESS_CHECK(colData.compressedSize_ > 0);
      columnIndices.push_back(col);
    }

    // 4c: Read and decompress
    // TODO: here we require the CompressRelationReader friend
    auto compressedBlock =
        reader.readCompressedBlockFromFile(blockMeta, columnIndices);
    auto decompressedBlock =
        reader.decompressBlock(compressedBlock, blockMeta.numRows_);

    // 4d: Determine parameters
    auto [numIndexColumns, includeGraphColumn] =
        CompressedRelationReader::prepareLocatedTriples(columnIndices);
    AD_CORRECTNESS_CHECK(numIndexColumns == 3);
    AD_CORRECTNESS_CHECK(includeGraphColumn);

    // 4e: Identify triples to remove and accumulate statistics
    auto result = identifyTriplesToRemoveFromBlock<isInternal>(
        blockIndex, decompressedBlock);

    // 4f: Accumulate results
    allDeletionsToRemove.insert(allDeletionsToRemove.end(),
                                result.deletionsToRemove.begin(),
                                result.deletionsToRemove.end());
    allInsertionsToRemove.insert(allInsertionsToRemove.end(),
                                 result.insertionsToRemove.begin(),
                                 result.insertionsToRemove.end());
    totalStats.numInsertionsRemoved_ += result.stats.numInsertionsRemoved_;
    totalStats.numDeletionsRemoved_ += result.stats.numDeletionsRemoved_;
    totalStats.numInsertionsKept_ += result.stats.numInsertionsKept_;
    totalStats.numDeletionsKept_ += result.stats.numDeletionsKept_;
  }

  return {std::move(allDeletionsToRemove), std::move(allInsertionsToRemove),
          totalStats};
}

// Explicit template instantiations
template DeltaTriples::TriplesToVacuum
DeltaTriples::identifyTriplesToVacuum<true>();
template DeltaTriples::TriplesToVacuum
DeltaTriples::identifyTriplesToVacuum<false>();

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
  return LocatedTriplesSharedState{std::make_shared<LocatedTriplesState>(
      locatedTriples_->locatedTriplesPerBlock_,
      locatedTriples_->internalLocatedTriplesPerBlock_,
      localVocab_.getLifetimeExtender(), locatedTriples_->index_)};
}

// ____________________________________________________________________________
LocatedTriplesSharedState DeltaTriples::getLocatedTriplesSharedStateReference()
    const {
  // Creating a `shared_ptr<const LocatedTriplesState>` from a
  // `shared_ptr<LocatedTriplesState>` is cheap.
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

    tracer.endTrace("acquiringDeltaTriplesWriteLock");
    if constexpr (std::is_void_v<ReturnType>) {
      tracer.beginTrace("operations");
      function(deltaTriples);
      tracer.endTrace("operations");
      updateMetadata();
      writeAndUpdateSnapshot();
    } else {
      tracer.beginTrace("operations");
      ReturnType returnValue = function(deltaTriples);
      tracer.endTrace("operations");
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
std::tuple<
    LocatedTriplesSharedState, std::vector<LocalVocabIndex>,
    std::vector<
        ad_utility::BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry>>
DeltaTriplesManager::getCurrentLocatedTriplesSharedStateWithVocab() const {
  return deltaTriples_.withReadLock([this](const DeltaTriples& deltaTriples) {
    auto [indices, ownedBlocks] = deltaTriples.copyLocalVocab();
    return std::make_tuple(*currentLocatedTriplesSharedState_.rlock(),
                           std::move(indices), std::move(ownedBlocks));
  });
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
std::pair<
    std::vector<LocalVocabIndex>,
    std::vector<
        ad_utility::BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry>>
DeltaTriples::copyLocalVocab() const {
  AD_CORRECTNESS_CHECK(localVocab_.otherSets().empty(),
                       "This function only copies from the primary word set.");
  std::vector<LocalVocabIndex> entries = ::ranges::to_vector(
      localVocab_.primaryWordSet() |
      ql::views::transform(
          [](const LocalVocabEntry& entry) { return &entry; }));
  return std::make_pair(std::move(entries),
                        localVocab_.getOwnedLocalBlankNodeBlocks());
}
