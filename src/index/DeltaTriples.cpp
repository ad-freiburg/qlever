// Copyright 2023 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>
//          Julian Mundhahs <mundhahj@tf.uni-freiburg.de>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "index/DeltaTriples.h"

#include "absl/strings/str_cat.h"
#include "index/Index.h"
#include "index/IndexImpl.h"
#include "index/LocatedTriples.h"

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
                                  std::span<const IdTriple<0>> idTriples,
                                  bool shouldExist) {
  std::array<std::vector<LocatedTriples::iterator>, Permutation::ALL.size()>
      intermediateHandles;
  for (auto permutation : Permutation::ALL) {
    auto& perm = index_.getPermutation(permutation);
    auto locatedTriples = LocatedTriple::locateTriplesInPermutation(
        // TODO<qup42>: replace with `getAugmentedMetadata` once integration
        //  is done
        idTriples, perm.metaData().blockData(), perm.keyOrder(), shouldExist,
        cancellationHandle);
    cancellationHandle->throwIfCancelled();
    intermediateHandles[static_cast<size_t>(permutation)] =
        this->locatedTriples()[static_cast<size_t>(permutation)].add(
            locatedTriples);
    cancellationHandle->throwIfCancelled();
  }
  std::vector<DeltaTriples::LocatedTripleHandles> handles{idTriples.size()};
  for (auto permutation : Permutation::ALL) {
    for (size_t i = 0; i < idTriples.size(); i++) {
      handles[i].forPermutation(permutation) =
          intermediateHandles[static_cast<size_t>(permutation)][i];
    }
  }
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
void DeltaTriples::insertTriples(CancellationHandle cancellationHandle,
                                 Triples triples) {
  LOG(DEBUG) << "Inserting"
             << " " << triples.size()
             << " triples (including idempotent triples)." << std::endl;
  modifyTriplesImpl(std::move(cancellationHandle), std::move(triples), true,
                    triplesInserted_, triplesDeleted_);
}

// ____________________________________________________________________________
void DeltaTriples::deleteTriples(CancellationHandle cancellationHandle,
                                 Triples triples) {
  LOG(DEBUG) << "Deleting"
             << " " << triples.size()
             << " triples (including idempotent triples)." << std::endl;
  modifyTriplesImpl(std::move(cancellationHandle), std::move(triples), false,
                    triplesDeleted_, triplesInserted_);
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
    ql::ranges::for_each(triple.ids_, convertId);
    ql::ranges::for_each(triple.payload_, convertId);
  });
}

// ____________________________________________________________________________
void DeltaTriples::modifyTriplesImpl(CancellationHandle cancellationHandle,
                                     Triples triples, bool shouldExist,
                                     TriplesToHandlesMap& targetMap,
                                     TriplesToHandlesMap& inverseMap) {
  rewriteLocalVocabEntriesAndBlankNodes(triples);
  ql::ranges::sort(triples);
  auto [first, last] = std::ranges::unique(triples);
  triples.erase(first, last);
  std::erase_if(triples, [&targetMap](const IdTriple<0>& triple) {
    return targetMap.contains(triple);
  });
  ql::ranges::for_each(triples, [this, &inverseMap](const IdTriple<0>& triple) {
    auto handle = inverseMap.find(triple);
    if (handle != inverseMap.end()) {
      eraseTripleInAllPermutations(handle->second);
      inverseMap.erase(triple);
    }
  });

  std::vector<LocatedTripleHandles> handles =
      locateAndAddTriples(std::move(cancellationHandle), triples, shouldExist);

  AD_CORRECTNESS_CHECK(triples.size() == handles.size());
  // TODO<qup42>: replace with ql::views::zip in C++23
  for (size_t i = 0; i < triples.size(); i++) {
    targetMap.insert({triples[i], handles[i]});
  }
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
      locatedTriples(), localVocab_.clone(), snapshotIndex)};
}

// ____________________________________________________________________________
DeltaTriples::DeltaTriples(const Index& index)
    : DeltaTriples(index.getImpl()) {}

// ____________________________________________________________________________
DeltaTriplesManager::DeltaTriplesManager(const IndexImpl& index)
    : deltaTriples_{index},
      currentLocatedTriplesSnapshot_{deltaTriples_.wlock()->getSnapshot()} {}

// _____________________________________________________________________________
void DeltaTriplesManager::modify(
    const std::function<void(DeltaTriples&)>& function) {
  // While holding the lock for the underlying `DeltaTriples`, perform the
  // actual `function` (typically some combination of insert and delete
  // operations) and (while still holding the lock) update the
  // `currentLocatedTriplesSnapshot_`.
  deltaTriples_.withWriteLock([this, &function](DeltaTriples& deltaTriples) {
    function(deltaTriples);
    auto newSnapshot = deltaTriples.getSnapshot();
    currentLocatedTriplesSnapshot_.withWriteLock(
        [&newSnapshot](auto& currentSnapshot) {
          currentSnapshot = std::move(newSnapshot);
        });
  });
}

// _____________________________________________________________________________
void DeltaTriplesManager::clear() { modify(&DeltaTriples::clear); }

// _____________________________________________________________________________
SharedLocatedTriplesSnapshot DeltaTriplesManager::getCurrentSnapshot() const {
  return *currentLocatedTriplesSnapshot_.rlock();
}

// _____________________________________________________________________________
void DeltaTriples::setOriginalMetadata(
    Permutation::Enum permutation,
    std::vector<CompressedBlockMetadata> metadata) {
  locatedTriples()
      .at(static_cast<size_t>(permutation))
      .setOriginalMetadata(std::move(metadata));
}
