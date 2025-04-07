// Copyright 2023 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>
//          Julian Mundhahs <mundhahj@tf.uni-freiburg.de>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "index/DeltaTriples.h"

#include <absl/strings/str_cat.h>

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
DeltaTriplesCount DeltaTriples::getCounts() const {
  return {numInserted(), numDeleted()};
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
  AD_EXPENSIVE_CHECK(ql::ranges::is_sorted(triples));
  AD_EXPENSIVE_CHECK(std::unique(triples.begin(), triples.end()) ==
                     triples.end());
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
    bool writeToDiskAfterRequest) {
  // While holding the lock for the underlying `DeltaTriples`, perform the
  // actual `function` (typically some combination of insert and delete
  // operations) and (while still holding the lock) update the
  // `currentLocatedTriplesSnapshot_`.
  return deltaTriples_.withWriteLock(
      [this, &function, writeToDiskAfterRequest](DeltaTriples& deltaTriples) {
        auto updateSnapshot = [this, &deltaTriples] {
          auto newSnapshot = deltaTriples.getSnapshot();
          currentLocatedTriplesSnapshot_.withWriteLock(
              [&newSnapshot](auto& currentSnapshot) {
                currentSnapshot = std::move(newSnapshot);
              });
        };
        auto writeAndUpdateSnapshot = [&updateSnapshot, &deltaTriples,
                                       writeToDiskAfterRequest]() {
          if (writeToDiskAfterRequest) {
            // TODO<RobinTF> Find a good way to track the time it takes to write
            // this and store it into the corresponding metadata object.
            deltaTriples.writeToDisk();
          }
          updateSnapshot();
        };

        if constexpr (std::is_void_v<ReturnType>) {
          function(deltaTriples);
          writeAndUpdateSnapshot();
        } else {
          ReturnType returnValue = function(deltaTriples);
          writeAndUpdateSnapshot();
          return returnValue;
        }
      });
}
// Explicit instantions
template void DeltaTriplesManager::modify<void>(
    std::function<void(DeltaTriples&)> const&, bool writeToDiskAfterRequest);
template nlohmann::json DeltaTriplesManager::modify<nlohmann::json>(
    const std::function<nlohmann::json(DeltaTriples&)>&,
    bool writeToDiskAfterRequest);
template DeltaTriplesCount DeltaTriplesManager::modify<DeltaTriplesCount>(
    const std::function<DeltaTriplesCount(DeltaTriples&)>&,
    bool writeToDiskAfterRequest);

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
void DeltaTriples::writeToDisk() const {
  if (!filenameForPersisting_.has_value()) {
    return;
  }
  auto toRange = [](const TriplesToHandlesMap& map) {
    return map | ql::views::keys |
           ql::views::transform(
               [](const IdTriple<0>& triple) -> const std::array<Id, 4>& {
                 return triple.ids_;
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
    static_assert(std::tuple_size_v<
                      decltype(std::declval<Triples::value_type>().payload_)> ==
                  0);
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
