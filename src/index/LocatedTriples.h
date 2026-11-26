// Copyright 2023 - 2025 The QLever Authors, in particular:
//
// 2023 - 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2024 - 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_LOCATEDTRIPLES_H
#define QLEVER_SRC_INDEX_LOCATEDTRIPLES_H

#include <boost/optional.hpp>

#include "backports/three_way_comparison.h"
#include "engine/idTable/IdTable.h"
#include "global/IdTriple.h"
#include "index/CompressedRelationMetadata.h"
#include "index/KeyOrder.h"
#include "util/CancellationHandle.h"
#include "util/CopyOnWritePtr.h"
#include "util/HashMap.h"
#include "util/HashSet.h"
#include "util/SortedSequence.h"
#include "util/TimeTracer.h"
#include "util/TransparentFunctors.h"
#include "util/TypeTraits.h"

class Permutation;

// The graphs whose triples in the original index (that is, not the delta
// triples) have been deleted completely (see `DeltaTriples::dropGraph`). The
// graphs are stored via the bits of their `Id`s, which for the `Id`s of the
// original index have the same order as the `Id`s in the permutations.
class DroppedGraphs {
 private:
  // The graphs, sorted and without duplicates.
  std::vector<Id::T> sorted_;
  // The same graphs, for fast lookups.
  ad_utility::HashSet<Id::T> hashed_;

 public:
  // Add the `graph`.
  void add(Id graph);

  // Return true iff the `graph` has been dropped.
  bool contains(Id graph) const { return hashed_.contains(graph.getBits()); }

  // Return true iff one of the graphs is in the closed range
  // `[first, last]`.
  bool containsGraphInRange(Id first, Id last) const;

  // Return the graphs, sorted by their `Id`s.
  std::vector<Id> graphs() const;

  size_t size() const { return sorted_.size(); }
  bool empty() const { return sorted_.empty(); }
};

struct NumAddedAndDeleted {
  size_t numAdded_;
  size_t numDeleted_;

  QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL(NumAddedAndDeleted, numAdded_,
                                              numDeleted_)

  friend std::ostream& operator<<(std::ostream& str,
                                  const NumAddedAndDeleted& n) {
    str << "added " << n.numAdded_ << ", deleted " << n.numDeleted_;
    return str;
  }
};

// Statistics collected during a vacuum operation on a block.
struct VacuumStatistics {
  // Only updates that have an effect are kept. Inserts that already exist
  // and deletes that don't exist are removed.
  size_t numDeletionsRemoved_;
  size_t numInsertionsRemoved_;
  size_t numDeletionsKept_;
  size_t numInsertionsKept_;

  size_t totalRemoved() const {
    return numDeletionsRemoved_ + numInsertionsRemoved_;
  }

  size_t totalKept() const { return numDeletionsKept_ + numInsertionsKept_; }

  VacuumStatistics& operator+=(const VacuumStatistics& other) {
    numDeletionsRemoved_ += other.numDeletionsRemoved_;
    numInsertionsRemoved_ += other.numInsertionsRemoved_;
    numDeletionsKept_ += other.numDeletionsKept_;
    numInsertionsKept_ += other.numInsertionsKept_;
    return *this;
  }

  friend void to_json(nlohmann::json& j, const VacuumStatistics& stats);
};

// Triples identified for removal during a vacuum operation.
struct TriplesToVacuum {
  std::vector<IdTriple<0>> deletionsToRemove_;
  std::vector<IdTriple<0>> insertionsToRemove_;
  VacuumStatistics stats_;
};

// A triple and its block in a particular permutation. For a detailed definition
// of all border cases, see the definition at the end of this file.
struct LocatedTriple {
  // The index of the block, according to the definition above.
  size_t blockIndex_;
  // The `Id`s of the triple in the order of the permutation. For example,
  // for an object pertaining to the OPS permutation: `triple_[0]` is the
  // object, `triple_[1]` is the predicate, `triple_[2]` is the subject,
  // and `triple_[3]` is the graph.
  IdTriple<0> triple_;

  // If `true`, the triple is inserted, otherwise it is deleted.
  bool insertOrDelete_;

  // Locate the given triples in the given permutation.
  static std::vector<LocatedTriple> locateTriplesInPermutation(
      ql::span<const IdTriple<0>> triples,
      ql::span<const CompressedBlockMetadata> blockMetadata,
      const qlever::KeyOrder& keyOrder, bool insertOrDelete,
      ad_utility::SharedCancellationHandle cancellationHandle);

  QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL(LocatedTriple, blockIndex_,
                                              triple_, insertOrDelete_)

  // This operator is only for debugging and testing. It returns a
  // human-readable representation.
  friend std::ostream& operator<<(std::ostream& os, const LocatedTriple& lt) {
    os << "LT(" << lt.blockIndex_ << " " << lt.triple_ << " "
       << lt.insertOrDelete_ << ")";
    return os;
  }
};

using SortedLocatedTriplesVector = ad_utility::SortedSequence<
    LocatedTriple, std::less<>,
    ad_utility::MemberProjection<&LocatedTriple::triple_>>;

using LocatedTriples = SortedLocatedTriplesVector;

// Sorted sets of located triples, grouped by block. We use this to store all
// located triples for a permutation.
class LocatedTriplesPerBlock {
 private:
  // For each block with a non-empty set of located triples, the located triples
  // in that block. The blocks are held via `CopyOnWritePtr`, so that copying
  // this class (which happens for every snapshot of the delta triples) only
  // copies pointers. All modifications go through `mutableBlock`, which clones
  // a block that is shared with a copy before modifying it.
  ad_utility::HashMap<size_t, ad_utility::CopyOnWritePtr<LocatedTriples>> map_;

  // The blocks that were modified since the last call to
  // `consolidateAllBlocks`, which then only has to consolidate these blocks.
  ad_utility::HashSet<size_t> unconsolidatedBlocks_;

  FRIEND_TEST(LocatedTriplesTest, numTriplesInBlock);
  FRIEND_TEST(LocatedTriplesTest, copyOnWrite);

  // Return the located triples of the block with the given index for
  // modification. Create the block if it does not exist and clone it if it is
  // shared with a copy of this class (see `map_`), and remember it as
  // unconsolidated. A clone gets spare capacity for `numNewTriples` triples.
  LocatedTriples& mutableBlock(size_t blockIndex, size_t numNewTriples = 0);

  // Implementation of the `mergeTriples` function (which has `numIndexColumns`
  // as a normal argument, and translates it into a template argument).
  template <size_t numIndexColumns, bool includeGraphColumn>
  IdTable mergeTriplesImpl(size_t blockIndex, const IdTable& block) const;

  // Stores the block metadata where the block borders have been adjusted for
  // the updated triples.
  std::optional<std::vector<CompressedBlockMetadata>> augmentedMetadata_;
  std::optional<std::shared_ptr<const std::vector<CompressedBlockMetadata>>>
      originalMetadata_;

  // The graphs whose triples in the original blocks are deleted (see
  // `DroppedGraphs`), and the position of the graph in the key order of the
  // permutation (3 if the graph is the last column). The original rows of a
  // block that may contain triples of these graphs are removed before the
  // located triples are merged (see `mergeTriples`). Such a block is treated
  // like a block with located triples (see `containsTriples`, `numTriples`,
  // and `isEmpty`), so that all the shortcuts that rely on the original block
  // metadata are disabled for it.
  std::shared_ptr<const DroppedGraphs> droppedGraphs_;
  size_t graphKeyPosition_ = 3;

  // Remove the rows of the original `block` with the given index that belong
  // to a dropped graph. See `mergeTriples` for the meaning of
  // `numIndexColumns` and `fixedColumns`.
  void removeRowsOfDroppedGraphs(
      IdTable& block, size_t numIndexColumns,
      const std::array<std::optional<Id>, 3>& fixedColumns) const;

 public:
  void updateAugmentedMetadata();

 public:
  // Get upper limits for the number of inserted and deleted located triples
  // for the given block.
  //
  // NOTE: This currently returns an upper bound for the total number of triples
  // in the block twice, in order to avoid counting the triples with
  // `insertOrDelete_ == true` and `insertOrDelete_ == false` separately, which
  // is expensive.
  //
  // TODO: Since the average number of located triples per block is usually
  // small, this estimate is usually fine. We could get better estimates in
  // constant time by maintaining a counter for each of these two numbers in
  // `SortedSequence` and update these counters for each consolidation.
  // However, note that that would still be an estimate because 1. the triple
  // might be counted twice between the parts and 2. at this point we do not
  // know whether an insertion or deletion is actually effective.
  NumAddedAndDeleted numTriples(size_t blockIndex) const;

  // Returns an optional reference to update triples for the block with the
  // index `blockIndex`. If no such block exists, return `std::nullopt`.
  boost::optional<const LocatedTriples&> getUpdatesIfPresent(
      size_t blockIndex) const;

  // Merge located triples for `blockIndex_` with the given input `block`. This
  // function must only be called if `containsTriples(blockIndex)` is true.
  // Return the result as an `IdTable`, which has the same number of columns as
  // `block`.
  //
  // `numIndexColumns` is the number of columns in `block`, except the graph
  // column and payload if any, that is, a number from `{1, 2, 3}`.
  // `includeGraphColumn` specifies whether `block` contains the graph column.
  //
  // If `block` has payload columns (which currently our located triples never
  // have), the value of the merged located triples for these columns is set to
  // UNDEF.
  //
  // For example, assume that `block` is from the POS permutation, that
  // `numIndexColumns` is 2 (that is, only OS are present in the block), that
  // `includeGraphColumn` is `true` (that is, G is also present in the block),
  // and that `block` has block has two additional payload columns X and Y.
  // Then the result has five columns (like the input `block`), and each merged
  // located triple will have values for OSG and UNDEF for X and Y.
  //
  // Before the merge, the rows of the `block` that belong to a dropped graph
  // are removed (see `droppedGraphs_`). If the graph is one of the
  // `numIndexColumns`, it is read from the `block`, otherwise it is fixed by
  // the scan and contained in the `fixedColumns` (the `Id`s of the fixed first
  // columns of the permutation, which are the same for all rows of the block).
  IdTable mergeTriples(
      size_t blockIndex, const IdTable& block, size_t numIndexColumns,
      bool includeGraphColumn,
      const std::array<std::optional<Id>, 3>& fixedColumns = {}) const;

  // Return true iff there are located triples in the block with the given
  // index, or if the block may contain triples of a dropped graph (see
  // `droppedGraphs_`).
  bool containsTriples(size_t blockIndex) const {
    return map_.contains(blockIndex) ||
           blockMayContainDroppedGraphs(blockIndex);
  }

  // Return true iff the original block with the given index may contain
  // triples of a dropped graph.
  bool blockMayContainDroppedGraphs(size_t blockIndex) const;

  // Set the dropped graphs and the position of the graph in the key order of
  // the permutation, see `droppedGraphs_`.
  void setDroppedGraphs(std::shared_ptr<const DroppedGraphs> droppedGraphs) {
    droppedGraphs_ = std::move(droppedGraphs);
  }
  void setGraphKeyPosition(size_t graphKeyPosition) {
    AD_CONTRACT_CHECK(graphKeyPosition <= 3);
    graphKeyPosition_ = graphKeyPosition;
  }

  // Return true iff there are dropped graphs.
  bool hasDroppedGraphs() const {
    return droppedGraphs_ != nullptr && !droppedGraphs_->empty();
  }

  // Add unsorted `locatedTriples` to the `LocatedTriplesPerBlock`.
  void add(ql::span<const LocatedTriple> locatedTriples,
           ad_utility::timer::TimeTracer& tracer =
               ad_utility::timer::DEFAULT_TIME_TRACER);

  // Removes the given `LocatedTriple` from the `LocatedTriplesPerBlock`.
  //
  // NOTE: `updateAugmentedMetadata()` must be called to update the block
  // metadata.
  void erase(size_t blockIndex, const LocatedTriple& lt);

  // Overload of erase above that removes multiple triples at once.
  // PRECONDITION: `sortedTriples` must be sorted.
  // NOTE: `updateAugmentedMetadata()` must be called to update the block
  // metadata.
  void erase(ql::span<LocatedTriple> sortedTriples);

  // Get the total number of `LocatedTriple`s (for all blocks).
  size_t numTriplesForTesting() const;

  // Get the number of blocks with a non-empty set of located triples.
  size_t numBlocks() const { return map_.size(); }

  // Return whether any of the blocks with index in the closed range
  // [`firstBlockIndex`, `lastBlockIndex`] has located triples. The cost is
  // linear in the smaller of the size of the range and `numBlocks()`.
  //
  // NOTE: This could be done in logarithmic time with an additional ordered
  // set of the block indices in `map_` (two binary searches). That set would
  // have to be maintained in `add` and `erase`. We keep `map_` a hash map
  // because the per-block lookups during scans are the hot path.
  bool containsLocatedTriplesInBlockRange(size_t firstBlockIndex,
                                          size_t lastBlockIndex) const;

  // Return whether there are any updates at all (including dropped graphs).
  bool isEmpty() const { return map_.empty() && !hasDroppedGraphs(); }

  // Consolidate the located triples in all blocks that were modified since
  // the last call to this function. Forwards to `SortedSequence`. Must be
  // called before any sorted access
  // (begin/end/size/mergeTriples/updateAugmentedMetadata) if the
  // `LocatedTriples` were modified since the last call to this function.
  void consolidateAllBlocks();

  // Must be called initially before using the `LocatedTriplesPerBlock` to
  // initialize the original block metadata that is augmented for updated
  // triples. This is currently done in `Permutation::loadFromDisk`.
  void setOriginalMetadata(
      std::shared_ptr<const std::vector<CompressedBlockMetadata>> metadata);
  void setOriginalMetadata(std::vector<CompressedBlockMetadata> metadata) {
    setOriginalMetadata(
        std::make_shared<const std::vector<CompressedBlockMetadata>>(
            std::move(metadata)));
  }

  // Returns the block metadata where the block borders have been updated to
  // account for the update triples. All triples (both insert and delete) will
  // enlarge the block borders.
  const std::vector<CompressedBlockMetadata>& getAugmentedMetadata() const {
    if (augmentedMetadata_.has_value()) {
      return augmentedMetadata_.value();
    }
    AD_CONTRACT_CHECK(originalMetadata_.has_value());
    return *originalMetadata_.value();
  }

  // Remove all located triples and the dropped graphs.
  void clear() {
    map_.clear();
    unconsolidatedBlocks_.clear();
    augmentedMetadata_.reset();
    droppedGraphs_.reset();
  }

  // Return the dropped graphs (or `nullptr` if there are none).
  const std::shared_ptr<const DroppedGraphs>& droppedGraphs() const {
    return droppedGraphs_;
  }

  // Identify, for all blocks in `perm` whose number of located triples is at
  // least `vacuum-minimum-block-size`, the redundant insertions (triple already
  // in index) and invalid deletions (triple not in index). The redundant
  // triples are then returned as `SPO`. Depending on the updates different
  // permutations may be more or less effective.
  TriplesToVacuum identifyTriplesToVacuum(
      const Permutation& perm,
      ad_utility::SharedCancellationHandle cancellationHandle) const;

  // Return `true` iff one of the blocks contains `triple` with the given
  // `insertOrDelete` status (`true` for inserted, `false` for deleted).
  //
  // NOTE: This is expensive because it iterates over all blocks and checks
  // containment in each. It is only used in our tests, for convenience.
  bool isLocatedTriple(const IdTriple<0>& triple, bool insertOrDelete) const;

  // Compute the located triples that are present in this
  // `LocatedTriplesPerBlock` instance but not in `oldBlocks`. The result is a
  // pair of vectors (insertions, deletions), each sorted in SPO order.
  std::array<std::vector<IdTriple<0>>, 2> computeDiff(
      const LocatedTriplesPerBlock& oldBlocks) const;

  // This operator is only for debugging and testing. It returns a
  // human-readable representation.
  friend std::ostream& operator<<(std::ostream& os,
                                  const LocatedTriplesPerBlock& ltpb) {
    // Get the block indices in sorted order.
    std::vector<size_t> blockIndices;
    ql::ranges::copy(ltpb.map_ | ql::views::keys,
                     std::back_inserter(blockIndices));
    ql::ranges::sort(blockIndices);
    for (auto blockIndex : blockIndices) {
      os << "LTs in Block #" << blockIndex << ": " << *ltpb.map_.at(blockIndex)
         << std::endl;
    }
    return os;
  }
};

// Human-readable representation , which are very useful for debugging.
std::ostream& operator<<(std::ostream& os, const std::vector<IdTriple<0>>& v);

// DEFINITION OF THE POSITION OF A LOCATED TRIPLE IN A PERMUTATION
//
// 1. The position is defined by the index of a block.
//
// 2. A triple belongs to the first block (with the smallest index) that also
// contains at least one triple that is larger than or equal to the input
// triple.
//
// 2.1. In particular, if the triple falls "between two blocks" (the last triple
// of the previous block is smaller and the first triple of the next block is
// larger), then the block is the next block.
//
// 2.2. [Exception to 2.1] triples that are equal to the last triple of a block
// with only the graph ID being higher, also belong to that block. This enforces
// the invariant that triples that only differ in their graph are stored in the
// same block (this is expected and enforced by the
// `CompressedRelationReader/Writer`).
//
// 2.3. In particular, if the triple is smaller than all triples in the
// permutation, the position is the first position of the first block.
//
// 3. If the triple is larger than all triples in the permutation, the block
// index is one after the largest block index.

#endif  // QLEVER_SRC_INDEX_LOCATEDTRIPLES_H
