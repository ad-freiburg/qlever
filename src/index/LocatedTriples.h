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
#include "index/CompressedRelation.h"
#include "index/KeyOrder.h"
#include "util/FlattenIterator.h"
#include "util/HashMap.h"
#include "util/MergeInputRange.h"
#include "util/TimeTracer.h"

class Permutation;

namespace ad_benchmark {
class EnsureIntegrationBenchmark;
class ZipMergeIteratorBenchmark;
class InsertBatchBenchmark;
}  // namespace ad_benchmark

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

// A sorted set of located triples. In `LocatedTriplesPerBlock` below, we use
// this to store all located triples with the same `blockIndex_`.
//
// NOTE: We could also overload `std::less` here, but the explicit specification
// of the order makes it clearer.
struct LocatedTripleCompare {
  bool operator()(const LocatedTriple& x, const LocatedTriple& y) const {
    return x.triple_ < y.triple_;
  }
};

// Data structure for `LocatedTriples` optimized for sorted access.
// The `LocatedTriples` are stored in a vector sorted by the triple. Newly
// inserted triples are sorted on access. Only the last inserted `LocatedTriple`
// for a triple is retained, so the last operation for a triple is retained.
class SortedLocatedTriplesVector {
  using storage = std::vector<LocatedTriple>;
  storage triples_ = {};
  size_t numItemsLargePart_ = 0;
  bool smallPartIsSorted_ = true;

  // Sort the `LocatedTriple`s and only keep the last `LocatedTriples` for each
  // triple.
  void sortAndMergeParts();
  void sortSmallPart();

  // For the range `rangeToSort` contained in `triples` sort it by triple and
  // keep the last `LocatedTriple` for each triple.
  CPP_template(typename R)(
      requires ql::ranges::range<
          R>) static void sortAndRemoveDuplicates(std::vector<LocatedTriple>&
                                                      triples,
                                                  R&& rangeToSort) {
    // Stable sort ensures that the operations for each triple are not
    // reordered. Older `LocatedTriple`s are before newer ones.
    ql::ranges::stable_sort(rangeToSort, {}, &LocatedTriple::triple_);
    // We want to keep the last `LocatedTriple` for elements with the same
    // triple. The first element on the reverse iterators is exactly this last
    // element.
    auto freedReverse = ql::ranges::unique(ql::views::reverse(rangeToSort), {},
                                           &LocatedTriple::triple_);
    // `unique` was on a reversed range so the freed range is also reversed.
    // Reverse it again to obtain the forward range of the erase element.
    auto toFree = ql::views::reverse(freedReverse);
    // Delete the freed up space which is at the beginning of `rangeToSort`.
    triples.erase(toFree.begin(), toFree.end());
  }

  // Whether the items are all sorted and deduplicated. Items can only be read
  // if `isClean` is true.
  bool isClean() const { return smallPartIsSorted_; }

  friend class ad_benchmark::EnsureIntegrationBenchmark;
  friend class ad_benchmark::ZipMergeIteratorBenchmark;
  friend class ad_benchmark::InsertBatchBenchmark;
  template <size_t>
  friend class BlockSortedLocatedTriplesVectorImpl;
  FRIEND_TEST(SortedVectorTest, sortedVector);

 public:
  SortedLocatedTriplesVector() = default;

  static SortedLocatedTriplesVector fromSorted(
      std::vector<LocatedTriple> sortedTriples);

  // Consolidates the stored items after inserts. `consolidate` must be called
  // before any access after inserting new items. After calling `consolidate`
  // `isClean` will be true.
  void consolidate(double threshold = 0.25);

  void insert(LocatedTriple lt);

  using iterator = ad_utility::detail::ZipMergeIteratorImpl<
      storage::iterator, std::less<>, decltype(&LocatedTriple::triple_)>;
  using const_iterator = ad_utility::detail::ZipMergeIteratorImpl<
      storage::const_iterator, std::less<>, decltype(&LocatedTriple::triple_)>;
  using const_reverse_iterator = storage::const_reverse_iterator;

  iterator begin();
  const_iterator begin() const;
  iterator end();
  const_iterator end() const;

  LocatedTriple& back();
  const LocatedTriple& back() const;

  void erase(const LocatedTriple& elem);
  void erase(std::vector<LocatedTriple> toDelete);
  void erase(ql::span<LocatedTriple> sortedTriples);
  CPP_template(typename R)(
      requires ql::ranges::range<
          R>) static void eraseSortedSubRange(std::vector<LocatedTriple>&
                                                  triples,
                                              R&& toDelete) {
    auto out = triples.begin();
    auto triple = triples.begin();
    auto deletion = toDelete.begin();
    LocatedTripleCompare comp;

    while (triple != triples.end() && deletion != toDelete.end()) {
      // triple < deletion
      if (comp(*triple, *deletion)) {
        if (out != triple) {
          *out = std::move(*triple);
        }
        ++out;
        ++triple;
      }
      // deletion < triple
      else if (comp(*deletion, *triple)) {
        // TODO: this would mean that on element is being deleted that is not in
        // the list. see `erase` for consistency
        ++deletion;
        // triple == deletion
      } else {
        ++triple;
        ++deletion;
      }
    }

    auto newEnd = std::move(triple, triples.end(), out);
    triples.erase(newEnd, triples.end());
  }

  size_t size() const;
  bool empty() const;

  bool operator==(const SortedLocatedTriplesVector& other) const {
    AD_CONTRACT_CHECK(isClean());
    AD_CONTRACT_CHECK(other.isClean());
    return triples_ == other.triples_;
  }
};
static_assert(std::ranges::range<SortedLocatedTriplesVector>);

// Variant of `SortedLocatedTriplesVector` that stores triples across multiple
// sorted, non-overlapping blocks. Blocks are dynamically split when they exceed
// `BlockSize`. After consolidation, each block is fully sorted and iteration is
// plain sequential vector traversal with no merge comparisons.
//
// `BlockSortedLocatedTriplesVector` is an alias for the default block size of
// 16384.
template <size_t BlockSize>
class BlockSortedLocatedTriplesVectorImpl {
  using Block = std::vector<LocatedTriple>;

  std::vector<Block> blocks_;
  std::vector<LocatedTriple> pending_;
  size_t totalSize_ = 0;

  static constexpr size_t kTargetBlockSize = BlockSize;

  // Find the block index where `lt` belongs by binary search on block
  // boundaries (each block's last element).
  size_t findBlock(const LocatedTriple& lt) const {
    LocatedTripleCompare comp;
    auto it = std::lower_bound(
        blocks_.begin(), blocks_.end(), lt,
        [&comp](const Block& block, const LocatedTriple& value) {
          return comp(block.back(), value);
        });
    return static_cast<size_t>(it - blocks_.begin());
  }

  // Split block at `blockIdx` into kTargetBlockSize-sized pieces if it exceeds
  // kTargetBlockSize.
  void splitIfNeeded(size_t blockIdx) {
    Block& block = blocks_.at(blockIdx);
    if (block.size() <= kTargetBlockSize) {
      return;
    }
    // Split into ceil(block.size() / kTargetBlockSize) new blocks.
    std::vector<Block> newBlocks;
    for (size_t offset = 0; offset < block.size(); offset += kTargetBlockSize) {
      size_t end = std::min(offset + kTargetBlockSize, block.size());
      newBlocks.emplace_back(std::make_move_iterator(block.begin() + offset),
                             std::make_move_iterator(block.begin() + end));
    }
    blocks_.erase(blocks_.begin() + blockIdx);
    blocks_.insert(blocks_.begin() + blockIdx,
                   std::make_move_iterator(newBlocks.begin()),
                   std::make_move_iterator(newBlocks.end()));
  }

  // Remove empty blocks from `blocks_`.
  void removeEmptyBlocks() {
    std::erase_if(blocks_, [](const Block& b) { return b.empty(); });
  }

  // Merge sorted `pending` elements into an existing sorted `block` with
  // last-wins deduplication on equal triples.
  static void mergeIntoBlock(Block& block, ql::span<LocatedTriple> pending) {
    if (pending.empty()) {
      return;
    }
    LocatedTripleCompare lt;
    Block merged;
    merged.reserve(block.size() + pending.size());

    auto blockIt = block.begin();
    auto pendIt = pending.begin();

    while (blockIt != block.end() && pendIt != pending.end()) {
      if (lt(*blockIt, *pendIt)) {
        merged.push_back(std::move(*blockIt));
        ++blockIt;
      } else if (lt(*pendIt, *blockIt)) {
        merged.push_back(std::move(*pendIt));
        ++pendIt;
      } else {
        // Equal triples: pending (newer) wins.
        merged.push_back(std::move(*pendIt));
        ++pendIt;
        ++blockIt;
      }
    }

    std::move(blockIt, block.end(), std::back_inserter(merged));
    std::move(pendIt, pending.end(), std::back_inserter(merged));

    block.swap(merged);
  }

  bool isClean() const { return pending_.empty(); }

  friend class ad_benchmark::EnsureIntegrationBenchmark;
  friend class ad_benchmark::ZipMergeIteratorBenchmark;
  friend class ad_benchmark::InsertBatchBenchmark;
  FRIEND_TEST(BlockSortedVectorTest, internals);

 public:
  BlockSortedLocatedTriplesVectorImpl() = default;

  static BlockSortedLocatedTriplesVectorImpl fromSorted(
      std::vector<LocatedTriple> sortedTriples) {
    AD_EXPENSIVE_CHECK(
        ql::ranges::is_sorted(sortedTriples, LocatedTripleCompare{}));
    BlockSortedLocatedTriplesVectorImpl result;
    result.totalSize_ = sortedTriples.size();

    for (size_t i = 0; i < sortedTriples.size(); i += kTargetBlockSize) {
      size_t end = std::min(i + kTargetBlockSize, sortedTriples.size());
      Block block;
      block.reserve(end - i);
      std::move(sortedTriples.begin() + i, sortedTriples.begin() + end,
                std::back_inserter(block));
      result.blocks_.push_back(std::move(block));
    }
    return result;
  }

  void consolidate(double /*threshold*/ = 0.25) {
    if (pending_.empty()) {
      return;
    }

    // Sort and deduplicate the pending buffer.
    SortedLocatedTriplesVector::sortAndRemoveDuplicates(pending_, pending_);

    if (blocks_.empty()) {
      // No existing blocks: the pending buffer becomes the first block.
      blocks_.push_back(std::move(pending_));
      splitIfNeeded(0);
    } else {
      // Merge-walk: distribute sorted pending elements into existing blocks.
      auto pendIt = pending_.begin();
      LocatedTripleCompare comp;

      for (size_t i = 0; i < blocks_.size() && pendIt != pending_.end(); ++i) {
        // Find the range of pending elements that belong to block i.
        auto pendEnd = pendIt;
        if (i + 1 < blocks_.size()) {
          // Elements <= this block's max go here.
          pendEnd =
              std::upper_bound(pendIt, pending_.end(), blocks_[i].back(), comp);
        } else {
          // Last block gets all remaining.
          pendEnd = pending_.end();
        }

        if (pendIt != pendEnd) {
          mergeIntoBlock(blocks_[i], ql::span(&*pendIt, pendEnd - pendIt));
          splitIfNeeded(i);
          // If we just split, the next block is at i+1 which is the new second
          // half. The loop increment will move to i+1 which is correct since
          // all pending elements up to pendEnd have already been merged.
        }
        pendIt = pendEnd;
      }

      // If there are remaining pending elements beyond all existing blocks,
      // append them to the last block (they are all > last block's max, so
      // sorted order is preserved) and split if needed.
      if (pendIt != pending_.end()) {
        auto& lastBlock = blocks_.back();
        lastBlock.insert(lastBlock.end(), std::make_move_iterator(pendIt),
                         std::make_move_iterator(pending_.end()));
        splitIfNeeded(blocks_.size() - 1);
      }
    }

    pending_.clear();
    totalSize_ = 0;
    for (const auto& block : blocks_) {
      totalSize_ += block.size();
    }
  }

  void insert(LocatedTriple lt) { pending_.push_back(std::move(lt)); }

  using iterator =
      ad_utility::detail::FlattenIterator<std::vector<Block>::iterator,
                                          Block::iterator>;
  using const_iterator =
      ad_utility::detail::FlattenIterator<std::vector<Block>::const_iterator,
                                          Block::const_iterator>;

  iterator begin() {
    AD_CONTRACT_CHECK(isClean());
    return iterator(blocks_.begin(), blocks_.end());
  }

  const_iterator begin() const {
    AD_CONTRACT_CHECK(isClean());
    return const_iterator(blocks_.begin(), blocks_.end());
  }

  iterator end() {
    AD_CONTRACT_CHECK(isClean());
    return iterator(blocks_.end(), blocks_.end());
  }

  const_iterator end() const {
    AD_CONTRACT_CHECK(isClean());
    return const_iterator(blocks_.end(), blocks_.end());
  }

  LocatedTriple& back() {
    AD_CONTRACT_CHECK(!empty());
    AD_CONTRACT_CHECK(isClean());
    return blocks_.back().back();
  }

  const LocatedTriple& back() const {
    AD_CONTRACT_CHECK(!empty());
    AD_CONTRACT_CHECK(isClean());
    return blocks_.back().back();
  }

  void erase(const LocatedTriple& elem) {
    AD_CONTRACT_CHECK(isClean());
    size_t blockIdx = findBlock(elem);
    AD_CONTRACT_CHECK(blockIdx < blocks_.size());
    auto& block = blocks_[blockIdx];
    auto iter = ql::ranges::lower_bound(block, elem, LocatedTripleCompare{});
    AD_CONTRACT_CHECK(iter != block.end() && *iter == elem);
    block.erase(iter);
    --totalSize_;
    if (block.empty()) {
      blocks_.erase(blocks_.begin() + blockIdx);
    }
  }

  void erase(std::vector<LocatedTriple> toDelete) {
    ql::ranges::sort(toDelete, {}, &LocatedTriple::triple_);
    erase(ql::span{toDelete});
  }

  void erase(ql::span<LocatedTriple> sortedTriples) {
    AD_CONTRACT_CHECK(isClean());

    auto delIt = sortedTriples.begin();
    LocatedTripleCompare comp;

    for (size_t i = 0; i < blocks_.size() && delIt != sortedTriples.end();
         ++i) {
      auto delEnd = delIt;
      if (i + 1 < blocks_.size()) {
        delEnd = std::upper_bound(delIt, sortedTriples.end(), blocks_[i].back(),
                                  comp);
      } else {
        delEnd = sortedTriples.end();
      }

      if (delIt != delEnd) {
        SortedLocatedTriplesVector::eraseSortedSubRange(
            blocks_[i], ql::ranges::subrange(delIt, delEnd));
      }
      delIt = delEnd;
    }

    removeEmptyBlocks();
    totalSize_ = 0;
    for (const auto& block : blocks_) {
      totalSize_ += block.size();
    }
  }

  size_t size() const {
    AD_CONTRACT_CHECK(isClean());
    return totalSize_;
  }

  bool empty() const { return totalSize_ == 0 && pending_.empty(); }

  bool operator==(const BlockSortedLocatedTriplesVectorImpl& other) const {
    AD_CONTRACT_CHECK(isClean());
    AD_CONTRACT_CHECK(other.isClean());
    return ql::ranges::equal(*this, other);
  }
};

using BlockSortedLocatedTriplesVector =
    BlockSortedLocatedTriplesVectorImpl<200>;
static_assert(std::ranges::range<BlockSortedLocatedTriplesVector>);

using LocatedTriples = SortedLocatedTriplesVector;

// This operator is only for debugging and testing. It returns a
// human-readable representation.
std::ostream& operator<<(std::ostream& os, const LocatedTriples& lts);

// Sorted sets of located triples, grouped by block. We use this to store all
// located triples for a permutation.
class LocatedTriplesPerBlock {
 private:
  // For each block with a non-empty set of located triples, the located triples
  // in that block.
  ad_utility::HashMap<size_t, LocatedTriples> map_;

  FRIEND_TEST(LocatedTriplesTest, numTriplesInBlock);

  // Implementation of the `mergeTriples` function (which has `numIndexColumns`
  // as a normal argument, and translates it into a template argument).
  template <size_t numIndexColumns, bool includeGraphColumn>
  IdTable mergeTriplesImpl(size_t blockIndex, const IdTable& block) const;

  // Stores the block metadata where the block borders have been adjusted for
  // the updated triples.
  std::optional<std::vector<CompressedBlockMetadata>> augmentedMetadata_;
  std::optional<std::shared_ptr<const std::vector<CompressedBlockMetadata>>>
      originalMetadata_;

 public:
  void updateAugmentedMetadata();

 public:
  // Get upper limits for the number of inserted and deleted located triples
  // for the given block.
  //
  // NOTE: This currently returns the total number of triples in the block
  // twice, in order to avoid counting the triples with `insertOrDelete_ ==
  // true` and `insertOrDelete_ == false` separately, which turned out to
  // be very expensive for a `std::set`, which is the underlying data
  // structure.
  //
  // TODO: Since the average number of located triples per block is usually
  // small, this estimate is usually fine. We could get better estimates in
  // constant time by maintaining a counter for each of these two numbers in
  // `LocatedTriplesPerBlock` and update these counters for each update
  // operation. However, note that that would still be an estimate because at
  // this point we do not know whether an insertion or deletion is actually
  // effective.
  NumAddedAndDeleted numTriples(size_t blockIndex) const;

  // Returns an optional reference to update triples for the block with the
  // index `blockIndex`. If no such block exists, return `std::nullopt`.
  boost::optional<const LocatedTriples&> getUpdatesIfPresent(
      size_t blockIndex) const;

  // Merge located triples for `blockIndex_` (there must be at least one,
  // otherwise this function must not be called) with the given input `block`.
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
  IdTable mergeTriples(size_t blockIndex, const IdTable& block,
                       size_t numIndexColumns, bool includeGraphColumn) const;

  // Return true iff there are located triples in the block with the given
  // index.
  bool containsTriples(size_t blockIndex) const {
    return map_.contains(blockIndex);
  }

  // Add `locatedTriples` to the `LocatedTriplesPerBlock`.
  void add(std::vector<LocatedTriple> locatedTriples,
           ad_utility::timer::TimeTracer& tracer =
               ad_utility::timer::DEFAULT_TIME_TRACER);

  // Removes the given `LocatedTriple` from the `LocatedTriplesPerBlock`.
  //
  // NOTE: `updateAugmentedMetadata()` must be called to update the block
  // metadata.
  void erase(size_t blockIndex, const LocatedTriple& lt);
  void erase(ql::span<LocatedTriple> sortedTriples);

  // Get the total number of `LocatedTriple`s (for all blocks).
  size_t numTriplesForTesting() const;

  // Get the number of blocks with a non-empty set of located triples.
  size_t numBlocks() const { return map_.size(); }

  // Sort the located triples in all blocks. Must be called before any sorted
  // access (begin/end/size/mergeTriples/updateAugmentedMetadata).
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
  };

  // Remove all located triples.
  void clear() {
    map_.clear();
    augmentedMetadata_.reset();
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
      os << "LTs in Block #" << blockIndex << ": " << ltpb.map_.at(blockIndex)
         << std::endl;
    }
    return os;
  };
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
