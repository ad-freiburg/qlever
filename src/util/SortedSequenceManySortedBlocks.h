// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// This class is an adapted and generalized version of the
// `BlockSortedLocatedTriplesVector` that Julian Mundhahs wrote in the course
// of PR #2792 (see the history of that PR).

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_SORTEDSEQUENCEMANYSORTEDBLOCKS_H
#define QLEVER_SRC_UTIL_SORTEDSEQUENCEMANYSORTEDBLOCKS_H

#include <gtest/gtest_prod.h>

#include <functional>
#include <iterator>
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "backports/functional.h"
#include "backports/span.h"
#include "util/Algorithm.h"
#include "util/Exception.h"
#include "util/FlattenIterator.h"

namespace ad_utility {

// Implementation of `SortedSequence` (see `util/SortedSequence.h`), based on
// the `BlockSortedLocatedTriplesVector` from the history of PR #2792: the
// elements are stored in a sequence of sorted, non-overlapping
// blocks of at most `BlockSize` elements each, plus a small buffer of pending
// insertions. `consolidate` sorts and deduplicates the pending buffer and
// merges it into the affected blocks (splitting blocks that become too
// large). This is the leaf level of a B+-tree with a flat block directory
// instead of a hierarchy of inner nodes, which suffices for the block counts
// that occur here.
//
// The key difference to `SortedSequenceTwoSortedRanges` (which keeps one vector
// split into a large and a small sorted part and merges them on the fly during
// iteration):
// after `consolidate`, iteration is a plain sequential traversal of the
// blocks with no comparisons at all, and a consolidate only rewrites the
// blocks that the pending elements actually touch, not the whole small part.
//
// As for `SortedSequenceTwoSortedRanges`: elements are sorted by the
// `Projection` applied to each element and compared using `Compare`; only the
// last inserted element for each projected key is retained; after inserting,
// `consolidate` must be called before reading.
template <typename ValueType, typename Compare = std::less<>,
          typename Projection = ql::identity, size_t BlockSize = 16384>
class SortedSequenceManySortedBlocks {
  static_assert(BlockSize >= 2);
  using Block = std::vector<ValueType>;
  std::vector<Block> blocks_;
  std::vector<ValueType> pending_;
  // The exact number of elements in `blocks_` (their elements are always
  // deduplicated; the pending buffer is not counted).
  size_t numItems_ = 0;
  [[no_unique_address]] Compare comp_ = {};
  [[no_unique_address]] Projection proj_ = {};

  // Compare two elements by their projected keys.
  bool less(const ValueType& a, const ValueType& b) const {
    return comp_(proj_(a), proj_(b));
  }

  // Return true iff the items are all sorted and deduplicated. Items can only
  // be read if `isConsolidated` is true. `isConsolidated` is true iff no
  // inserts have been made since the last call to `consolidate` or
  // construction. Deletes keep this invariant true.
  bool isConsolidated() const { return pending_.empty(); }

  // The index of the block that elements with the projected key of `elem`
  // belong to: the first block whose last element is not smaller. Returns
  // `blocks_.size()` if `elem` is larger than all stored elements.
  size_t findBlock(const ValueType& elem) const {
    auto it = ql::ranges::partition_point(
        blocks_,
        [this, &elem](const Block& block) { return less(block.back(), elem); });
    return static_cast<size_t>(it - blocks_.begin());
  }

  // For the range `rangeToSort` contained in `elements` sort it by the
  // projected key and keep the last element for each projected key (this is
  // the same procedure as in `SortedSequenceTwoSortedRanges`).
  CPP_template_2(typename R)(
      requires ql::ranges::range<
          R>) void sortAndRemoveDuplicates(std::vector<ValueType>& elements,
                                           R&& rangeToSort) const {
    // Stable sort ensures that the operations for each key are not reordered.
    // Older elements are before newer ones.
    ql::ranges::stable_sort(rangeToSort, comp_, proj_);
    // We want to keep the last element for consecutive groups with the same
    // projected key. `unique` keeps the first element for consecutive groups
    // with the same element. So `unique(reverse)` does exactly what we want.
    auto freedReverse =
        ql::ranges::unique(ql::views::reverse(rangeToSort), {}, proj_);
    // std::ranges and ranges-v3 have different return types for `unique`. The
    // `#ifdef` below accommodates the differences.
#ifdef QLEVER_CPP_17
    auto eraseEnd = freedReverse.base();
#else
    auto eraseEnd = freedReverse.begin().base();
#endif
    // Delete the freed up space which is at the beginning of `rangeToSort`.
    elements.erase(ql::ranges::begin(rangeToSort), eraseEnd);
  }

  // Merge the sorted and deduplicated `pending` elements into the sorted and
  // deduplicated `block`, with the (newer) pending element winning on equal
  // projected keys. The result replaces `block` and is again sorted and
  // deduplicated.
  void mergeIntoBlock(Block& block, ql::span<const ValueType> pending) {
    Block merged;
    merged.reserve(block.size() + pending.size());
    auto blockIt = block.begin();
    auto pendIt = pending.begin();
    while (blockIt != block.end() && pendIt != pending.end()) {
      if (less(*blockIt, *pendIt)) {
        merged.push_back(std::move(*blockIt));
        ++blockIt;
      } else if (less(*pendIt, *blockIt)) {
        merged.push_back(*pendIt);
        ++pendIt;
      } else {
        // Equal projected keys: the (newer) pending element wins.
        merged.push_back(*pendIt);
        ++pendIt;
        ++blockIt;
      }
    }
    std::move(blockIt, block.end(), std::back_inserter(merged));
    std::copy(pendIt, pending.end(), std::back_inserter(merged));
    block.swap(merged);
  }

  // Cut the sorted and deduplicated `elements` into blocks of at most
  // `maxBlockSize` elements (of roughly equal size) and append them to `out`.
  static void cutIntoBlocks(std::vector<ValueType> elements,
                            std::vector<Block>& out, size_t maxBlockSize) {
    size_t n = elements.size();
    if (n == 0) {
      return;
    }
    size_t numBlocks = (n + maxBlockSize - 1) / maxBlockSize;
    size_t perBlock = (n + numBlocks - 1) / numBlocks;
    if (numBlocks == 1) {
      out.push_back(std::move(elements));
      return;
    }
    for (size_t i = 0; i < n; i += perBlock) {
      size_t end = std::min(i + perBlock, n);
      out.emplace_back(std::make_move_iterator(elements.begin() + i),
                       std::make_move_iterator(elements.begin() + end));
    }
  }

  // If the block at `blockIndex` has more than `BlockSize` elements, replace
  // it by blocks of roughly equal size that all comply with the limit. Return
  // the number of blocks that now occupy the position (1 if no split was
  // needed).
  size_t splitIfNeeded(size_t blockIndex) {
    if (blocks_[blockIndex].size() <= BlockSize) {
      return 1;
    }
    std::vector<Block> pieces;
    cutIntoBlocks(std::move(blocks_[blockIndex]), pieces, BlockSize);
    size_t numPieces = pieces.size();
    // Replace the oversized block by the first piece and insert the rest
    // after it. The block directory is small (its size is the number of
    // elements divided by `BlockSize`), so the linear cost of the insertion
    // is negligible.
    blocks_[blockIndex] = std::move(pieces.front());
    blocks_.insert(blocks_.begin() + blockIndex + 1,
                   std::make_move_iterator(pieces.begin() + 1),
                   std::make_move_iterator(pieces.end()));
    return numPieces;
  }

  // Remove the elements from `block` whose projected key occurs in the sorted
  // range `toDelete`, and return the number of removed elements.
  CPP_template_2(typename R)(requires ql::ranges::input_range<R>) size_t
      eraseSortedFromBlock(Block& block, R&& toDelete) {
    auto newEnd = ad_utility::inplace_set_difference(block, toDelete, comp_,
                                                     proj_, proj_);
    size_t numErased = static_cast<size_t>(block.end() - newEnd);
    block.erase(newEnd, block.end());
    return numErased;
  }

  // Remove all empty blocks.
  void removeEmptyBlocks() {
    auto newEnd = std::remove_if(blocks_.begin(), blocks_.end(),
                                 [](const Block& b) { return b.empty(); });
    blocks_.erase(newEnd, blocks_.end());
  }

  // Walk the sorted range `elems` along the blocks and call
  // `apply(blockIndex, subrangeOfElems)` for each block together with the
  // slice of `elems` that belongs to it (elements beyond the last block are
  // routed to the last block). `apply` may modify the block, but the block
  // boundaries used for the routing are the ones from before the walk.
  template <typename It, typename F>
  void routeToBlocks(It first, It last, F apply) {
    size_t numBlocks = blocks_.size();
    for (size_t i = 0; i < numBlocks && first != last; ++i) {
      It sliceEnd;
      if (i + 1 < numBlocks) {
        // All elements up to and including this block's largest key.
        sliceEnd =
            std::upper_bound(first, last, proj_(blocks_[i].back()),
                             [this](const auto& key, const ValueType& elem) {
                               return comp_(key, proj_(elem));
                             });
      } else {
        // The last block gets all remaining elements.
        sliceEnd = last;
      }
      if (first != sliceEnd) {
        apply(i, ql::ranges::subrange(first, sliceEnd));
      }
      first = sliceEnd;
    }
  }

 public:
  SortedSequenceManySortedBlocks() = default;

  // Create a `SortedSequencePlus` from already sorted and deduplicated
  // elements.
  static SortedSequenceManySortedBlocks fromSorted(
      std::vector<ValueType> sortedElements, Compare comp = {},
      Projection proj = {}) {
    AD_EXPENSIVE_CHECK(ql::ranges::is_sorted(sortedElements, comp, proj));
    // No duplicate elements (elements with the same projected key).
    AD_EXPENSIVE_CHECK(ql::ranges::adjacent_find(sortedElements, {}, proj) ==
                       sortedElements.end());
    SortedSequenceManySortedBlocks seq;
    seq.comp_ = std::move(comp);
    seq.proj_ = std::move(proj);
    seq.numItems_ = sortedElements.size();
    cutIntoBlocks(std::move(sortedElements), seq.blocks_, BlockSize);
    return seq;
  }

  // Some GTest matchers require `value_type`, also add it as a type for the
  // `Container` requirement.
  using value_type = ValueType;

  // Consolidate the stored items after inserts have been performed: sort and
  // deduplicate the pending buffer and merge it into the affected blocks.
  // `consolidate` must be called before any read access after inserting new
  // items. After calling `consolidate` `isConsolidated` will be true.
  //
  // NOTE: The `threshold` parameter exists for interface compatibility with
  // `SortedSequenceTwoSortedRanges` and is ignored: a consolidate always merges
  // the pending elements into their blocks (which only rewrites the affected
  // blocks, so there is no separate cheaper mode).
  void consolidate([[maybe_unused]] double threshold = 0.25) {
    if (pending_.empty()) {
      return;
    }
    sortAndRemoveDuplicates(
        pending_, ql::ranges::subrange(pending_.begin(), pending_.end()));
    if (blocks_.empty()) {
      numItems_ = pending_.size();
      cutIntoBlocks(std::move(pending_), blocks_, BlockSize);
      pending_.clear();
      return;
    }
    // First route the pending elements to their blocks (while the block
    // directory is unmodified), then merge the slices into their blocks in
    // reverse order, so that the splitting of a block that became too large
    // does not shift the indices of the blocks that are still to be
    // processed.
    using PendingIt = typename std::vector<ValueType>::const_iterator;
    std::vector<std::tuple<size_t, PendingIt, PendingIt>> slices;
    routeToBlocks(pending_.cbegin(), pending_.cend(),
                  [&slices](size_t blockIndex, auto slice) {
                    slices.emplace_back(blockIndex, slice.begin(), slice.end());
                  });
    for (auto it = slices.rbegin(); it != slices.rend(); ++it) {
      auto [blockIndex, first, last] = *it;
      size_t oldSize = blocks_[blockIndex].size();
      mergeIntoBlock(blocks_[blockIndex],
                     ql::span<const ValueType>(
                         &*first, static_cast<size_t>(last - first)));
      numItems_ += blocks_[blockIndex].size() - oldSize;
      splitIfNeeded(blockIndex);
    }
    pending_.clear();
  }

  // Insert an element. `consolidate` must be called before the next read
  // access.
  void insert(ValueType elem) { pending_.push_back(std::move(elem)); }

 private:
  // Helper for the overloads of `getSortedView`.
  template <typename Self>
  static auto getSortedViewImpl(Self& self) {
    AD_CONTRACT_CHECK(self.isConsolidated());
    using OuterIt = decltype(self.blocks_.begin());
    using InnerIt = decltype(self.blocks_.begin()->begin());
    return FlattenView<OuterIt, InnerIt>(self.blocks_.begin(),
                                         self.blocks_.end());
  }

 public:
  // Return a view of the sorted and deduplicated elements in this container.
  // Requires `isConsolidated` to be true. In contrast to `SortedSequence`,
  // the iteration is a plain sequential traversal without any comparisons.
  auto getSortedView() & { return getSortedViewImpl(*this); }
  auto getSortedView() const& { return getSortedViewImpl(*this); }
  void getSortedView() && = delete;
  void getSortedView() const&& = delete;

  // Return a reference to the last element. Requires the container to not be
  // empty and `isConsolidated` to be true. Note: modifying the returned
  // element such that its projected key changes breaks the invariants of this
  // class.
  const ValueType& back() const {
    AD_CONTRACT_CHECK(!empty());
    AD_CONTRACT_CHECK(isConsolidated());
    return blocks_.back().back();
  }
  ValueType& back() {
    return const_cast<ValueType&>(std::as_const(*this).back());
  }

  // Return a reference to the first element. Requires the container to not be
  // empty and `isConsolidated` to be true. The same note as for `back()`
  // applies.
  const ValueType& front() const {
    AD_CONTRACT_CHECK(!empty());
    AD_CONTRACT_CHECK(isConsolidated());
    return blocks_.front().front();
  }
  ValueType& front() {
    return const_cast<ValueType&>(std::as_const(*this).front());
  }

  // Erase the element with the projected key of `elem`, if present. This is
  // expensive and preserves `isConsolidated`.
  void erase(const ValueType& elem) {
    AD_CONTRACT_CHECK(isConsolidated());
    size_t blockIndex = findBlock(elem);
    if (blockIndex == blocks_.size()) {
      return;
    }
    Block& block = blocks_[blockIndex];
    auto iter = ql::ranges::lower_bound(block, proj_(elem), comp_, proj_);
    // From `lower_bound` we get `!comp_(proj_(*iter), proj_(elem))`. If the
    // comparison below is also false the elements are equivalent under
    // `proj_` and `comp_`.
    if (iter == block.end() || comp_(proj_(elem), proj_(*iter))) {
      return;
    }
    block.erase(iter);
    --numItems_;
    if (block.empty()) {
      blocks_.erase(blocks_.begin() + blockIndex);
    }
  }

  // Erase multiple elements that may contain duplicates. If the elements to
  // delete are already sorted use `eraseSorted`.
  // Note: calling this function is expensive (O(n)) and preserves
  // `isConsolidated`.
  void eraseUnsorted(std::vector<ValueType> toDelete) {
    AD_CONTRACT_CHECK(isConsolidated());
    ql::ranges::sort(toDelete, comp_, proj_);
    eraseSorted(ql::span(toDelete));
  }

  // Erase multiple elements that are already sorted but still may contain
  // duplicates. In contrast to `SortedSequence`, only the blocks that contain
  // elements to delete are touched.
  void eraseSorted(ql::span<ValueType> sortedElems) {
    AD_CONTRACT_CHECK(isConsolidated());
    AD_EXPENSIVE_CHECK(ql::ranges::is_sorted(sortedElems, comp_, proj_));
    routeToBlocks(sortedElems.begin(), sortedElems.end(),
                  [this](size_t blockIndex, auto slice) {
                    numItems_ -=
                        eraseSortedFromBlock(blocks_[blockIndex], slice);
                  });
    removeEmptyBlocks();
  }

  // Return an upper bound of the size (elements in the pending buffer might
  // be duplicates of each other or of stored elements). Can be called even
  // when `isConsolidated` is false.
  size_t sizeUpperBound() const { return numItems_ + pending_.size(); }
  // Return the exact size. Requires `isConsolidated` to be true.
  size_t sizeForTesting() const {
    AD_CONTRACT_CHECK(isConsolidated());
    return numItems_;
  }

  // Return whether the container is empty. Can be called even when
  // `isConsolidated` is `false`.
  bool empty() const { return blocks_.empty() && pending_.empty(); }

  // Clear the elements.
  void clear() {
    blocks_.clear();
    pending_.clear();
    numItems_ = 0;
  }

  // This operator is only for debugging and testing. It returns a
  // human-readable representation. Requires `isConsolidated` to be true.
  friend std::ostream& operator<<(std::ostream& os,
                                  const SortedSequenceManySortedBlocks& sv) {
    os << "{ ";
    ql::ranges::copy(sv.getSortedView(),
                     std::ostream_iterator<ValueType>(os, " "));
    os << "}";
    return os;
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_SORTEDSEQUENCEMANYSORTEDBLOCKS_H
