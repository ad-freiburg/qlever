// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_SORTEDSEQUENCE_H
#define QLEVER_SRC_UTIL_SORTEDSEQUENCE_H

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
#include "util/views/ZipMergeUniqueView.h"

namespace ad_utility {

// Data structure optimized for sorted access. Elements are stored in a vector
// sorted by the `Projection` applied to each element and compared using
// `Compare`. After inserting elements `consolidate` must be called before
// reading. Only the last inserted element for each projected key is retained.
//
// Internally the elements are stored in two parts - an always sorted large
// part and a small part. Newly inserted elements are first inserted into the
// small part. `consolidate` sorts the small part. Iteration will then
// internally merge the two sorted parts using `ZipMergeUniqueView`. Once the
// small part gets large, it is merged into the large part, which is always
// sorted.
template <typename ValueType, typename Compare = std::less<>,
          typename Projection = ql::identity>
class SortedSequence {
  using Storage = std::vector<ValueType>;
  Storage elements_ = {};
  size_t numItemsLargePart_ = 0;
  bool smallPartIsSorted_ = true;
  [[no_unique_address]] Compare comp_ = {};
  [[no_unique_address]] Projection proj_ = {};

  // Helper for the overloads of `smallPart`/`largePart`.
  template <typename Self>
  static auto partImpl(Self& self, bool wantSmall) {
    auto mid = self.elements_.begin() + self.numItemsLargePart_;
    return wantSmall ? ql::ranges::subrange(mid, self.elements_.end())
                     : ql::ranges::subrange(self.elements_.begin(), mid);
  }

  // Return the subrange of the small part from the whole storage vector.
  auto smallPart() { return partImpl(*this, true); }
  auto smallPart() const { return partImpl(*this, true); }
  // Return the subrange of the large part from the whole storage vector.
  auto largePart() { return partImpl(*this, false); }
  auto largePart() const { return partImpl(*this, false); }

  // Merge the elements of the small part into the large part. The small part
  // must be sorted before calling this function. Preserves `isConsolidated`.
  void mergeParts() {
    AD_CORRECTNESS_CHECK(smallPartIsSorted_);
    Storage merged;
    merged.reserve(elements_.capacity());
    ql::ranges::move(getSortedView(), std::back_insert_iterator(merged));
    elements_.swap(merged);
    numItemsLargePart_ = elements_.size();
    smallPartIsSorted_ = true;
  }
  // Sort and deduplicate the elements in the small part. Afterwards
  // `isConsolidated()` is true.
  void sortSmallPart() {
    sortAndRemoveDuplicates(elements_, smallPart());
    smallPartIsSorted_ = true;
  }

  // Return true iff the items are all sorted and deduplicated. Items can only
  // be read if `isConsolidated` is true. `isConsolidated` is true iff no
  // inserts have been made since the last call to `consolidate` or
  // construction. Deletes keep this invariant true.
  //
  // Note: Also enforces the class invariant that the boundary index
  // `numItemsLargePart_` lies within `elements_`; if it ever drifts out of
  // range, sorted-access methods fail this check rather than silently UB'ing
  // through `elements_.begin() + numItemsLargePart_`.
  bool isConsolidated() const {
    AD_CORRECTNESS_CHECK(numItemsLargePart_ <= elements_.size());
    return smallPartIsSorted_;
  }

  // For the range `rangeToSort` contained in `elements` sort it by the
  // projected key and keep the last element for each projected key.
  CPP_template_2(typename R)(
      requires ql::ranges::range<
          R>) static void sortAndRemoveDuplicates(Storage& elements,
                                                  R&& rangeToSort) {
    // Stable sort ensures that the operations for each key are not reordered.
    // Older elements are before newer ones.
    ql::ranges::stable_sort(rangeToSort, Compare{}, Projection{});
    // We want to keep the last element for consecutive groups with the same
    // projected key. `unique` keeps the first element for consecutive groups
    // with the same element. So `unique(reverse)` does exactly what we want.
    auto freedReverse =
        ql::ranges::unique(ql::views::reverse(rangeToSort), {}, Projection{});
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

  // Let `r1` be a sorted subrange of `elements_` and `r2` be an arbitrary
  // sorted range not overlapping with `r1`. Delete all elements from `r1` that
  // are also contained in `r2`. Within this class, `r1` is always unique
  // (elements are deduplicated before calling this function). Duplicates in
  // `r2` make no difference.
  CPP_template_2(typename R1, typename R2)(
      requires ql::ranges::forward_range<R1> CPP_and_2
          ql::ranges::output_range<R1, ValueType>
              CPP_and_2 ql::ranges::input_range<R2>) size_t
      eraseSortedSubRange(R1&& r1, R2&& r2) {
    auto newEndOfSubrange =
        ad_utility::inplace_set_difference(r1, r2, comp_, proj_, proj_);
    auto numItemsErased = std::distance(newEndOfSubrange, ql::ranges::end(r1));
    elements_.erase(newEndOfSubrange, ql::ranges::end(r1));
    return numItemsErased;
  }

  FRIEND_TEST(SortedSequenceTest, eraseSortedSubRange);
  FRIEND_TEST(SortedSequenceTest, sortAndRemoveDuplicates);
  FRIEND_TEST(SortedSequenceTest, constructor);
  FRIEND_TEST(SortedSequenceTest, insert);
  friend struct SortedSequencePairsTestHelper;

 public:
  SortedSequence() = default;

  // Create a `SortedSequence` from already sorted and deduplicated elements.
  static SortedSequence fromSorted(std::vector<ValueType> sortedElements,
                                   Compare comp = {}, Projection proj = {}) {
    AD_EXPENSIVE_CHECK(ql::ranges::is_sorted(sortedElements, comp, proj));
    // No duplicate elements (elements with the same projected key).
    AD_EXPENSIVE_CHECK(ql::ranges::adjacent_find(sortedElements, {}, proj) ==
                       sortedElements.end());
    SortedSequence seq;
    seq.comp_ = std::move(comp);
    seq.proj_ = std::move(proj);
    seq.numItemsLargePart_ = sortedElements.size();
    seq.elements_ = std::move(sortedElements);
    return seq;
  }

  // Some GTest matchers require `value_type`, also add it as a type for the
  // `Container` requirement.
  using value_type = ValueType;

  // Consolidate the stored items after inserts have been performed by sorting
  // and deduplicating the small part. If the small part makes up more than a
  // fraction `threshold` of the total items, the two parts are additionally
  // merged.
  // `consolidate` must be called before any read access after inserting new
  // items. After calling `consolidate` `isConsolidated` will be true.
  void consolidate(double threshold = 0.25) {
    if (smallPartIsSorted_) {
      return;
    }

    sortSmallPart();
    if (static_cast<double>(elements_.size() - numItemsLargePart_) /
            elements_.size() >
        threshold) {
      mergeParts();
    }
  }

  // Insert an element. `consolidate` must be called before the next read
  // access.
  void insert(ValueType elem) {
    elements_.push_back(std::move(elem));
    smallPartIsSorted_ = false;
  }

 private:
  // Helper for the overloads of `getSortedView`.
  template <typename Self>
  auto getSortedViewImpl(Self& self) const {
    AD_CONTRACT_CHECK(self.isConsolidated());
    auto mid = self.elements_.begin() + self.numItemsLargePart_;
    return ZipMergeUniqueView(ql::ranges::subrange(self.elements_.begin(), mid),
                              ql::ranges::subrange(mid, self.elements_.end()),
                              self.comp_, self.proj_);
  }

 public:
  // Return a view of the sorted and deduplicated elements in this container.
  // Requires `isConsolidated` to be true.
  auto getSortedView() & { return getSortedViewImpl(*this); }
  auto getSortedView() const& { return getSortedViewImpl(*this); }
  void getSortedView() && = delete;
  void getSortedView() const&& = delete;

 private:
  // Return the extreme (maximal if `wantMax`, else minimal) element w.r.t.
  // `comp_`/`proj_`. On a tie (equal projected key) the small part's element
  // wins, since it holds the most recently inserted value for that key.
  // Requires the container to not be empty and `isConsolidated` to be true.
  const ValueType& extremumImpl(bool wantMax) const {
    AD_CONTRACT_CHECK(!empty());
    AD_CONTRACT_CHECK(isConsolidated());
    auto frontOrBack = [wantMax](const auto& range) -> decltype(auto) {
      return wantMax ? range.back() : range.front();
    };
    // If we only have a single part directly return its respective end.
    if (numItemsLargePart_ == elements_.size() || numItemsLargePart_ == 0) {
      return frontOrBack(elements_);
    }
    auto& largePartBoundary = frontOrBack(largePart());
    auto& smallPartBoundary = frontOrBack(smallPart());
    bool largeWins =
        wantMax ? comp_(proj_(smallPartBoundary), proj_(largePartBoundary))
                : comp_(proj_(largePartBoundary), proj_(smallPartBoundary));
    return largeWins ? largePartBoundary : smallPartBoundary;
  }

 public:
  // Return a reference to the last element. Requires the container to not be
  // empty and `isConsolidated` to be true.
  const ValueType& back() const { return extremumImpl(true); }
  // Return a reference to the last element. Requires the container to not be
  // empty and `isConsolidated` to be true. Note: modifying the returned element
  // such that its projected key changes breaks the invariants of this class.
  ValueType& back() {
    return const_cast<ValueType&>(std::as_const(*this).back());
  }

  // Return a reference to the first element. Requires the container to not be
  // empty and `isConsolidated` to be true.
  const ValueType& front() const { return extremumImpl(false); }
  // Return a reference to the first element. Requires the container to not be
  // empty and `isConsolidated` to be true. Note: modifying the returned element
  // such that its projected key changes breaks the invariants of this class.
  ValueType& front() {
    return const_cast<ValueType&>(std::as_const(*this).front());
  }

  // Erase a single element. When deleting multiple elements use `eraseUnsorted`
  // or `eraseSorted`. This is expensive and preserves `isConsolidated`.
  void erase(const ValueType& elem) {
    AD_CONTRACT_CHECK(isConsolidated());
    // Delete `elem` in the range `subrange` (which is contained in
    // `elements_`) and return the number of deleted elements.
    auto deleteInRange = [this, &elem](auto subrange) {
      auto iter = ql::ranges::lower_bound(subrange, proj_(elem), comp_, proj_);
      // From lower_bound we get `!comp_(proj_(*iter), proj_(elem))`. If the
      // comparison below is also false the elements are equivalent under proj_
      // and comp_.
      if (iter == ql::ranges::end(subrange) ||
          comp_(proj_(elem), proj_(*iter))) {
        return 0;
      }
      elements_.erase(iter);
      return 1;
    };
    deleteInRange(smallPart());
    numItemsLargePart_ -= deleteInRange(largePart());
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
  // duplicates.
  // Note: calling this function is expensive (O(n)) and preserves
  // `isConsolidated`.
  void eraseSorted(ql::span<ValueType> sortedElems) {
    AD_CONTRACT_CHECK(isConsolidated());
    AD_EXPENSIVE_CHECK(ql::ranges::is_sorted(sortedElems, comp_, proj_));
    eraseSortedSubRange(smallPart(), sortedElems);
    numItemsLargePart_ -= eraseSortedSubRange(largePart(), sortedElems);
  }

  // Return an upper bound of the size. Can be called even when
  // `isConsolidated` is false.
  // Without actually reading all items the size is not known because items
  // might be duplicated between the small and large parts and also inside the
  // parts.
  size_t sizeUpperBound() const { return elements_.size(); }
  // Return an exact size but read the whole vector for this. Requires
  // `isConsolidated` to be true.
  size_t sizeForTesting() const {
    return ql::ranges::distance(getSortedView());
  }

  // Return whether the container is empty. Can be called even when
  // `isConsolidated` is `false`.
  bool empty() const {
    // No need to ensure that items are sorted, because it keeps one element for
    // each projected key. So the elements cannot get empty through sorting.
    return elements_.empty();
  }
  // Clear the elements.
  void clear() {
    elements_.clear();
    numItemsLargePart_ = 0;
    smallPartIsSorted_ = true;
  }

  // This operator is only for debugging and testing. It returns a
  // human-readable representation. Requires `isConsolidated` to be true.
  friend std::ostream& operator<<(std::ostream& os, const SortedSequence& sv) {
    os << "{ ";
    ql::ranges::copy(sv.getSortedView(),
                     std::ostream_iterator<ValueType>(os, " "));
    os << "}";
    return os;
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_SORTEDSEQUENCE_H
