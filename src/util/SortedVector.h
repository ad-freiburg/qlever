// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_SORTEDVECTOR_H
#define QLEVER_SRC_UTIL_SORTEDVECTOR_H

#include <functional>
#include <iterator>
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "backports/functional.h"
#include "backports/span.h"
#include "util/Exception.h"
#include "util/ZipMergeIterator.h"

namespace ad_utility {

// Data structure optimized for sorted access. Elements are stored in a vector
// sorted by the `Projection` applied to each element and compared using
// `Compare`. After inserting elements `consolidate` must be called before
// reading. Only the last inserted element for each projected key is retained.
//
// Internally the elements are stored in two-parts - a large and a small part.
// Newly inserted elements are first inserted into the small part. `consolidate`
// sorts the small part. Once the small parts get large it is merged into the
// large part which is always sorted.
CPP_template(typename ValueType, typename Compare = std::less<>,
             typename Projection = ql::identity)(
    requires true) class SortedVector {
 public:
  // Some GTest matchers require `value_type`, also add it as a type for the
  // `Container` requirement.
  using value_type = ValueType;

 private:
  using Storage = std::vector<value_type>;
  Storage elements_ = {};
  size_t numItemsLargePart_ = 0;
  bool smallPartIsSorted_ = true;
  [[no_unique_address]] Compare comp_ = {};
  [[no_unique_address]] Projection proj_ = {};

  // Returns the subrange of the small/large part from the whole storage vector.
  auto smallPart() {
    return ql::ranges::subrange(elements_.begin() + numItemsLargePart_,
                                elements_.end());
  }
  auto largePart() {
    return ql::ranges::subrange(elements_.begin(),
                                elements_.begin() + numItemsLargePart_);
  }

  // Merge the elements of the small part into the large part. The small part
  // must be sorted before.
  void mergeParts() {
    AD_CORRECTNESS_CHECK(smallPartIsSorted_);
    Storage merged;
    merged.reserve(elements_.capacity());
    ql::ranges::move(*this, std::back_insert_iterator(merged));
    elements_.swap(merged);
    numItemsLargePart_ = elements_.size();
    smallPartIsSorted_ = true;
  }
  void sortSmallPart() {
    sortAndRemoveDuplicates(elements_, smallPart());
    smallPartIsSorted_ = true;
  }

  // Whether the items are all sorted and deduplicated. Items can only be read
  // if `isClean` is true. Also enforces the class invariant that the boundary
  // index `numItemsLargePart_` lies within `elements_`; if it ever drifts out
  // of range, sorted-access methods fail this check rather than silently UB'ing
  // through `elements_.begin() + numItemsLargePart_`.
  bool isClean() const {
    return smallPartIsSorted_ && numItemsLargePart_ <= elements_.size();
  }

  template <typename Self>
  static auto iterImpl(Self& self, bool atEnd) {
    AD_CONTRACT_CHECK(self.isClean());
    using Iter =
        std::conditional_t<std::is_const_v<Self>, const_iterator, iterator>;
    auto mid = self.elements_.begin() + self.numItemsLargePart_;
    return Iter(atEnd ? mid : self.elements_.begin(), mid,
                atEnd ? self.elements_.end() : mid, self.elements_.end(),
                self.comp_, self.proj_);
  }

  FRIEND_TEST(SortedVectorTest, eraseSortedSubRange);
  FRIEND_TEST(SortedVectorTest, sortAndRemoveDuplicates);
  FRIEND_TEST(SortedVectorTest, constructor);
  FRIEND_TEST(SortedVectorTest, insert);
  friend struct SortedVectorPairsTestHelper;

 public:
  SortedVector() = default;

  // For the range `rangeToSort` contained in `elements` sort it by the
  // projected key and keep the last element for each projected key.
  CPP_template(typename R)(
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

  // Removes everything in `r1` that is contained in `r2`. Returns the number
  // of items deleted. NOTE: `r1` must be a subrange of `elements`.
  CPP_template(typename R1, typename R2)(
      requires ql::ranges::forward_range<R1>&& ql::ranges::output_range<
          R1, ValueType>&& ql::ranges::input_range<R2>) static size_t
      eraseSortedSubRange(Storage& elements, R1&& r1, R2&& r2) {
    auto [_, newEndOfSubrange] = ql::ranges::set_difference(
        r1, r2, ql::ranges::begin(r1), Compare{}, Projection{}, Projection{});
    auto numItemsErased = std::distance(newEndOfSubrange, ql::ranges::end(r1));
    elements.erase(newEndOfSubrange, ql::ranges::end(r1));
    return numItemsErased;
  }

  static SortedVector fromSorted(std::vector<ValueType> sortedElements,
                                 Compare comp = {}, Projection proj = {}) {
    AD_EXPENSIVE_CHECK(ql::ranges::is_sorted(sortedElements, comp, proj));
    // No duplicate elements (elements with the same projected key).
    AD_EXPENSIVE_CHECK(ql::ranges::adjacent_find(sortedElements, {}, proj) ==
                       sortedElements.end());
    SortedVector vec;
    vec.comp_ = std::move(comp);
    vec.proj_ = std::move(proj);
    vec.numItemsLargePart_ = sortedElements.size();
    vec.elements_ = std::move(sortedElements);
    return vec;
  }

  // Consolidates the stored items after inserts. `consolidate` must be called
  // before any access after inserting new items. After calling `consolidate`
  // `isClean` will be true.
  void consolidate(double threshold = 0.25) {
    if (smallPartIsSorted_) return;

    sortSmallPart();
    if (static_cast<double>(elements_.size() - numItemsLargePart_) /
            elements_.size() >
        threshold) {
      mergeParts();
    }
  }

  void insert(ValueType elem) {
    elements_.push_back(std::move(elem));
    smallPartIsSorted_ = false;
  }

  using iterator = detail::ZipMergeIteratorImpl<typename Storage::iterator,
                                                Compare, Projection>;
  using const_iterator =
      detail::ZipMergeIteratorImpl<typename Storage::const_iterator, Compare,
                                   Projection>;

  iterator begin() { return iterImpl(*this, false); }
  const_iterator begin() const { return iterImpl(*this, false); }
  iterator end() { return iterImpl(*this, true); }
  const_iterator end() const { return iterImpl(*this, true); }

  const ValueType& back() const {
    AD_CONTRACT_CHECK(!empty());
    AD_CONTRACT_CHECK(isClean());
    if (numItemsLargePart_ == elements_.size()) {
      return elements_.back();
    }
    auto& lastLargePart = elements_.at(numItemsLargePart_ - 1);
    auto& lastSmallPart = elements_.back();
    return comp_(proj_(lastSmallPart), proj_(lastLargePart)) ? lastLargePart
                                                             : lastSmallPart;
  }
  ValueType& back() {
    return const_cast<ValueType&>(std::as_const(*this).back());
  }

  // Erase a single or multiple elements.
  void erase(const ValueType& elem) {
    // TODO: the semantics are wonky here. The single element `erase` erases by
    // the whole element while the multi element overloads identify by
    // the projected elements
    AD_CONTRACT_CHECK(isClean());
    auto deleteInRange = [this, &elem](auto subrange) {
      auto iter = ql::ranges::lower_bound(subrange, elem);
      if (iter == ql::ranges::end(subrange) || *iter != elem) {
        return 0;
      }
      elements_.erase(iter);
      return 1;
    };
    numItemsLargePart_ -= deleteInRange(largePart());
    deleteInRange(smallPart());
  }
  void eraseUnsorted(std::vector<ValueType> toDelete) {
    AD_CONTRACT_CHECK(isClean());
    ql::ranges::sort(toDelete, comp_, proj_);
    eraseSorted(ql::span(toDelete));
  }
  // Erase multiple elements that are already sorted.
  void eraseSorted(ql::span<ValueType> sortedElems) {
    AD_CONTRACT_CHECK(isClean());
    AD_EXPENSIVE_CHECK(ql::ranges::is_sorted(sortedElems, comp_, proj_));
    numItemsLargePart_ -=
        eraseSortedSubRange(elements_, largePart(), sortedElems);
    eraseSortedSubRange(elements_, smallPart(), sortedElems);
  }

  // Returns an upper bound of the size. Without actually reading all items the
  // size is not known because items might be duplicated between the small and
  // large parts.
  size_t sizeUpperBound() const {
    AD_CONTRACT_CHECK(isClean());
    return elements_.size();
  }
  // Returns an exact size but reads the whole vector for this.
  size_t sizeForTesting() const {
    AD_CONTRACT_CHECK(isClean());
    return std::distance(begin(), end());
  }

  bool empty() const {
    // No need to ensure that items are sorted, because it keeps one element for
    // each projected key. So the elements cannot get empty through sorting.
    return elements_.empty();
  }
  void clear() {
    elements_.clear();
    numItemsLargePart_ = 0;
    smallPartIsSorted_ = true;
  }

  friend std::ostream& operator<<(std::ostream& os, const SortedVector& sv) {
    os << "{ ";
    ql::ranges::copy(sv, std::ostream_iterator<ValueType>(os, " "));
    os << "}";
    return os;
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_SORTEDVECTOR_H
