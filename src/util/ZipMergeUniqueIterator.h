// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ZIPMERGEITERATOR_H
#define QLEVER_SRC_UTIL_ZIPMERGEITERATOR_H

#include <functional>
#include <iterator>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "backports/functional.h"

namespace ad_utility {
namespace detail {

// Lazily merges and deduplicates two sorted input ranges into a single sorted
// sequence.
//
// Elements are yielded one at a time. The `Compare` function is applied to the
// projected values (`Projection` applied to each element) to determine order.
// When both ranges contain an element that compares equal under the projection,
// only the element from the *second* range is yielded and the element from the
// *first* range is skipped (last-wins tie-break).
CPP_template(typename It, typename Compare = std::less<>,
             class Projection = ql::identity)(
    requires true) class ZipMergeUniqueIteratorImpl {
 public:
  using iterator_category = std::forward_iterator_tag;
  using iterator_concept = std::forward_iterator_tag;
  using value_type = typename std::iterator_traits<It>::value_type;
  using difference_type = std::ptrdiff_t;
  using pointer = typename std::iterator_traits<It>::pointer;
  using reference = typename std::iterator_traits<It>::reference;

 private:
  It it1_;
  It end1_;
  It it2_;
  It end2_;
  [[no_unique_address]] Compare comp_;
  Projection proj_;

 public:
  explicit ZipMergeUniqueIteratorImpl(It b1, It e1, It b2, It e2,
                                      Compare cmp = {}, Projection proj = {})
      : it1_(b1), end1_(e1), it2_(b2), end2_(e2), comp_(cmp), proj_(proj) {
    if constexpr (std::is_pointer_v<Compare> ||
                  std::is_member_pointer_v<Compare>) {
      AD_CONTRACT_CHECK(comp_ != nullptr);
    }
    if constexpr (std::is_pointer_v<Projection> ||
                  std::is_member_pointer_v<Projection>) {
      AD_CONTRACT_CHECK(proj != nullptr);
    }
    decision_ = decide();
  }

  ZipMergeUniqueIteratorImpl() = default;

 private:
  enum class Decision {
    UseFirst,           // yield *it1_ and advance it1_
    UseSecond,          // yield *it2_ and advance it2_
    UseSecondSkipFirst  // equal projection: yield *it2_, advance both
  };

  Decision decision_;

  // Determine which element to yield next.
  Decision decide() const {
    if (it2_ == end2_) return Decision::UseFirst;
    if (it1_ == end1_) return Decision::UseSecond;
    const auto& p1 = std::invoke(proj_, *it1_);
    const auto& p2 = std::invoke(proj_, *it2_);
    if (std::invoke(comp_, p1, p2)) return Decision::UseFirst;
    if (std::invoke(comp_, p2, p1)) return Decision::UseSecond;
    return Decision::UseSecondSkipFirst;
  }

 public:
  reference operator*() const {
    return (decision_ == Decision::UseFirst) ? *it1_ : *it2_;
  }
  pointer operator->() const {
    return (decision_ == Decision::UseFirst ? it1_.operator->()
                                            : it2_.operator->());
  }

  ZipMergeUniqueIteratorImpl& operator++() {
    switch (decision_) {
      case Decision::UseFirst:
        ++it1_;
        break;
      case Decision::UseSecond:
        ++it2_;
        break;
      case Decision::UseSecondSkipFirst:
        ++it1_;
        ++it2_;
        break;
    }
    decision_ = decide();
    return *this;
  }
  ZipMergeUniqueIteratorImpl operator++(int) {
    ZipMergeUniqueIteratorImpl tmp = *this;
    ++(*this);
    return tmp;
  }

  bool operator==(const ZipMergeUniqueIteratorImpl& o) const {
    return std::tie(it1_, end1_, it2_, end2_) ==
           std::tie(o.it1_, o.end1_, o.it2_, o.end2_);
  }
  // An explicit definition is required for C++17 compatibility.
  bool operator!=(const ZipMergeUniqueIteratorImpl& other) const {
    return !(*this == other);
  }
};

struct ZipMergeUniqueIteratorStruct {
  CPP_template(typename It, typename Compare = std::less<>,
               typename Projection = ql::identity)(requires true)
      ZipMergeUniqueIteratorImpl<It, Compare, Projection>
      operator()(It begin1, It end1, It begin2, It end2, Compare cmp = {},
                 Projection proj = {}) const {
    return ZipMergeUniqueIteratorImpl<It, Compare, Projection>(
        begin1, end1, begin2, end2, std::move(cmp), std::move(proj));
  }

  CPP_template(typename Range1, typename Range2, typename Compare = std::less<>,
               typename Projection = ql::identity)(
      requires(ql::ranges::borrowed_range<std::remove_reference_t<Range1>> ||
               std::is_lvalue_reference_v<Range1>) &&
      (ql::ranges::borrowed_range<std::remove_reference_t<Range2>> ||
       std::is_lvalue_reference_v<Range2>)) auto
  operator()(Range1&& range1, Range2&& range2, Compare cmp = {},
             Projection proj = {}) const {
    return (*this)(ql::ranges::begin(range1), ql::ranges::end(range1),
                   ql::ranges::begin(range2), ql::ranges::end(range2),
                   std::move(cmp), std::move(proj));
  }
};

}  // namespace detail

constexpr detail::ZipMergeUniqueIteratorStruct zipUniqueIterator;

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_ZIPMERGEITERATOR_H
