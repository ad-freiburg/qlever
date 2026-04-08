// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#pragma once

#include <functional>
#include <iterator>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "backports/functional.h"
#include "util/Iterators.h"

namespace ad_utility {
namespace detail {

// Lazily merges two sorted input ranges into a single sorted sequence.
//
// Elements are yielded one at a time. The `Compare` function is applied to the
// projected values (`Projection` applied to each element) to determine order.
// When both ranges contain an element that compares equal under the projection,
// only the element from the *second* range is yielded and the element from the
// *first* range is skipped (last-wins tie-break).
CPP_template(typename It, typename Compare = std::less<>,
             class Projection = ql::identity)(
    requires true) class ZipMergeIteratorImpl {
 public:
  using iterator_category = std::input_iterator_tag;
  using iterator_concept = std::input_iterator_tag;
  using value_type = std::iterator_traits<It>::value_type;
  using difference_type = std::ptrdiff_t;
  using pointer = std::iterator_traits<It>::pointer;
  using reference = std::iterator_traits<It>::reference;

 private:
  It it1;
  It end1;
  It it2;
  It end2;
  Compare comp;
  Projection proj;

 public:
  explicit ZipMergeIteratorImpl(It b1, It e1, It b2, It e2, Compare cmp = {},
                                Projection proj = {})
      : it1(b1), end1(e1), it2(b2), end2(e2), comp(cmp), proj(proj) {}

  ZipMergeIteratorImpl() = default;

 private:
  enum class Decision {
    UseFirst,           // yield *it1_ and advance it1_
    UseSecond,          // yield *it2_ and advance it2_
    UseSecondSkipFirst  // equal projection: yield *it2_, advance both
  };

  // Determine which element to yield next.
  Decision decide() const {
    if (it2 == end2) return Decision::UseFirst;
    if (it1 == end1) return Decision::UseSecond;
    const auto& p1 = std::invoke(proj, *it1);
    const auto& p2 = std::invoke(proj, *it2);
    if (std::invoke(comp, p1, p2)) return Decision::UseFirst;
    if (std::invoke(comp, p2, p1)) return Decision::UseSecond;
    return Decision::UseSecondSkipFirst;
  }

 public:
  reference operator*() const {
    return (decide() == Decision::UseFirst) ? *it1 : *it2;
  }
  pointer operator->() const {
    return (decide() == Decision::UseFirst ? it1.operator->()
                                           : it2.operator->());
  }

  ZipMergeIteratorImpl& operator++() {
    switch (decide()) {
      case Decision::UseFirst:
        ++it1;
        break;
      case Decision::UseSecond:
        ++it2;
        break;
      case Decision::UseSecondSkipFirst:
        ++it1;
        ++it2;
        break;
    }
    return *this;
  }
  ZipMergeIteratorImpl operator++(int) {
    ZipMergeIteratorImpl tmp = *this;
    ++(*this);
    return tmp;
  }

  bool operator==(const ZipMergeIteratorImpl& o) const {
    return std::tie(it1, end1, it2, end2) ==
           std::tie(o.it1, o.end1, o.it2, o.end2);
  }
};

}  // namespace detail

struct ZipIteratorStruct {
  CPP_template(typename It, typename Compare = std::less<>,
               typename Projection = ql::identity)(
      requires true) detail::ZipMergeIteratorImpl<It, Compare, Projection>
  operator()(It begin1, It end1, It begin2, It end2, Compare cmp = {},
             Projection proj = {}) const {
    return detail::ZipMergeIteratorImpl<It, Compare, Projection>(
        std::forward<It>(begin1), std::forward<It>(end1),
        std::forward<It>(begin2), std::forward<It>(end2), std::move(cmp),
        std::move(proj));
  }
};

constexpr ZipIteratorStruct zipIterator;
}  // namespace ad_utility
