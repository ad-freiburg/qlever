// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#pragma once

#include <functional>

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
//
// Both ranges must have the same `range_reference_t`.
//
// Use the `mergeInputRange` variable below to construct instances without
// naming the template parameters.
CPP_template(typename Range1, typename Range2, typename Compare,
             typename Projection)(
    requires ql::ranges::input_range<Range1> CPP_and ql::ranges::input_range<
        Range2>
        CPP_and ql::concepts::same_as<
            ql::ranges::range_reference_t<Range1>,
            ql::ranges::range_reference_t<Range2>>) class MergeInputRangeImpl
    : public InputRangeMixin<
          MergeInputRangeImpl<Range1, Range2, Compare, Projection>> {
  Range1 range1_;
  Range2 range2_;
  Compare compare_;
  Projection proj_;

  ql::ranges::iterator_t<Range1> it1_;
  ql::ranges::sentinel_t<Range1> end1_;
  ql::ranges::iterator_t<Range2> it2_;
  ql::ranges::sentinel_t<Range2> end2_;

  // The three possible outcomes of `decide()`.
  enum class Decision {
    UseFirst,           // yield *it1_ and advance it1_
    UseSecond,          // yield *it2_ and advance it2_
    UseSecondSkipFirst  // equal projection: yield *it2_, advance both
  };

  // Determine which element to yield next.
  Decision decide() const {
    if (it2_ == end2_) return Decision::UseFirst;
    if (it1_ == end1_) return Decision::UseSecond;
    const auto& p1 = std::invoke(proj_, *it1_);
    const auto& p2 = std::invoke(proj_, *it2_);
    if (std::invoke(compare_, p1, p2)) return Decision::UseFirst;
    if (std::invoke(compare_, p2, p1)) return Decision::UseSecond;
    return Decision::UseSecondSkipFirst;
  }

 public:
  using reference = ql::ranges::range_reference_t<Range1>;

  MergeInputRangeImpl(Range1 r1, Range2 r2, Compare cmp, Projection proj)
      : range1_(std::move(r1)),
        range2_(std::move(r2)),
        compare_(std::move(cmp)),
        proj_(std::move(proj)) {}

  // `InputRangeMixin` interface.
  void start() {
    it1_ = ql::ranges::begin(range1_);
    end1_ = ql::ranges::end(range1_);
    it2_ = ql::ranges::begin(range2_);
    end2_ = ql::ranges::end(range2_);
  }

  bool isFinished() const { return it1_ == end1_ && it2_ == end2_; }

  reference get() { return decide() == Decision::UseFirst ? *it1_ : *it2_; }

  void next() {
    switch (decide()) {
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
  }
};

}  // namespace detail

// Constructor struct for `MergeInputRange`. Use the `mergeInputRange` variable
// below — no need to instantiate this struct manually.
struct MergeInputRangeStruct {
  // Merge `r1` and `r2` (both must be sorted by `proj` + `cmp`) into a single
  // sorted sequence. When `proj(*it1) == proj(*it2)` the element from `r2` is
  // yielded and the element from `r1` is skipped.
  CPP_template(typename R1, typename R2, typename Compare = std::less<>,
               typename Projection = ql::identity)(
      requires ql::ranges::input_range<R1> CPP_and ql::ranges::input_range<R2>
          CPP_and ql::concepts::same_as<ql::ranges::range_reference_t<R1>,
                                        ql::ranges::range_reference_t<R2>>)
      detail::MergeInputRangeImpl<std::decay_t<R1>, std::decay_t<R2>, Compare,
                                  Projection>
      operator()(R1&& r1, R2&& r2, Compare cmp = {},
                 Projection proj = {}) const {
    return {std::forward<R1>(r1), std::forward<R2>(r2), std::move(cmp),
            std::move(proj)};
  }
};

// A variable so callers write `mergeInputRange(r1, r2)` or
// `mergeInputRange(r1, r2, cmp, proj)` without naming template parameters.
constexpr MergeInputRangeStruct mergeInputRange;

}  // namespace ad_utility
