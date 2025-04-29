//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#pragma once
#include <optional>

#include "backports/algorithm.h"
#include "util/Views.h"

// This file contains helper classes that can be used to replace
// `cppcoro::generator`s in C++17 mode by providing abstractions for various
// typical abstractions for input ranges.

namespace ad_utility {
// This class is similar to `ql::views::transform` with the following
// differences:
// 1. The new values are computed when the iterators are advanced, not when they
// are dereferenced. This makes the behavior more efficient if an iterator is
// dereferenced multiple times, and makes the behavior correct if an iterator is
// dereferenced multiple times AND the transformation functors modifies its
// input (e.g. by moving it).
// 2. The computed values are therefore cached (using the `InputRangeFromGet`
// facility), which might have a small performance overhead, as results that are
// used only once have to be moved and can't benefit from return value
// optimization (RVO).
// 3. This class only yields an input range, independent of the range category
// of the input.
CPP_class_template(typename View, typename F)(requires(
    ql::ranges::input_range<View>&& ql::ranges::view<View>&&
        std::is_object_v<F>&&
            ranges::invocable<F, ql::ranges::range_reference_t<
                                     View>>)) struct CachingTransformInputRange
    : InputRangeFromGet<std::decay_t<
          std::invoke_result_t<F, ql::ranges::range_reference_t<View>>>> {
 private:
  using Res = std::invoke_result_t<F, ql::ranges::range_reference_t<View>>;
  static_assert(std::is_object_v<Res>,
                "The functor of `CachingTransformInputRange` must yield an "
                "object type, not a reference");

  // The input view, the function, and the current iterator into the `view_`.
  // The iterator is `nullopt` before the first call to `get`.
  View view_;
  F transfomation_;
  std::optional<ql::ranges::iterator_t<View>> it_;

 public:
  // Constructor.
  explicit CachingTransformInputRange(View view, F transformation = {})
      : view_{std::move(view)}, transfomation_(std::move(transformation)) {}

  // TODO<joka921> Make this private again and give explicit access to low-level
  // tools like the ones below.
 public:
  // The `get` function required by the mixin.
  std::optional<Res> get() {
    // We deliberately defer the call to begin, to make the behavior consistent
    // with lazy `generator`s.
    if (!it_.has_value()) {
      it_ = view_.begin();
    } else if (*it_ != view_.end()) {
      ++(it_.value());
    }
    if (*it_ == view_.end()) {
      return std::nullopt;
    }
    return std::invoke(transfomation_, *it_.value());
  }

  // Get access to the underlying view.
  const View& underlyingView() { return view_; }

  // Give the mixin access to the private `get` function.
  friend class InputRangeFromGet<Res>;
};

// Deduction guides to correctly propagate the input as a value or reference.
// This is the exact same way `std::ranges` and `range-v3` behave.
template <typename Range, typename F>
CachingTransformInputRange(Range&&, F)
    -> CachingTransformInputRange<all_t<Range>, F>;

namespace loopControl {
// A class to represent control flows in generator-like state machines,
// like `break`, `continue`, or `yield a value`.
template <typename T>
struct LoopControl {
 private:
  // A `continue` statement of a loop.
  struct Continue {};
  // A `break` statement of a loop.
  struct Break {};
  // A statement that first yields a value and then immediately breaks the loop.
  struct BreakWithValue {
    T value_;
  };

 public:
  using type = T;
  using V = std::variant<T, Continue, Break, BreakWithValue>;
  V v_;
  bool isContinue() const { return std::holds_alternative<Continue>(v_); }
  bool isBreak() const {
    return std::holds_alternative<Break>(v_) ||
           std::holds_alternative<BreakWithValue>(v_);
  }

  // If the variant either holds a `T` or a `BreakWithValue`, return the `T` by
  // moving it out, else `nullopt`.
  std::optional<T> moveValueIfPresent() {
    auto visitor = [](auto& v) -> std::optional<T> {
      using U = std::remove_reference_t<decltype(v)>;
      if constexpr (ad_utility::SimilarToAny<U, Continue, Break>) {
        return std::nullopt;
      } else if constexpr (ad_utility::SimilarTo<U, T>) {
        return std::move(v);
      } else {
        static_assert(ad_utility::SimilarTo<U, BreakWithValue>);
        return std::move(v.value_);
      }
    };
    return std::visit(visitor, v_);
  }

  // Factory functions to create all possible values.
  static LoopControl makeContinue() { return LoopControl{Continue{}}; }
  static LoopControl makeBreak() { return LoopControl{Break{}}; }
  static LoopControl breakWithValue(T t) {
    return LoopControl{BreakWithValue{std::move(t)}};
  }
  static LoopControl yieldValue(T t) { return LoopControl{std::move(t)}; }
};

// `loopControlValueT` is a helper variable to get the stored type out of a
// `LoopControl` object. This can be used for constraints etc. in the definition
// of classes that use the `LoopControl`.
template <typename T>
struct LoopControlValue {};

template <typename T>
struct LoopControlValue<LoopControl<T>> {
  using type = typename LoopControl<T>::type;
};

template <typename T>
using loopControlValueT = typename LoopControlValue<T>::type;

}  // namespace loopControl

// expose the `LoopControl` class in the `ad_utility` namespace.
template <typename T>
using LoopControl = loopControl::LoopControl<T>;

// This class can be used to simulate a generator that consists of a single
// for-loop, but where the loop contains control-flows such as `break` and
// `continue`. It is given a `View` and a functor `F` which must return a
// `LoopControl` object (see above) when being called with the elements of the
// `View`. It has the following semantics:
// 1. In principle the functor `F` is called for each element in the input
// (unless there is a `break`, see below).
// 2. If the value of `F(curElement)` is a `Continue` object, then the current
// element is skipped, and nothing is yielded.
// 3. If the value of `F(curElement)` is a `Break` object, then nothing is
// yielded in the current step, and the iteration stops completely.
// 4. If the value of `F(curElement)` is a `BreakWithValue` object, then the
// value is yielded, but the iteration stops afterward (this is similar to a
// `return` statement in a for-loop).
// 5. Otherwise, the value of `F(curElement)` is a plain value, then that value
// is yielded and the iteration resumes.

// First some small helpers to get the actual value types of the classes below.
namespace detail {
template <typename View, typename F>
using Res = loopControl::loopControlValueT<
    std::invoke_result_t<F, ql::ranges::range_reference_t<View>>>;

template <typename F>
using ResFromFunction = loopControl::loopControlValueT<std::invoke_result_t<F>>;
}  // namespace detail

// A class that allows to synthesize an input range directly from a callable
// that returns `LoopControl<T>`.
CPP_class_template(typename F)(
    requires std::is_object_v<F>) struct InputRangeFromLoopControlGet
    : InputRangeFromGet<detail::ResFromFunction<F>> {
 private:
  F getFunction_;
  using Res = detail::ResFromFunction<F>;
  static_assert(std::is_object_v<Res>,
                "The functor of `InputRangeFromLoopControlGet` must yield an "
                "object type, not a reference");

  // If set to true then we have seen a `break` statement and no more values
  // are yielded.
  bool receivedBreak_ = false;

 public:
  // Constructor.
  explicit InputRangeFromLoopControlGet(F transformation = {})
      : getFunction_{std::move(transformation)} {}

  // The central `get` function, required by the mixin.
  std::optional<Res> get() {
    if (receivedBreak_) {
      return std::nullopt;
    }
    // This loop is executed exactly once unless there is a `continue`
    // statement.
    while (true) {
      auto loopControl = getFunction_();
      if (loopControl.isContinue()) {
        continue;
      }
      if (loopControl.isBreak()) {
        receivedBreak_ = true;
      }
      return loopControl.moveValueIfPresent();
    }
  }
};

// A class that takes a view and a function that transforms the elements of the
// view into a `LoopControl` object, and synthesizes an `input_range` from these
// arguments.
CPP_class_template(typename View, typename F)(requires(
    ql::ranges::input_range<View>&& ql::ranges::view<View>&&
        std::is_object_v<F>)) struct CachingContinuableTransformInputRange
    : InputRangeFromGet<detail::Res<View, F>> {
 private:
  using Res = detail::Res<View, F>;
  static_assert(
      std::is_object_v<Res>,
      "The functor of `CachingContinuableTransformInputRange` must yield an "
      "object type, not a reference");

  // The actual iteration is handled by `CachingTransformInputRange`, we only
  // add the control-flow layer.
  ad_utility::CachingTransformInputRange<View, F> impl_;
  // If set to true then we have seen a `break` statement and no more values
  // are yielded.
  bool receivedBreak_ = false;

 public:
  // Constructor.
  explicit CachingContinuableTransformInputRange(View view,
                                                 F transformation = {})
      : impl_{std::move(view), std::move(transformation)} {}

 private:
  // The central `get` function, required by the mixin.
  std::optional<Res> get() {
    if (receivedBreak_) {
      return std::nullopt;
    }
    // This loop is executed exactly once unless there is a `continue`
    // statement.
    while (true) {
      auto opt = impl_.get();
      if (!opt.has_value()) {
        return std::nullopt;
      }
      auto& loopControl = opt.value();
      if (loopControl.isContinue()) {
        continue;
      }
      if (loopControl.isBreak()) {
        receivedBreak_ = true;
      }
      return loopControl.moveValueIfPresent();
    }
  }
  friend class InputRangeFromGet<Res>;
};

template <typename Range, typename F>
CachingContinuableTransformInputRange(Range&&, F)
    -> CachingContinuableTransformInputRange<all_t<Range>, F>;
}  // namespace ad_utility
