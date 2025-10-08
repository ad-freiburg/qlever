// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_VIEWS_TAKEUNTILINCLUSIVEVIEW_H
#define QLEVER_SRC_UTIL_VIEWS_TAKEUNTILINCLUSIVEVIEW_H
#include "backports/iterator.h"
#include "util/Views.h"

namespace ad_utility {
// Implement `TakeUntilInclusiveView` (the type) and
// `ad_utility::views::takeUntilInclusive` (the pipeable range adaptor object)
// with the following characteristics:
// It is given a `view` and a `predicate` and yields the elements of the `view`
// as long as they don't fulfill the predicate. The important distinction to
// `take_while` is, that the first element that fulfills the predicate is also
// yielded, meaning that `predicate(el) == true` means that the current element
// is the *last* element of the view.
// So `TakeUntilInclusive{{0, 2, 4, 3, 5, 6}, isOdd}` will yield `0, 2, 4, 3`,
// because `3` is the first odd element.
// NOTE:
// 1. The predicate is evaluated during `operator*`. Only if `operator*` is not
// called for an element of the view, it is evaluated during `operator++`. This
// allows the consumer to move out the current element if desired. It has the
// caveat however, that `operator*` is NOT THREADSAFE, although it is marked
// `const` because it has to modify mutable state that tracks, whether the
// predicate has already been evaluated for the current element.
CPP_template(typename V, typename Pred)(
    requires ql::ranges::input_range<V>&& ql::ranges::view<V>&&
        std::is_object_v<Pred>&& std::indirect_unary_predicate<
            const Pred, ql::ranges::iterator_t<V>>) class TakeUntilInclusiveView
    : public ql::ranges::view_interface<TakeUntilInclusiveView<V, Pred>> {
 private:
  V base_;
  Pred pred_;

  class Iterator {
   public:
    using BaseIter = ql::ranges::iterator_t<V>;
    using BaseSentinel = ql::ranges::sentinel_t<V>;
    using iterator_category = std::input_iterator_tag;
    using value_type = ql::ranges::range_value_t<V>;
    using reference = ql::ranges::range_reference_t<V>;
    using difference_type = ql::ranges::range_difference_t<V>;

   private:
    BaseIter current_;
    BaseSentinel end_;
    const Pred* pred_;

    bool done_ = false;
    // Note: The following two have to be `mutable`, because the operator* has
    // to be `const` to fulfill the concepts for an iterator.

    // If `true` then the predicate was already evaluated during `operator*`,
    // and thus the check can be skipped during `operator++`.
    mutable bool doneWasChecked_ = false;
    // If `doneWasChecked` is true, then this variable stores the result of the
    // predicate evaluation during `operator*`.
    mutable bool doneAfterIncrement_ = false;

   public:
    Iterator() = default;
    Iterator(BaseIter current, BaseSentinel end, const Pred* pred)
        : current_(current), end_(end), pred_(pred) {}

    // _________________________________________________________________________
    decltype(auto) operator*() const {
      decltype(auto) res = *current_;
      // evaluate the predicate exactly once per element, and store the result
      // of the evaluation.
      if (!std::exchange(doneWasChecked_, true)) {
        doneAfterIncrement_ = std::invoke(*pred_, res);
      }
      // We have to distinguish the case of `*current_` returning a reference
      // vs. an object.
      if constexpr (std::is_object_v<decltype(res)>) {
        // For objects, this is NRVO (almost guaranteed copy-elision).
        return res;
      } else {
        // Perfect forwarding for references.
        return AD_FWD(res);
      }
    }

    Iterator& operator++() {
      // Only evaluate the predicate, if it hasn't been evaluated during
      // `operator*`, e.g. because an element is skipped without being looked at
      // (e.g. in a `views::drop`).
      if (doneWasChecked_) {
        done_ = doneAfterIncrement_;
      } else if (std::invoke(*pred_, *current_)) {
        done_ = true;  // Stop after this one
      }

      // Don't advance the iterator if we are done. While being semantically
      // valid, this advancing might be expensive depending on the underlying
      // view.
      if (!done_) {
        ++current_;
      }
      // Reset the state for the next element.
      doneWasChecked_ = false;
      doneAfterIncrement_ = false;
      return *this;
    }

    void operator++(int) { ++(*this); }

    // TODO<joka921> Implement `ql::default_sentinel`
    bool operator==(ql::default_sentinel_t) const {
      return done_ || current_ == end_;
    }
  };

 public:
  TakeUntilInclusiveView() = default;
  TakeUntilInclusiveView(V base, Pred pred)
      : base_(std::move(base)), pred_(std::move(pred)) {}

  auto begin() {
    return Iterator{ql::ranges::begin(base_), ql::ranges::end(base_), &pred_};
  }

  auto end() { return ql::default_sentinel; }
};

// Deduction guides.
template <class R, class Pred>
TakeUntilInclusiveView(R&&, Pred) -> TakeUntilInclusiveView<all_t<R>, Pred>;

namespace views {
// Implement the required machinery to make `takeUntilInclusive` pipeable with
// other views.
struct takeUntilInclusiveFn {
  CPP_template(typename R,
               typename Pred)(requires ql::ranges::viewable_range<R>) auto
  operator()(R&& r, Pred pred) const {
    return TakeUntilInclusiveView{std::views::all(std::forward<R>(r)),
                                  std::move(pred)};
  }

  template <typename Pred>
  constexpr auto operator()(Pred pred) const {
    return ::ranges::make_view_closure(CPP_lambda(pred = std::move(pred))(
        auto&& r)(requires ql::ranges::viewable_range<decltype(r)>) {
      return TakeUntilInclusiveView(AD_FWD(r), pred);
    });
  }
};

// Definition of the range adaptor object
// `ad_utility::views::takeUntilInclusive`.
inline constexpr takeUntilInclusiveFn takeUntilInclusive;
}  // namespace views
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_VIEWS_TAKEUNTILINCLUSIVEVIEW_H
