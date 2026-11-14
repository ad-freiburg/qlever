// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_BACKPORTS_SHIFT_H
#define QLEVER_SRC_BACKPORTS_SHIFT_H

///
// This header implements the cpp20 functions shift_left and
// shift_right based on https://en.cppreference.com/w/cpp/algorithm/shift
///

#include <algorithm>
#include <cassert>

#include "backports/algorithm.h"
#include "backports/iterator.h"
#include "backports/type_traits.h"

namespace ql {

namespace detail {
// An iterator can have its own `shiftLeft`, see `BasicIdColumnIterator`.
template <typename It>
CPP_requires(hasMemberShiftLeft_,
             requires(It first, It last)(first.shiftLeft(last, 0)));
template <typename It>
CPP_concept HasMemberShiftLeft = CPP_requires_ref(hasMemberShiftLeft_, It);

struct ShiftLeftFn {
  CPP_template(typename It)(requires ql::concepts::forward_iterator<It>)
  constexpr It operator()(It first, It last,
                          ql::iter_difference_t<It> n) const {
    assert(n >= 0);
    if constexpr (HasMemberShiftLeft<It>) {
      return first.shiftLeft(last, n);
    } else {
      auto mid = ql::ranges::next(first, n, last);
      if (n == 0 || mid == last) {
        return first;
      }
      // Unlike the classic `std::move`, this also works for proxy references.
      return ql::ranges::move(mid, last, first).out;
    }
  }
};
}  // namespace detail

///
// Shifts the elements in the range [first, last) by n positions towards the
// beginning of the range.
//
// If n == 0 || n >= last - first, there are no effects.
// Otherwise, for every integer i in [0, last - first - n), moves the element
// at position first + n + i to position first + i.
//
// A customization point object (like `ql::ranges::swap`): if the iterator has a
// member `first.shiftLeft(last, n)`, it is used instead.
///
inline constexpr detail::ShiftLeftFn shift_left{};

///
// Shifts the elements in the range [first, last) by n positions towards the end
// of the range.
//
// If n == 0 || n >= last - first, there are no effects.
// Otherwise, for every integer i in [0, last - first - n), moves the element
// at position first + i to position first + n + i.
///
CPP_template(typename ForwardIt)(
    requires ql::concepts::forward_iterator<ForwardIt>)
constexpr ForwardIt
    shift_right(ForwardIt first, ForwardIt last,
                typename std::iterator_traits<ForwardIt>::difference_type n) {
  assert(n >= 0);
  if (n == 0 || n >= std::distance(first, last)) {
    return last;
  }
  if constexpr (std::is_base_of_v<std::bidirectional_iterator_tag,
                                  typename std::iterator_traits<
                                      ForwardIt>::iterator_category>) {
    auto srcEnd = std::next(first, std::distance(first, last) - n);
    return std::move_backward(first, srcEnd, last);
  } else {
    auto result = std::next(first, n);
    if (result == last) return last;
    auto destHead = first;
    auto destTail = result;
    while (destHead != result) {
      if (destTail == last) {
        std::move(std::move(first), std::move(destHead), result);
        return result;
      }
      ++destHead;
      ++destTail;
    }
    for (;;) {
      auto cursor = first;
      while (cursor != result) {
        if (destTail == last) {
          destHead = std::move(cursor, result, std::move(destHead));
          std::move(std::move(first), std::move(cursor), std::move(destHead));
          return result;
        }
        std::iter_swap(cursor, destHead);
        ++destHead;
        ++destTail;
        ++cursor;
      }
    }
  }
}

}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_SHIFT_H
