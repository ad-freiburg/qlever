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
#include <iterator>
#include <type_traits>

#include "backports/algorithm.h"

namespace ql {

///
// Shifts the elements in the range [first, last) by n positions towards the
// beginning of the range.
//
// If n == 0 || n >= last - first, there are no effects.
// Otherwise, for every integer i in [0, last - first - n), moves the element
// at position first + n + i to position first + i.
///
CPP_template(typename ForwardIt)(
    requires ql::concepts::forward_iterator<ForwardIt>) constexpr ForwardIt
    shift_left(ForwardIt first, ForwardIt last,
               typename std::iterator_traits<ForwardIt>::difference_type n) {
  assert(n >= 0);
  if (n == 0) {
    return first;
  }

  auto mid = ql::ranges::next(first, n, last);
  if (mid == last) {
    return first;
  }
  return std::move(std::move(mid), std::move(last), std::move(first));
}

///
// Shifts the elements in the range [first, last) by n positions towards the end
// of the range.
//
// If n == 0 || n >= last - first, there are no effects.
// Otherwise, for every integer i in [0, last - first - n), moves the element
// at position first + i to position first + n + i.
///
CPP_template(typename ForwardIt)(
    requires ql::concepts::forward_iterator<ForwardIt>) constexpr ForwardIt
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
