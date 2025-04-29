#ifndef QLEVER_SRC_BACKPORTS_SHIFT_H
#define QLEVER_SRC_BACKPORTS_SHIFT_H

// This header implements the cpp20 functions std::shift_left and
// std::shift_right

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
  if (n == 0) {
    return last;
  }
  static_assert(ql::concepts::bidirectional_iterator<ForwardIt>,
                "ql::shift_right is not yet implemented for "
                "`forward_iterator`s. If you need it, please add it (see for "
                "example the somewhat complicated but efficient implementation "
                "in libstdc++, and don't forget to write unit tests");
  auto mid = ql::ranges::next(last, -n, first);
  if (mid == first) {
    return last;
  }

  return std::move_backward(std::move(first), std::move(mid), std::move(last));
}

}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_SHIFT_H
