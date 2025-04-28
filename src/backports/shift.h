#ifndef QLEVER_SRC_BACKPORTS_SHIFT_H
#define QLEVER_SRC_BACKPORTS_SHIFT_H

// This header implements the cpp20 functions std::shift_left and
// std::shift_right

#include <algorithm>
#include <iterator>
#include <type_traits>

namespace ql {

///
// Shifts the elements in the range [first, last) by n positions towards the
// beginning of the range.
//
// If n == 0 || n >= last - first, there are no effects.
// Otherwise, for every integer i in [0, last - first - n), moves the element
// at position first + n + i to position first + i.
///
template <class ForwardIt>
constexpr ForwardIt shift_left(
    ForwardIt first, ForwardIt last,
    typename std::iterator_traits<ForwardIt>::difference_type n) {
  if (n == 0 || n >= std::distance(first, last)) {
    return first;
  }
  ForwardIt result = first;
  ForwardIt input_it = std::next(first, n);

  return std::move(input_it, last, result);
}

///
// Shifts the elements in the range [first, last) by n positions towards the end
// of the range.
//
// If n == 0 || n >= last - first, there are no effects.
// Otherwise, for every integer i in [0, last - first - n), moves the element
// at position first + i to position first + n + i.
///
template <class ForwardIt>
constexpr ForwardIt shift_right(
    ForwardIt first, ForwardIt last,
    typename std::iterator_traits<ForwardIt>::difference_type n) {
  if (n == 0 || n >= std::distance(first, last)) {
    return last;
  }
  auto result = std::next(first, n);
  if constexpr (std::is_base_of_v<std::bidirectional_iterator_tag,
                                  typename std::iterator_traits<
                                      ForwardIt>::iterator_category>) {
    auto src_end = std::next(first, std::distance(first, last) - n);
    std::move_backward(first, src_end, last);
  } else {
    using value_type = typename std::iterator_traits<ForwardIt>::value_type;
    std::vector<value_type> temp(
        first, std::next(first, std::distance(first, last) - n));
    std::move(temp.begin(), temp.end(), result);
  }
  return result;
}

}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_SHIFT_H
