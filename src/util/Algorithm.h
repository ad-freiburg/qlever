// Copyright 2022 - 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Julian Mundhahs <mundhahj@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#ifndef QLEVER_ALGORITHM_H
#define QLEVER_ALGORITHM_H

#include <numeric>
#include <string>
#include <string_view>
#include <utility>

#include "backports/algorithm.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/HashSet.h"
#include "util/Iterators.h"
#include "util/TypeTraits.h"

namespace ad_utility {

namespace algorithm::detail {
// Concept implementations for the `contains` function below. Checks whether a
// type has a member function `contains` or `find`respectively.
template <typename T, typename U>
CPP_requires(HasMemberContains,
             requires(const T& t, const U& u)(t.contains(u)));
template <typename T, typename U>
CPP_requires(HasMemberFind, requires(const T& t, const U& u)(t.find(u)));
}  // namespace algorithm::detail

/**
 * Checks whether an element is contained in a container.
 *  Works on the following types of containers:
 *  1.  `std::string_[view]`, where you also can find substrings.
 *  2. containers that either have a member function `contains` that returns a
 *     bool, or alternatively a member function `find` that returns an iterator.
 *  3. For all other containers, the generic `ql::ranges::find` algorithm is
 * used.
 *
 * @param container Container& Elements to be searched
 * @param element T Element to search for
 * @return bool
 */
template <typename Container, typename T>
constexpr bool contains(Container&& container, const T& element) {
  // Overload for types like std::string that have a `find` member function
  if constexpr (ad_utility::isSimilar<Container, std::string> ||
                ad_utility::isSimilar<Container, std::string_view>) {
    return container.find(element) != container.npos;
  } else if constexpr (CPP_requires_ref(algorithm::detail::HasMemberContains,
                                        const Container&, T)) {
    return container.contains(element);
  } else if constexpr (CPP_requires_ref(algorithm::detail::HasMemberFind,
                                        const Container&, T)) {
    return container.find(element) != container.end();
  } else {
    return ql::ranges::find(std::begin(container), std::end(container),
                            element) != std::end(container);
  }
}

/**
 * Checks whether an element in the container satisfies the predicate.
 *
 * @param container Container& Elements to be searched
 * @param predicate Predicate Predicate to check
 * @return bool
 */
template <typename Container, typename Predicate>
bool contains_if(const Container& container, const Predicate& predicate) {
  return std::find_if(container.begin(), container.end(), predicate) !=
         container.end();
}

/**
 * Appends the second vector to the first one.
 *
 * @param destination Vector& to which to append
 * @param source Vector&& to append
 */
CPP_template(typename T, typename U)(
    requires ad_utility::SimilarTo<
        std::vector<T>, U>) void appendVector(std::vector<T>& destination,
                                              U&& source) {
  destination.insert(destination.end(),
                     ad_utility::makeForwardingIterator<U>(source.begin()),
                     ad_utility::makeForwardingIterator<U>(source.end()));
}

/**
 * Applies the UnaryOperation to all elements of the range and return a new
 * vector which contains the results.
 */
template <typename Range, typename F>
auto transform(Range&& input, F unaryOp) {
  using Output = std::decay_t<decltype(std::invoke(
      unaryOp, *ad_utility::makeForwardingIterator<Range>(input.begin())))>;
  std::vector<Output> out;
  out.reserve(input.size());
  ql::ranges::transform(
      ad_utility::makeForwardingIterator<Range>(input.begin()),
      ad_utility::makeForwardingIterator<Range>(input.end()),
      std::back_inserter(out), unaryOp);
  return out;
}
/*
@brief Takes two vectors, pairs up their content at the same index positions
and copies them into `std::pair`s, who are returned inside a vector.
Example: `{1,2}` and `{3,4}` are returned as `{(1,3), (2,4)}`.
*/
template <typename T1, typename T2>
std::vector<std::pair<T1, T2>> zipVectors(const std::vector<T1>& vectorA,
                                          const std::vector<T2>& vectorB) {
  // Both vectors must have the same length.
  AD_CONTRACT_CHECK(vectorA.size() == vectorB.size());

  std::vector<std::pair<T1, T2>> vectorsPairedUp{};
  vectorsPairedUp.reserve(vectorA.size());

  ql::ranges::transform(
      vectorA, vectorB, std::back_inserter(vectorsPairedUp),
      [](const auto& a, const auto& b) { return std::make_pair(a, b); });

  return vectorsPairedUp;
}

/**
 * Flatten a vector<vector<T>> into a vector<T>. Currently requires an rvalue
 * (temporary or `std::move`d value) as an input.
 */
template <typename T>
std::vector<T> flatten(std::vector<std::vector<T>>&& input) {
  std::vector<T> out;
  // Reserve the total required space. It is the sum of all the vectors
  // lengths.
  out.reserve(std::accumulate(
      input.begin(), input.end(), 0,
      [](size_t i, const std::vector<T>& elem) { return i + elem.size(); }));
  for (auto& sub : input) {
    // As the input is an rvalue, it is save to always move.
    appendVector(out, std::move(sub));
  }
  return out;
}

// Remove duplicates in the given vector without changing the order. For
// example: 4, 6, 6, 2, 2, 4, 2 becomes 4, 6, 2.
//
// NOTE: This makes two copies of the first element in `input` with a
// particular value. One copy for the result, and one copy for the hash set
// used to keep track of which values we have already seen. One of these
// copies could be avoided, but our current uses of this function are
// currently not at all performance-critical (small `input` and small `T`).
CPP_template(typename Range)(requires ql::ranges::forward_range<
                             Range>) auto removeDuplicates(const Range& input)
    -> std::vector<typename std::iterator_traits<
        ql::ranges::iterator_t<Range>>::value_type> {
  using T =
      typename std::iterator_traits<ql::ranges::iterator_t<Range>>::value_type;
  std::vector<T> result;
  ad_utility::HashSet<T> distinctElements;
  for (const T& element : input) {
    if (!distinctElements.contains(element)) {
      result.emplace_back(element);
      distinctElements.insert(element);
    }
  }
  return result;
}

// Return a new `std::input` that is obtained by applying the `function` to each
// of the elements of the `input`.
CPP_template(typename Array, typename Function)(
    requires ad_utility::isArray<std::decay_t<Array>> CPP_and
        ql::concepts::invocable<
            Function,
            typename Array::value_type>) auto transformArray(Array&& input,
                                                             Function
                                                                 function) {
  return std::apply(
      [&function](auto&&... vals) {
        return std::array{std::invoke(function, AD_FWD(vals))...};
      },
      AD_FWD(input));
}

// Same as `std::lower_bound`, but the comparator doesn't compare two values,
// but an iterator (first argument) and a value (second argument). The
// implementation is copied from libstdc++ which has this function as an
// internal detail, but doesn't expose it to the outside.
CPP_template(typename ForwardIterator, typename Tp,
             typename Compare)(requires ql::concepts::forward_iterator<
                               ForwardIterator>) constexpr ForwardIterator
    lower_bound_iterator(ForwardIterator first, ForwardIterator last,
                         const Tp& val, Compare comp) {
  using DistanceType =
      typename std::iterator_traits<ForwardIterator>::difference_type;

  DistanceType len = std::distance(first, last);

  while (len > 0) {
    DistanceType half = len >> 1;
    ForwardIterator middle = first;
    std::advance(middle, half);
    if (comp(middle, val)) {
      first = middle;
      ++first;
      len = len - half - 1;
    } else
      len = half;
  }
  return first;
}

// Same as `std::upper_bound`, but the comparator doesn't compare two values,
// but a value (first argument) and an iterator (second argument). The
// implementation is copied from libstdc++ which has this function as an
// internal detail, but doesn't expose it to the outside.
CPP_template(typename ForwardIterator, typename Tp,
             typename Compare)(requires ql::concepts::forward_iterator<
                               ForwardIterator>) constexpr ForwardIterator
    upper_bound_iterator(ForwardIterator first, ForwardIterator last,
                         const Tp& val, Compare comp) {
  using DistanceType =
      typename std::iterator_traits<ForwardIterator>::difference_type;

  DistanceType len = std::distance(first, last);

  while (len > 0) {
    DistanceType half = len >> 1;
    ForwardIterator middle = first;
    std::advance(middle, half);
    if (comp(val, middle))
      len = half;
    else {
      first = middle;
      ++first;
      len = len - half - 1;
    }
  }
  return first;
}

}  // namespace ad_utility

#endif  // QLEVER_ALGORITHM_H
