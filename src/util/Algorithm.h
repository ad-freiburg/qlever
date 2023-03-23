// Copyright 2022 - 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Julian Mundhahs <mundhahj@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#ifndef QLEVER_ALGORITHM_H
#define QLEVER_ALGORITHM_H

#include <algorithm>
#include <numeric>
#include <string>
#include <string_view>
#include <utility>

#include "util/Forward.h"
#include "util/HashSet.h"
#include "util/Iterators.h"
#include "util/TypeTraits.h"

namespace ad_utility {

/**
 * Checks whether an element is contained in a container.
 *
 * @param container Container& Elements to be searched
 * @param element T Element to search for
 * @return bool
 */
template <typename Container, typename T>
inline bool contains(Container&& container, const T& element) {
  // Overload for types like std::string that have a `find` member function
  if constexpr (ad_utility::isSimilar<Container, std::string> ||
                ad_utility::isSimilar<Container, std::string_view>) {
    return container.find(element) != container.npos;
  } else {
    return std::ranges::find(container.begin(), container.end(), element) !=
           container.end();
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
template <typename T, ad_utility::SimilarTo<std::vector<T>> U>
void appendVector(std::vector<T>& destination, U&& source) {
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
  using Output = std::decay_t<decltype(unaryOp(
      *ad_utility::makeForwardingIterator<Range>(input.begin())))>;
  std::vector<Output> out;
  out.reserve(input.size());
  std::transform(ad_utility::makeForwardingIterator<Range>(input.begin()),
                 ad_utility::makeForwardingIterator<Range>(input.end()),
                 std::back_inserter(out), unaryOp);
  return out;
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
template <typename T>
std::vector<T> removeDuplicates(const std::vector<T>& input) {
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

}  // namespace ad_utility

#endif  // QLEVER_ALGORITHM_H
