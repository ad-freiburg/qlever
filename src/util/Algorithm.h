// Copyright 2022 - 2026 The QLever Authors, in particular:
//
// 2022-2023 Julian Mundhahs <mundhahj@cs.uni-freiburg.de>, UFR
// 2022-2023 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_ALGORITHM_H
#define QLEVER_ALGORITHM_H

#include <boost/optional.hpp>
#include <numeric>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/shift.h"
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
 * Looks up `key` in a map-like container and returns an optional reference to
 * the mapped value if found, or `boost::none` otherwise.
 *
 * Uses `boost::optional` rather than `std::optional` because the latter does
 * not support reference types.
 *
 * Parameters:
 * map: The map-like container to search in (must have a `find` member that
 * returns an iterator to a key-value pair)
 * key: The key to look up
 * return: boost::optional<const mapped_type&>
 */
template <typename Map, typename Key>
auto findOptional(const Map& map, const Key& key)
    -> boost::optional<const typename Map::mapped_type&> {
  auto it = map.find(key);
  if (it != map.end()) {
    return it->second;
  }
  return {};
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

namespace detail {
// Return the next probe distance of a galloping search: double `step`, but
// never beyond `remaining` (the distance from the new lower end to `last`).
// Capping keeps the doubling free of signed overflow for every range size; a
// probe distance larger than `remaining` would be clamped to `remaining` by
// the caller anyway, so the probed positions are the same as with plain
// doubling.
template <typename DistanceType>
constexpr DistanceType nextGallopStep(DistanceType step,
                                      DistanceType remaining) {
  return step > remaining / 2 ? remaining : step * 2;
}
}  // namespace detail

// Galloping `lower_bound_iterator` starting from `hint`: exponential probe
// forward while the probed element is still less than `val`, then binary
// search in the bracket. Preconditions: `hint` is within `[first, last]` and
// at or before the answer, and the range is sorted by `comp` (same comparator
// contract as `lower_bound_iterator`, which compares an iterator as the first
// argument to a value). Both hold when resolving a batch in sorted order
// carrying the previous result as the hint.
CPP_template(typename RandomIt, typename Tp, typename Compare)(
    requires ql::concepts::random_access_iterator<RandomIt> CPP_and
        ql::concepts::invocable<Compare&, RandomIt,
                                const Tp&>) constexpr RandomIt
    gallop_lower_bound_iterator([[maybe_unused]] RandomIt first, RandomIt last,
                                const Tp& val, Compare comp, RandomIt hint) {
  using DistanceType = typename std::iterator_traits<RandomIt>::difference_type;
  // `first` is only needed for this check, which is compiled out unless
  // expensive checks are enabled (hence `[[maybe_unused]]`).
  AD_EXPENSIVE_CHECK(first <= hint && hint <= last);
  RandomIt lo = hint;
  DistanceType step = 1;
  while (true) {
    const DistanceType remaining = last - lo;
    const DistanceType jump = step < remaining ? step : remaining;
    const RandomIt hi = lo + jump;
    if (hi == last || !comp(hi, val)) {
      return lower_bound_iterator(lo, hi, val, comp);
    }
    lo = hi;
    step = detail::nextGallopStep(step, last - lo);
  }
}

// Galloping `upper_bound_iterator` starting from `hint`: mirror image of
// `gallop_lower_bound_iterator` for the comparator contract of
// `upper_bound_iterator` (a value as the first argument, an iterator as the
// second). Same preconditions: `hint` is within `[first, last]` and at or
// before the answer.
CPP_template(typename RandomIt, typename Tp, typename Compare)(
    requires ql::concepts::random_access_iterator<RandomIt> CPP_and
        ql::concepts::invocable<Compare&, const Tp&,
                                RandomIt>) constexpr RandomIt
    gallop_upper_bound_iterator([[maybe_unused]] RandomIt first, RandomIt last,
                                const Tp& val, Compare comp, RandomIt hint) {
  using DistanceType = typename std::iterator_traits<RandomIt>::difference_type;
  // `first` is only needed for this check, which is compiled out unless
  // expensive checks are enabled (hence `[[maybe_unused]]`).
  AD_EXPENSIVE_CHECK(first <= hint && hint <= last);
  RandomIt lo = hint;
  DistanceType step = 1;
  while (true) {
    const DistanceType remaining = last - lo;
    const DistanceType jump = step < remaining ? step : remaining;
    const RandomIt hi = lo + jump;
    if (hi == last || comp(val, hi)) {
      return upper_bound_iterator(lo, hi, val, comp);
    }
    lo = hi;
    step = detail::nextGallopStep(step, last - lo);
  }
}

// Resolve a batch of `queries` against the sorted range `[first, last)` and
// return one `lower_bound` offset per query, in the original order of
// `queries`. Process a copy of the batch in sorted order (keeping each query's
// original position for scattering the results back) and carry the previous
// hit as the gallop hint for `gallop_lower_bound_iterator`: the sorted order
// guarantees that each hint is at or before the answer of the next query.
// Callers distinguish exact hits from holes by comparing each result against
// the query.
CPP_template(typename RandomIt, typename QueryRange)(
    requires ql::concepts::random_access_iterator<RandomIt> CPP_and
        ql::ranges::sized_range<QueryRange>)
    std::vector<size_t> batch_lower_bound_with_hints(
        RandomIt first, RandomIt last, const QueryRange& queries) {
  // `remove_const_t` because the value type of e.g. `ql::span<const size_t>`
  // is `const size_t`, which must not be copied into the sorted query pairs.
  using QueryType = std::remove_const_t<ql::ranges::range_value_t<QueryRange>>;
  if (ql::ranges::empty(queries)) {
    return {};
  }
  std::vector<std::pair<QueryType, size_t>> sortedQueries;
  sortedQueries.reserve(ql::ranges::size(queries));
  size_t index = 0;
  for (const auto& query : queries) {
    sortedQueries.emplace_back(query, index++);
  }
  ql::ranges::sort(sortedQueries, [](const auto& a, const auto& b) {
    return a.first < b.first;
  });
  std::vector<size_t> result(sortedQueries.size());
  const auto comp = [](RandomIt it, const QueryType& value) {
    return *it < value;
  };
  // The first query starts at `hint == first`, which is always a valid hint.
  RandomIt hint = first;
  for (const auto& [query, originalIndex] : sortedQueries) {
    const RandomIt it =
        gallop_lower_bound_iterator(first, last, query, comp, hint);
    result[originalIndex] = static_cast<size_t>(it - first);
    hint = it;
  }
  return result;
}

// In place version of `ql::ranges::set_difference` which writes the output to
// the beginning of `r1`. `std::set_difference` is undefined for this case where
// the output overlaps with one of the input ranges. Additionally, this allows
// some further optimizations.
CPP_template_2(typename R1, typename R2, typename Compare = std::less<>,
               typename Proj1 = ql::identity, typename Proj2 = ql::identity)(
    requires ql::ranges::range<R1> CPP_and_2 ql::ranges::range<R2> CPP_and_2
        ql::concepts::mergeable<
            ql::ranges::iterator_t<R1>, ql::ranges::iterator_t<R2>,
            ql::ranges::iterator_t<R1>, Compare, Proj1,
            Proj2>) auto inplace_set_difference(R1&& r1, R2&& r2,
                                                Compare comp = {},
                                                Proj1 proj1 = {},
                                                Proj2 proj2 = {}) {
  AD_EXPENSIVE_CHECK(ql::ranges::is_sorted(r1, comp, proj1));
  AD_EXPENSIVE_CHECK(ql::ranges::is_sorted(r2, comp, proj2));
  auto it1 = ql::ranges::begin(r1);
  auto end1 = ql::ranges::end(r1);
  auto it2 = ql::ranges::begin(r2);
  auto end2 = ql::ranges::end(r2);
  auto output = ql::ranges::begin(r1);

  while (it1 != end1) {
    if (it2 == end2) {
      // All remaining `r1` elements belong in the output. If output == it1 the
      // elements are already in place. Otherwise, shift them left by the gap.
      if (output == it1) {
        return end1;
      }
      return ql::shift_left(output, end1, std::distance(output, it1));
    }
    if (std::invoke(comp, std::invoke(proj1, *it1), std::invoke(proj2, *it2))) {
      // No need to copy if the element is already at the target destination.
      if (output != it1) {
        *output = std::move(*it1);
      }
      ++output;
      ++it1;  // *it1 < *it2 → keep
    } else if (std::invoke(comp, std::invoke(proj2, *it2),
                           std::invoke(proj1, *it1))) {
      ++it2;  // *it2 < *it1 → skip r2 element
    } else {
      ++it1;
      ++it2;  // equal → discard from r1
    }
  }
  return output;
}

}  // namespace ad_utility

#endif  // QLEVER_ALGORITHM_H
