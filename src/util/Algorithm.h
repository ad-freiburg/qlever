// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#ifndef QLEVER_ALGORITHM_H
#define QLEVER_ALGORITHM_H

namespace ad_utility {

/**
 * Checks whether an element is contained in a container.
 *
 * @param container Container& Elements to be searched
 * @param element T Element to search for
 * @return bool
 */
template <typename Container, typename T>
inline bool contains(const Container& container, const T& element) {
  return std::find(container.begin(), container.end(), element) !=
         container.end();
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

}  // namespace ad_utility

#endif  // QLEVER_ALGORITHM_H
