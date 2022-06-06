// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#pragma once

#include <concepts>
#include <vector>

namespace ad_utility {

template <class T>
inline bool contains(std::vector<T> elements, T element) {
  return std::find(elements.begin(), elements.end(), element) != elements.end();
}

template <typename T, typename Predicate>
inline bool contains_if(std::vector<T> elements, Predicate predicate)
  requires std::predicate<Predicate, T>
{
  return std::find_if(elements.begin(), elements.end(), predicate) !=
         elements.end();
}

}  // namespace ad_utility