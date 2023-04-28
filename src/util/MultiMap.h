//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

namespace ad_utility {

template<typename Container>
void removeKeyValuePair(Container& container, const typename Container::key_type& key, const typename Container::mapped_type& value) {
  auto iterPair = container.equal_range(key);

  for (auto it = iterPair.first; it != iterPair.second; ++it) {
    if (it->second == value) {
      container.erase(it);
      break;
    }
  }
}

template<typename Container>
bool containsKeyValuePair(const Container& container, const typename Container::key_type& key, const typename Container::mapped_type& value) {
  auto iterPair = container.equal_range(key);

  for (auto it = iterPair.first; it != iterPair.second; ++it) {
    if (it->second == value) {
      return true;
    }
  }
  return false;
}
}
