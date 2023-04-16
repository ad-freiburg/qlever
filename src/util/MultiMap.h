//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

namespace ad_utility {

template<typename Container>
void removeKeyValuePair(Container& container, typename Container::key_type key, typename Container::mapped_type value) {
  auto iterPair = container.equal_range(key);

  for (auto it = iterPair.first; it != iterPair.second; ++it) {
    if (it->second == value) {
      container.erase(it);
      break;
    }
  }
}

}