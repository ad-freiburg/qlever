// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.

#include "util/ConcurrentCache.h"

namespace ad_utility {
CacheStatus fromString(std::string_view str) {
  using enum ad_utility::CacheStatus;
  static const ad_utility::HashMap<std::string, ad_utility::CacheStatus> m = {
      {"cached_not_pinned", cachedNotPinned},
      {"cached_pinned", cachedPinned},
      {"computed", computed},
      {"not_in_cache_not_computed", notInCacheAndNotComputed}};

  auto it = m.find(str);
  if (it == m.end()) {
    AD_FAIL();
  }
  return it->second;
}
}  // namespace ad_utility
