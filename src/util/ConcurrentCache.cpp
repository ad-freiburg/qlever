// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.

#include "util/ConcurrentCache.h"

namespace ad_utility {
CacheStatus fromString(std::string_view str) {
  static const ad_utility::HashMap<std::string, ad_utility::CacheStatus> m = {
      {"cached_not_pinned", ad_utility::CacheStatus::cachedNotPinned},
      {"cached_pinned", ad_utility::CacheStatus::cachedPinned},
      {"computed", ad_utility::CacheStatus::computed},
      {"not_in_cache_not_computed",
       ad_utility::CacheStatus::notInCacheAndNotComputed}};

  auto it = m.find(str);
  if (it == m.end()) {
    AD_FAIL();
  }
  return it->second;
}
}  // namespace ad_utility
