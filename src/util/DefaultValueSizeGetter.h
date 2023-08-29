// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (August of 2023,
// schlegea@informatik.uni-freiburg.de)
#pragma once

#include <string>

#include "util/Cache.h"
#include "util/MemorySize/MemorySize.h"

namespace ad_utility {
// Multiple default `ValueSizeGetter` for usage in caches. For information, what
// a `ValueSizeGetter` is, please see `Cache.h`.

// `ValueSizeGetter` for caches, which simply calls the `sizeof` operator.
struct SizeOfSizeGetter {
  ad_utility::MemorySize operator()(const auto& obj) const {
    return ad_utility::MemorySize::bytes(sizeof(obj));
  }
};

// `ValueSizeGetter` for instances of `std::basic_string`.
template <ad_utility::isInstantiation<std::basic_string> StringType>
struct StringSizeGetter {
  ad_utility::MemorySize operator()(const StringType& str) const {
    return ad_utility::MemorySize::bytes(
        str.size() * sizeof(typename StringType::value_type));
  }
};

}  // namespace ad_utility
