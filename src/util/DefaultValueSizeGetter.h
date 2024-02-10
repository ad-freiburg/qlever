// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (August of 2023,
// schlegea@informatik.uni-freiburg.de)
#pragma once

#include <string>
#include <type_traits>

#include "util/Cache.h"
#include "util/MemorySize/MemorySize.h"

namespace ad_utility {
// Multiple default `ValueSizeGetter` for usage in caches. For information, what
// a `ValueSizeGetter` is, please see `Cache.h`.

// `ValueSizeGetter` for caches, which simply calls the `sizeof` operator.
struct SizeOfSizeGetter {
  template <typename T>
  /*
  If an object internally uses pointers, than `sizeof` doesn't return the actual
  size of the object.

  However, this is not something, we can easily check for, so instead we use
  `std::is_trivially_copyable_v`, which implies the absence of pointers.
  It does that, because if you have pointers, and write your class correctly,
  than you will have to define custom move/copy constructor and assignment
  operators.
  */
  requires std::is_trivially_copyable_v<T>
  ad_utility::MemorySize operator()(const T& obj) const {
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
