//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_MEMORY_H
#define QLEVER_MEMORY_H

#include <memory>

#include "../Forward.h"

namespace ad_std {

/// Similar to std::construct_at from C++20, but not constexpr. See
/// https://en.cppreference.com/w/cpp/memory/construct_at
template <class T, class... Args>
T* construct_at(T* p, Args&&... args) {
  return ::new (const_cast<void*>(static_cast<const volatile void*>(p)))
      T(AD_FWD(args)...);
}
}  // namespace ad_std

#endif  // QLEVER_MEMORY_H
