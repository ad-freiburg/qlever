//  Copyright 2026, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_MEMORYHELPERS_H
#define QLEVER_MEMORYHELPERS_H

#include <memory>
#include <type_traits>

namespace ad_utility {

// Helper to create a shared_ptr with automatic type deduction.
// Usage: auto ptr = toSharedPtr(std::move(myObject));
template <typename T>
auto toSharedPtr(T&& element) {
  return std::make_shared<std::remove_cvref_t<T>>(std::forward<T>(element));
}

}  // namespace ad_utility

#endif  // QLEVER_MEMORYHELPERS_H
