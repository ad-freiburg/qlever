//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_GLOBALMEMORYMANAGEMENT_H
#define QLEVER_GLOBALMEMORYMANAGEMENT_H

#include "../util/AllocatorWithLimit.h"

inline auto& globalMemoryLimit() {
  using T = ad_utility::Synchronized<ad_utility::detail::AllocationMemoryLeft,
                                     ad_utility::SpinLock>;
  static T limit(std::numeric_limits<size_t>::max());
  return limit;
}

/// Overloads for the global "new" and "delete" operator, which respect the
/// limit set by `globalMemoryLimit`. The declarations are not needed as they
/// are implictly present, but we have them here for documentation reasonst.
// void* operator new (size_t sz);
// void operator delete(void* ptr) noexcept;

#endif  // QLEVER_GLOBALMEMORYMANAGEMENT_H
