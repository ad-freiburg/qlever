//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "global/Id.h"
#include "util/AllocatorWithLimit.h"
#include "util/MemorySize/MemorySize.h"

namespace ad_utility::testing {
// Create an unlimited allocator.
inline ad_utility::AllocatorWithLimit<Id> makeAllocator(
    MemorySize memorySize = MemorySize::max()) {
  ad_utility::AllocatorWithLimit<Id> a{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(memorySize)};
  return a;
}
}  // namespace ad_utility::testing
