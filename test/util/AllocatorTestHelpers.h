//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_TEST_UTIL_ALLOCATORTESTHELPERS_H
#define QLEVER_TEST_UTIL_ALLOCATORTESTHELPERS_H

#include "global/Id.h"
#include "util/AllocatorWithLimit.h"
#include "util/MemorySize/MemorySize.h"

namespace qlever::testing {
// Create an unlimited allocator.
inline ad_utility::AllocatorWithLimit<Id> makeAllocator(
    ad_utility::MemorySize memorySize = ad_utility::MemorySize::max()) {
  ad_utility::AllocatorWithLimit<Id> a{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(memorySize)};
  return a;
}
}  // namespace qlever::testing
#endif  // QLEVER_TEST_UTIL_ALLOCATORTESTHELPERS_H
