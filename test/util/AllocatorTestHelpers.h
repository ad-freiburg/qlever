//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_TEST_UTIL_ALLOCATORTESTHELPERS_H
#define QLEVER_TEST_UTIL_ALLOCATORTESTHELPERS_H

#include "global/Id.h"
#include "util/Allocator.h"
#include "util/AllocatorWithLimit.h"
#include "util/MemorySize/MemorySize.h"

namespace ad_utility::testing {
// Create an (effectively unlimited) allocator. Routed through the backend-
// agnostic `qlever::Allocator` seam so the same tests build and run under both
// the `limit` and the `pmr` allocator backends.
inline ad_utility::AllocatorWithLimit<Id> makeAllocator(
    MemorySize memorySize = MemorySize::max()) {
  return qlever::makeAllocatorWithLimit<Id>(memorySize);
}
}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_ALLOCATORTESTHELPERS_H
