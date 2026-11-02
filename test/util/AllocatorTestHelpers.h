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
// Create a backend-agnostic allocator with the specified memory limit. If no
// argument is specified, the allocator is unlimited. Unlimited allocators are
// non-owning copies of a single global allocator, so that copying them costs
// no atomic operation.
inline ad_utility::AllocatorWithLimit<Id> makeAllocator(
    MemorySize memorySize = MemorySize::max()) {
  if (memorySize == MemorySize::max()) {
    // The owner is intentionally never destroyed, because non-owning copies
    // might be stored in objects with static storage duration that are
    // destroyed later.
    static const auto* owner = new ad_utility::AllocatorWithLimit<Id>{
        qlever::makeAllocatorWithLimit<Id>(memorySize)};
    return owner->nonOwningCopy();
  }
  return qlever::makeAllocatorWithLimit<Id>(memorySize);
}
}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_ALLOCATORTESTHELPERS_H
