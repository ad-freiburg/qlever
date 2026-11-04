// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <vector>

#include "util/CountingMemoryResource.h"

using ad_utility::CountingMemoryResource;

// _____________________________________________________________________________
TEST(CountingMemoryResource, countsAllocationsAndDeallocations) {
  CountingMemoryResource resource;
  EXPECT_EQ(resource.numBytesAllocated(), 0u);
  void* a = resource.allocate(100, 8);
  EXPECT_EQ(resource.numBytesAllocated(), 100u);
  void* b = resource.allocate(28, 4);
  EXPECT_EQ(resource.numBytesAllocated(), 128u);
  resource.deallocate(a, 100, 8);
  EXPECT_EQ(resource.numBytesAllocated(), 28u);
  resource.deallocate(b, 28, 4);
  EXPECT_EQ(resource.numBytesAllocated(), 0u);
}

// _____________________________________________________________________________
TEST(CountingMemoryResource, worksWithPolymorphicAllocator) {
  CountingMemoryResource resource;
  {
    std::vector<int, ql::pmr::polymorphic_allocator<int>> vec{
        ql::pmr::polymorphic_allocator<int>{&resource}};
    vec.reserve(1000);
    EXPECT_EQ(resource.numBytesAllocated(), 1000 * sizeof(int));
    vec.push_back(42);
    EXPECT_EQ(resource.numBytesAllocated(), 1000 * sizeof(int));
  }
  EXPECT_EQ(resource.numBytesAllocated(), 0u);
}

// _____________________________________________________________________________
TEST(CountingMemoryResource, upstreamAndEquality) {
  CountingMemoryResource upstream;
  CountingMemoryResource resource{&upstream};
  void* ptr = resource.allocate(64, 16);
  EXPECT_EQ(resource.numBytesAllocated(), 64u);
  EXPECT_EQ(upstream.numBytesAllocated(), 64u);
  resource.deallocate(ptr, 64, 16);
  EXPECT_EQ(upstream.numBytesAllocated(), 0u);

  EXPECT_TRUE(resource.is_equal(resource));
  EXPECT_FALSE(resource.is_equal(upstream));
}
