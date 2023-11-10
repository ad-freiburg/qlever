//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "util/CachingMemoryResource.h"

using ad_utility::CachingMemoryResource;

TEST(CachingMemoryResource, allocateAndDeallocate) {
  CachingMemoryResource resource;
  auto ptr = static_cast<std::pmr::memory_resource*>(&resource);

  auto p11 = ptr->allocate(1, 1);
  auto p12a = ptr->allocate(1, 2);
  auto p12b = ptr->allocate(1, 2);
  auto p168 = ptr->allocate(16, 8);

  // Disallow all further allocations.
  std::pmr::set_default_resource(std::pmr::null_memory_resource());

  // Deallocating and allocating the same amount again reuses the pointers.
  ptr->deallocate(p11, 1, 1);
  ptr->deallocate(p168, 16, 8);
  auto p = ptr->allocate(1, 1);
  EXPECT_EQ(p, p11);
  ptr->deallocate(p, 1, 1);

  p = ptr->allocate(16, 8);
  EXPECT_EQ(p, p168);
  ptr->deallocate(p, 16, 8);
  // Deallocate all the other pointers to avoid memory leaks

  ptr->deallocate(p12a, 1, 2);
  ptr->deallocate(p12b, 1, 2);
}

TEST(CachingMemoryResource, equality) {
  CachingMemoryResource r1;
  CachingMemoryResource r2;

  using M = std::pmr::memory_resource*;

  auto p1 = static_cast<M>(&r1);
  auto p2 = static_cast<M>(&r2);
  auto p3 = std::pmr::get_default_resource();

  EXPECT_EQ(p1, p1);
  EXPECT_EQ(p2, p2);
  EXPECT_EQ(p3, p3);
  EXPECT_NE(p1, p2);
  EXPECT_NE(p2, p3);
  EXPECT_NE(p1, p3);
}
