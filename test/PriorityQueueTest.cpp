// Copyright 2020, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)

#include <gtest/gtest.h>
#include "../src/util/FlexibleCache.h"
#include <string>

TEST(PqTest, Simple) {
  ad_utility::PQ<int,std::string, std::less<>>  pq{std::less<>()};
  pq.insert(3, "hello");
  auto handle = pq.insert(2, "bye");
  handle = pq.insert(37, " 37");
  pq.updateKey(1, handle);
  handle = pq.pop();
  ASSERT_EQ(1, handle->mScore);
  ASSERT_EQ(" 37", handle->mValue);

  handle = pq.pop();
  ASSERT_EQ(2, handle->mScore);
  ASSERT_EQ("bye", handle->mValue);
  handle = pq.pop();
  ASSERT_EQ(3, handle->mScore);
  ASSERT_EQ("hello", handle->mValue);
  ASSERT_EQ(0, pq.size());
}

TEST(PqTest, MapBased) {
  ad_utility::SortedPQ<int,std::string, std::less<>>  pq{std::less<>()};
  pq.insert(3, "hello");
  auto handle = pq.insert(2, "bye");
  handle = pq.insert(37, " 37");
  pq.updateKey(1, handle);
  handle = pq.pop();
  ASSERT_EQ(1, handle.mScore);
  ASSERT_EQ(" 37", handle.mValue);

  handle = pq.pop();
  ASSERT_EQ(2, handle.mScore);
  ASSERT_EQ("bye", handle.mValue);
  handle = pq.pop();
  ASSERT_EQ(3, handle.mScore);
  ASSERT_EQ("hello", handle.mValue);
  ASSERT_EQ(0, pq.size());
}


