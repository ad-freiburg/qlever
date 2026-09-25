// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <thread>

#include "util/RecyclingPool.h"

using ad_utility::RecyclingPool;
using Pool = RecyclingPool<std::vector<int>>;

namespace {
// Return a factory that creates a vector with the single element `i`.
auto makeVec(int i) {
  return [i]() { return std::vector<int>{i}; };
}
}  // namespace

// _____________________________________________________________________________
TEST(RecyclingPool, takeAndGiveBack) {
  Pool pool;
  EXPECT_EQ(pool.numStoredObjects(), 0);
  auto a = pool.take(makeVec(1));
  auto b = pool.take(makeVec(2));
  EXPECT_THAT(a, ::testing::ElementsAre(1));
  EXPECT_THAT(b, ::testing::ElementsAre(2));
  pool.giveBack(std::move(a));
  EXPECT_EQ(pool.numStoredObjects(), 1);
  // The object that was given back is reused, and not reset.
  EXPECT_THAT(pool.take(makeVec(3)), ::testing::ElementsAre(1));
  EXPECT_EQ(pool.numStoredObjects(), 0);
  // The pool is empty again, so a new object is created.
  EXPECT_THAT(pool.take(makeVec(4)), ::testing::ElementsAre(4));
}

// _____________________________________________________________________________
TEST(RecyclingPool, poolDoesNotGrowBeyondTheNumberOfTakenObjects) {
  Pool pool;
  // Nothing is taken, so a foreign object is simply dropped.
  pool.giveBack({42});
  EXPECT_EQ(pool.numStoredObjects(), 0);

  auto a = pool.take(makeVec(1));
  auto b = pool.take(makeVec(2));
  // Two objects are taken, so two (arbitrary) objects are accepted.
  pool.giveBack({42});
  pool.giveBack({43});
  pool.giveBack(std::move(a));
  pool.giveBack(std::move(b));
  EXPECT_EQ(pool.numStoredObjects(), 2);
  EXPECT_THAT(pool.take(makeVec(3)), ::testing::ElementsAre(43));
}

// _____________________________________________________________________________
TEST(RecyclingPool, makeRecyclingOwner) {
  auto pool = std::make_shared<Pool>();
  std::weak_ptr<Pool> weakPool = pool;
  auto owner = Pool::makeRecyclingOwner(pool, pool->take(makeVec(1)));
  auto copy = owner;
  // The owner keeps the pool alive.
  pool.reset();
  EXPECT_FALSE(weakPool.expired());
  owner.reset();
  EXPECT_EQ(weakPool.lock()->numStoredObjects(), 0);
  copy.reset();
  // The last owner has been destroyed and gave the object back, after which
  // the pool (which was only kept alive by the owner) was destroyed as well.
  EXPECT_TRUE(weakPool.expired());

  pool = std::make_shared<Pool>();
  auto owner2 = Pool::makeRecyclingOwner(pool, pool->take(makeVec(5)));
  owner2.reset();
  EXPECT_EQ(pool->numStoredObjects(), 1);
  EXPECT_THAT(pool->take(makeVec(6)), ::testing::ElementsAre(5));
}

// _____________________________________________________________________________
TEST(RecyclingPool, concurrentUse) {
  Pool pool;
  static constexpr size_t numThreads = 4;
  std::vector<std::thread> threads;
  for (size_t i = 0; i < numThreads; ++i) {
    threads.emplace_back([&pool]() {
      for (int j = 0; j < 1000; ++j) {
        pool.giveBack(pool.take(makeVec(j)));
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_LE(pool.numStoredObjects(), numThreads);
  EXPECT_GE(pool.numStoredObjects(), 1);
}
