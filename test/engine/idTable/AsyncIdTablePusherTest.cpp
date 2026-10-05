// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_future.hpp>
#include <future>
#include <stdexcept>
#include <vector>

#include "../../util/AllocatorTestHelpers.h"
#include "../../util/GTestHelpers.h"
#include "../../util/IdTableHelpers.h"
#include "./AsyncPushTestHelpers.h"
#include "engine/idTable/AsyncIdTablePusher.h"

namespace {
namespace net = boost::asio;
using ad_utility::AsyncIdTablePusher;
using Pusher = AsyncIdTablePusher<0>;
using asyncPushTestHelpers::pushConcurrently;
using asyncPushTestHelpers::viewOf;

// Return the rows of the `table` as vectors, sorted lexicographically, so that
// tables can be compared independently of the order of their rows.
std::vector<std::vector<Id>> sortedRows(const IdTable& table) {
  std::vector<std::vector<Id>> rows;
  for (const auto& row : table) {
    rows.emplace_back(row.begin(), row.end());
  }
  ql::ranges::sort(rows);
  return rows;
}

// Push the `tables` concurrently into a pusher with the given `blocksize`, and
// check that all the blocks that it hands over are complete, and that these
// blocks together with the remainder of `finish` consist of exactly the rows of
// the `tables`.
void testPusher(const std::vector<IdTable>& tables, size_t blocksize,
                ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  SCOPED_TRACE(absl::StrCat("blocksize = ", blocksize));
  const size_t numColumns = tables.at(0).numColumns();
  auto alloc = ad_utility::testing::makeAllocator();
  net::thread_pool pool{4};
  // NOTE: The sink runs on the strand of the pusher, so the `blocks` need no
  // further synchronization.
  std::vector<IdTable> blocks;
  Pusher pusher{pool.get_executor(), numColumns, blocksize, alloc,
                [&blocks](IdTableStatic<0> block) {
                  blocks.emplace_back(std::move(block));
                }};

  for (auto& future : pushConcurrently(pusher, tables)) {
    future.get();
  }

  IdTable expected{numColumns, alloc};
  for (const auto& table : tables) {
    expected.insertAtEnd(table);
  }
  IdTable actual{numColumns, alloc};
  for (const auto& block : blocks) {
    EXPECT_EQ(block.numRows(), blocksize);
    actual.insertAtEnd(block);
  }
  auto remainder = pusher.finish();
  EXPECT_LT(remainder.numRows(), blocksize);
  EXPECT_EQ(remainder.numRows(), expected.numRows() % blocksize);
  EXPECT_EQ(pusher.numPendingRows(), 0);
  actual.insertAtEnd(remainder);
  EXPECT_EQ(sortedRows(actual), sortedRows(expected));
}

// Return `numTables` random tables of slightly different sizes.
std::vector<IdTable> makeTables(size_t numTables, size_t numColumns) {
  std::vector<IdTable> tables;
  for (size_t i = 0; i < numTables; ++i) {
    tables.push_back(createRandomlyFilledIdTable(200 + 37 * i, numColumns));
  }
  return tables;
}
}  // namespace

// _____________________________________________________________________________
TEST(AsyncIdTablePusher, concurrentPushes) {
  auto tables = makeTables(8, 3);
  // A blocksize that is much larger than all the tables together, one that is
  // much smaller than a single table, and the degenerate case of a single row
  // per block.
  testPusher(tables, 100'000);
  testPusher(tables, 64);
  testPusher(tables, 1);
  // A blocksize that divides the total number of rows, so that `finish`
  // returns an empty remainder.
  size_t totalRows = 0;
  for (const auto& table : tables) {
    totalRows += table.numRows();
  }
  AD_CORRECTNESS_CHECK(totalRows % 4 == 0);
  testPusher(tables, totalRows / 4);
}

// _____________________________________________________________________________
TEST(AsyncIdTablePusher, emptyTableAndReuse) {
  auto alloc = ad_utility::testing::makeAllocator();
  net::thread_pool pool{2};
  size_t numBlocks = 0;
  Pusher pusher{pool.get_executor(), 2, 10, alloc,
                [&numBlocks](IdTableStatic<0>) { ++numBlocks; }};

  // Pushing an empty table completes immediately and allocates nothing.
  IdTable empty{2, alloc};
  pusher.asyncPushBlock(viewOf(empty), net::use_future).get();
  EXPECT_EQ(pusher.numPendingRows(), 0);
  EXPECT_EQ(pusher.finish().numRows(), 0);

  // After `finish` the pusher can be used again.
  auto table = createRandomlyFilledIdTable(25, 2);
  for (size_t i = 0; i < 2; ++i) {
    pusher.asyncPushBlock(viewOf(table), net::use_future).get();
    EXPECT_EQ(pusher.numPendingRows(), 5);
    auto remainder = pusher.finish();
    EXPECT_EQ(remainder.numRows(), 5);
  }
  EXPECT_EQ(numBlocks, 4);

  // The number of columns has to match.
  IdTable wrongNumColumns{3, alloc};
  EXPECT_ANY_THROW(
      pusher.asyncPushBlock(viewOf(wrongNumColumns), net::use_future));
}

// _____________________________________________________________________________
TEST(AsyncIdTablePusher, noAccessWhileInFlight) {
  auto alloc = ad_utility::testing::makeAllocator();
  net::thread_pool pool{2};
  // The sink blocks until `release` is fulfilled, which keeps the push that
  // fills the block in flight.
  std::promise<void> sinkEntered;
  std::promise<void> release;
  auto releaseFuture = release.get_future().share();
  Pusher pusher{pool.get_executor(), 2, 10, alloc,
                [&sinkEntered, releaseFuture](IdTableStatic<0>) {
                  sinkEntered.set_value();
                  releaseFuture.wait();
                }};
  auto table = createRandomlyFilledIdTable(10, 2);
  auto future = pusher.asyncPushBlock(viewOf(table), net::use_future);
  sinkEntered.get_future().wait();
  AD_EXPECT_THROW_WITH_MESSAGE(pusher.numPendingRows(),
                               ::testing::HasSubstr("still in flight"));
  AD_EXPECT_THROW_WITH_MESSAGE(pusher.finish(),
                               ::testing::HasSubstr("still in flight"));

  // Once the push has completed, both are allowed again.
  release.set_value();
  future.get();
  EXPECT_EQ(pusher.numPendingRows(), 0);
  EXPECT_EQ(pusher.finish().numRows(), 0);
}

// _____________________________________________________________________________
TEST(AsyncIdTablePusher, exceptionInSink) {
  auto alloc = ad_utility::testing::makeAllocator();
  net::thread_pool pool{4};
  Pusher pusher{pool.get_executor(), 2, 10, alloc, [](IdTableStatic<0>) {
                  throw std::runtime_error{"sink failed"};
                }};
  auto tables = makeTables(4, 2);

  // Every push that is still in flight when the sink throws, and every push
  // that is started afterwards, completes with the exception.
  size_t numFailed = 0;
  for (auto& future : pushConcurrently(pusher, tables)) {
    try {
      future.get();
    } catch (const std::runtime_error& e) {
      EXPECT_STREQ(e.what(), "sink failed");
      ++numFailed;
    }
  }
  EXPECT_GE(numFailed, 1);
  EXPECT_THROW(
      pusher.asyncPushBlock(viewOf(tables.at(0)), net::use_future).get(),
      std::runtime_error);

  // `finish` resets the exception, so a push that doesn't fill a block
  // succeeds again.
  pusher.finish();
  auto small = createRandomlyFilledIdTable(5, 2);
  pusher.asyncPushBlock(viewOf(small), net::use_future).get();
  EXPECT_EQ(pusher.finish().numRows(), 5);
}

// _____________________________________________________________________________
TEST(AsyncIdTablePusher, completionHandlerRunsOnItsExecutor) {
  auto alloc = ad_utility::testing::makeAllocator();
  net::thread_pool pool{2};
  net::thread_pool handlerPool{1};
  Pusher pusher{pool.get_executor(), 1, 4, alloc, [](IdTableStatic<0>) {}};
  auto table = createRandomlyFilledIdTable(10, 1);
  auto handlerExecutor = handlerPool.get_executor();
  std::packaged_task<bool(std::exception_ptr)> task{
      [handlerExecutor](std::exception_ptr ex) {
        return ex == nullptr && handlerExecutor.running_in_this_thread();
      }};
  // Asio treats a `std::packaged_task` as a completion token (also when it is
  // wrapped by `bind_executor`), so `asyncPushBlock` returns its future.
  auto future = pusher.asyncPushBlock(
      viewOf(table), net::bind_executor(handlerExecutor, std::move(task)));
  EXPECT_TRUE(future.get());
}
