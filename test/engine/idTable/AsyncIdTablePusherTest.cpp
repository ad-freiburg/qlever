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
#include <optional>
#include <set>
#include <stdexcept>
#include <utility>
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
  // NOTE: The `handlerPool` has to be declared before the `pool`, such that
  // the `pool` is joined before the `handlerPool` is destroyed. A thread of
  // the `pool` posts the completion handler to the `handlerPool`, and Asio
  // signals the waiting thread of the `handlerPool` only after unlocking its
  // mutex. So the handler may already have run (and the `future` below may
  // already be ready) while the thread of the `pool` still accesses the
  // `handlerPool`.
  net::thread_pool handlerPool{1};
  net::thread_pool pool{2};
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

// _____________________________________________________________________________
TEST(AsyncIdTablePusher, sinkThatRecyclesBlocks) {
  auto alloc = ad_utility::testing::makeAllocator();
  net::thread_pool pool{4};
  constexpr size_t blocksize = 64;
  // The sink keeps the block that it was handed last, and hands back the one
  // before (like the sorter, which hands back the block that its background
  // thread is done with). It records the contents of each block and the
  // address of its first column. NOTE: The sink runs on the strand of the
  // pusher, so its state needs no further synchronization.
  IdTable actual{3, alloc};
  std::set<const Id*> buffers;
  std::optional<IdTableStatic<0>> previous;
  size_t numBlocks = 0;
  Pusher pusher{pool.get_executor(), 3, blocksize, alloc,
                [&](IdTableStatic<0> block) -> std::optional<IdTableStatic<0>> {
                  EXPECT_EQ(block.numRows(), blocksize);
                  ++numBlocks;
                  buffers.insert(block.getColumn(0).data());
                  actual.insertAtEnd(block);
                  return std::exchange(previous, std::move(block));
                }};
  auto tables = makeTables(8, 3);
  for (auto& future : pushConcurrently(pusher, tables)) {
    future.get();
  }
  actual.insertAtEnd(pusher.finish());

  IdTable expected{3, alloc};
  for (const auto& table : tables) {
    expected.insertAtEnd(table);
  }
  EXPECT_EQ(sortedRows(actual), sortedRows(expected));
  // The first block is allocated by the constructor, the second one after the
  // sink has handed back nothing, and from then on the two blocks alternate.
  // NOTE: Both blocks stay alive (one in the pusher, one in `previous`), so
  // a fresh allocation can never have the address of one of them.
  ASSERT_GT(numBlocks, 2u);
  EXPECT_EQ(buffers.size(), 2u);
}

// _____________________________________________________________________________
TEST(AsyncIdTablePusher, recycledBlockWithWrongNumberOfColumnsIsIgnored) {
  auto alloc = ad_utility::testing::makeAllocator();
  net::thread_pool pool{4};
  constexpr size_t blocksize = 16;
  IdTable actual{2, alloc};
  size_t numBlocks = 0;
  // The sink hands back a nonempty block with a different number of columns,
  // which the pusher must not use as its next block.
  Pusher pusher{pool.get_executor(), 2, blocksize, alloc,
                [&](IdTableStatic<0> block) -> std::optional<IdTableStatic<0>> {
                  EXPECT_EQ(block.numRows(), blocksize);
                  ++numBlocks;
                  actual.insertAtEnd(block);
                  return createRandomlyFilledIdTable(5, 3).toStatic<0>();
                }};
  auto tables = makeTables(4, 2);
  for (auto& future : pushConcurrently(pusher, tables)) {
    future.get();
  }
  auto remainder = pusher.finish();
  EXPECT_EQ(remainder.numColumns(), 2u);
  actual.insertAtEnd(remainder);

  IdTable expected{2, alloc};
  for (const auto& table : tables) {
    expected.insertAtEnd(table);
  }
  EXPECT_GT(numBlocks, 0u);
  EXPECT_EQ(sortedRows(actual), sortedRows(expected));
}

// _____________________________________________________________________________
TEST(AsyncIdTablePusher, sinkThatReturnsNothing) {
  // A sink that has the signature of `Pusher::Sink`, but never hands back a
  // block, behaves like a sink that returns `void`.
  auto alloc = ad_utility::testing::makeAllocator();
  net::thread_pool pool{2};
  size_t numBlocks = 0;
  Pusher pusher{pool.get_executor(), 2, 10, alloc,
                Pusher::Sink{[&numBlocks](IdTableStatic<0> block)
                                 -> std::optional<IdTableStatic<0>> {
                  EXPECT_EQ(block.numRows(), 10u);
                  ++numBlocks;
                  return std::nullopt;
                }}};
  auto table = createRandomlyFilledIdTable(25, 2);
  pusher.asyncPushBlock(viewOf(table), net::use_future).get();
  EXPECT_EQ(numBlocks, 2u);
  EXPECT_EQ(pusher.finish().numRows(), 5u);
}
