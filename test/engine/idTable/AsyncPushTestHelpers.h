// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_ENGINE_IDTABLE_ASYNCPUSHTESTHELPERS_H
#define QLEVER_TEST_ENGINE_IDTABLE_ASYNCPUSHTESTHELPERS_H

#include <boost/asio/use_future.hpp>
#include <future>
#include <memory>
#include <vector>

#include "engine/idTable/IdTable.h"
#include "util/jthread.h"

namespace asyncPushTestHelpers {

// Return a view of the `table` in the form that `asyncPushBlock` (of
// `AsyncIdTablePusher` and `CompressedExternalIdTableSorter`) expects. The
// `table` has to outlive the push.
inline std::shared_ptr<const IdTableView<0>> viewOf(const IdTable& table) {
  return std::make_shared<const IdTableView<0>>(table.asStaticView<0>());
}

// Start an `asyncPushBlock` on the `pusher` (an `AsyncIdTablePusher` or a
// `CompressedExternalIdTableSorter`) for each of the `tables`, and return the
// futures of these pushes once all of them have been started.
//
// Each push is started from a thread of its own. This is *not* needed for the
// pushes to run concurrently: `asyncPushBlock` returns immediately, and the
// copying of the rows runs on the executor of the `pusher` in any case. What
// the threads test is that *starting* a push is thread-safe, i.e. that several
// threads may call `asyncPushBlock` at the same time, which is the documented
// way of using it.
//
// The pushes are deliberately not waited for (via `.get()`) inside the
// threads, but their futures are returned: an exception would then terminate
// the program instead of being propagated to the test, and the tests check for
// such exceptions in the main thread (see
// `AsyncIdTablePusher.exceptionInSink`).
template <typename Pusher>
std::vector<std::future<void>> pushConcurrently(
    Pusher& pusher, const std::vector<IdTable>& tables) {
  std::vector<std::future<void>> futures(tables.size());
  {
    // NOTE: The threads are joined at the end of this scope, so all the
    // `futures` have been assigned before they are returned.
    std::vector<ad_utility::JThread> threads;
    for (size_t i = 0; i < tables.size(); ++i) {
      threads.emplace_back([&pusher, &tables, &futures, i]() {
        futures[i] =
            pusher.asyncPushBlock(viewOf(tables[i]), boost::asio::use_future);
      });
    }
  }
  return futures;
}

}  // namespace asyncPushTestHelpers

#endif  // QLEVER_TEST_ENGINE_IDTABLE_ASYNCPUSHTESTHELPERS_H
