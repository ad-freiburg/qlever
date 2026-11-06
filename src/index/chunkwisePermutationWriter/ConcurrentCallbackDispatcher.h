// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_CONCURRENTCALLBACKDISPATCHER_H
#define QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_CONCURRENTCALLBACKDISPATCHER_H

#include <absl/functional/any_invocable.h>

#include <atomic>
#include <condition_variable>
#include <exception>
#include <memory>
#include <mutex>
#include <vector>

#include "index/chunkwisePermutationWriter/Options.h"
#include "util/AsyncResourcePool.h"
#include "util/NoCopyNoMove.h"

namespace chunkwisePermutationWriter {

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

namespace net = boost::asio;

// Invoke the `ConcurrentBlockCallback`s of the chunkwise permutation writer
// for the blocks of the chunks, with a bound on the number of blocks for which
// the callbacks are in flight at the same time (see
// `Options::numCallbackBlocksInFlight_`). The exceptions that the callbacks
// report (or throw) are passed on to the `ReportException` function. Once
// `cancel` has been called, no more callbacks are invoked.
class ConcurrentCallbackDispatcher : public ad_utility::NoCopyNoMove {
 public:
  // The function to which the exceptions of the callbacks are reported. It is
  // called from the thread that invokes the `done` of a callback (or that
  // invokes a throwing callback), so it is called concurrently and has to be
  // thread-safe. It must not block either, because it runs on the threads of
  // the callbacks (typically the threads of the pool). It is called without
  // any lock of the dispatcher, so it may call `cancel`.
  using ReportException = absl::AnyInvocable<void(std::exception_ptr)>;

 private:
  using Semaphore = ad_utility::AsyncResourcePool<void>;
  using Permit = Semaphore::Handle;

  std::vector<ConcurrentBlockCallback> callbacks_;
  ReportException reportException_;
  // Bounds the number of blocks for which the callbacks are in flight.
  Semaphore permits_;
  std::atomic<bool> wasCancelled_{false};

  // The number of blocks for which the callbacks are currently in flight,
  // protected by `mutex_`.
  std::mutex mutex_;
  std::condition_variable conditionVariable_;
  size_t numBlocksInFlight_ = 0;

 public:
  // Create a dispatcher for the `callbacks`, which allows for at most
  // `maxNumBlocksInFlight` blocks (at least one) at the same time. The waits
  // for a free slot are completed on the `executor`. The `reportException`
  // has to be thread-safe and non-blocking, see `ReportException`.
  ConcurrentCallbackDispatcher(net::any_io_executor executor,
                               std::vector<ConcurrentBlockCallback> callbacks,
                               size_t maxNumBlocksInFlight,
                               ReportException reportException);

  // Return `true` iff there are no callbacks at all.
  bool empty() const { return callbacks_.empty(); }

  // Wait until there is a free slot for another block, then invoke all the
  // callbacks for the `block`. Return `false` (without invoking any callback)
  // iff the dispatcher was cancelled before. If there are no callbacks, then
  // return `true` right away. The `keepAlive` is held until the
  // last callback for the `block` is done, and it has to keep this dispatcher
  // alive, because the callbacks may be done at any time, in particular after
  // `waitUntilIdle` has returned to the owner of this dispatcher.
  net::awaitable<bool> asyncDispatch(SharedBlock block,
                                     std::shared_ptr<void> keepAlive);

  // Make all current and future calls to `asyncDispatch` return `false`. The
  // callbacks that are already in flight are not affected.
  void cancel();

  // Block until no callbacks are in flight anymore.
  void waitUntilIdle();

 private:
  // Invoke all the `callbacks_` for the `block`, and give the `permit` back
  // (and release the `keepAlive`) once all of them are done.
  void invokeCallbacks(SharedBlock block, Permit permit,
                       std::shared_ptr<void> keepAlive);
};

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

}  // namespace chunkwisePermutationWriter

#endif  // QLEVER_SRC_INDEX_CHUNKWISEPERMUTATIONWRITER_CONCURRENTCALLBACKDISPATCHER_H
