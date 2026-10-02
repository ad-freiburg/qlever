// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_ASYNCIDTABLEPUSHER_H
#define QLEVER_SRC_ENGINE_IDTABLE_ASYNCIDTABLEPUSHER_H

#include <absl/functional/any_invocable.h>

#include <algorithm>
#include <atomic>
#include <boost/asio/async_result.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <cstddef>
#include <exception>
#include <memory>
#include <utility>
#include <vector>

#include "backports/asio.h"
#include "engine/idTable/IdTable.h"
#include "util/AsyncHandlerUtils.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Forward.h"

namespace ad_utility {

// Collect the rows of `IdTable`s that are pushed concurrently from several
// threads into blocks of a fixed size, and hand each complete block to a
// `Sink`. The purpose of this class is that the copying of the rows (which for
// large tables is by far the most expensive part of a push) happens in
// parallel instead of being serialized.
//
// This works as follows: The current block is resized to a complete block up
// front, so that a push only has to *reserve* the range of rows into which it
// then copies. The reserving runs on a strand; it is a handful of integer
// operations, so the next push can start its copy almost immediately. The
// copies themselves run on the underlying executor, outside of the strand. The
// only point at which the pushes have to be synchronized is when a block is
// full: it can only be handed to the `Sink` once all the copies into it have
// finished. A push that finds the block full in the meantime does not block a
// thread, but is queued on the strand and resumed as soon as the block has been
// handed over.
//
// NOTE: The rows of a single pushed table are not necessarily contiguous in the
// resulting blocks, and the blocks contain the rows of concurrent pushes in an
// arbitrary order. Only use this for inputs that are sorted (or otherwise
// reordered) afterwards anyway.
template <size_t NumStaticCols>
class AsyncIdTablePusher {
 public:
  using Block = IdTableStatic<NumStaticCols>;
  // Receive a complete block. It is called on the strand of this class, so the
  // calls never overlap, but a call delays all the pushes that wait for the
  // next block, so it should be cheap or hand its work off.
  using Sink = absl::AnyInvocable<void(Block)>;
  // The type-erased completion handler of `asyncPushBlock`, see there.
  using Handler = absl::AnyInvocable<void(std::exception_ptr)>;
  // The type in which `asyncPushBlock` takes the table to push. A view can be
  // cheaply created from any `IdTable` via `asStaticView<0>()`. The
  // `shared_ptr` allows a caller to tie the lifetime of the rows to the push
  // operation, by creating it via the aliasing constructor of `std::shared_ptr`
  // from a `shared_ptr` to an object that owns both the rows and the view.
  using TablePtr = std::shared_ptr<const IdTableView<0>>;

 private:
  using Allocator = typename Block::Allocator;

  // The state of a single `asyncPushBlock` that is in flight.
  struct PushOperation {
    // The pushed table. NOTE: The `shared_ptr` only keeps the view alive, the
    // rows that it refers to have to be kept alive by the caller of
    // `asyncPushBlock` (e.g. via an aliasing `shared_ptr`, see `TablePtr`).
    TablePtr table_;
    // The number of rows that have already been copied into a block.
    size_t numPushed_ = 0;
    Handler handler_;
  };
  using OperationPtr = std::shared_ptr<PushOperation>;

  ql::any_io_executor executor_;
  boost::asio::strand<ql::any_io_executor> strand_;
  size_t numColumns_;
  size_t blocksize_;
  Allocator allocator_;
  Sink sink_;

  // The number of `asyncPushBlock`s that have been started, but whose
  // completion handler has not yet been called. It is used to check that no
  // push is in flight when this is required (see `finish`, `numPendingRows`,
  // and the destructor). The members above never change after construction,
  // and `block_` is only written outside of `strand_` into disjoint ranges of
  // rows (see `copyColumnsOnExecutor`). Apart from those, this counter and
  // `pushedSinceFinish_` below are the only members that are accessed outside
  // of `strand_`.
  std::atomic<size_t> numOperationsInFlight_ = 0;
  // `true` iff an `asyncPushBlock` has been started since the construction or
  // the last `finish`. It is the cheap first check of `mayHavePendingRows`,
  // which the sorter calls for every row-wise push.
  std::atomic<bool> pushedSinceFinish_ = false;

  // All the following members are only accessed from within `strand_` (or by
  // `finish`, when no push is in flight), so no further synchronization is
  // needed.

  // The block into which the rows are currently copied.
  Block block_;
  // `true` iff `block_` has been resized to a complete block of `blocksize_`
  // rows up front. In that case the number of rows that have actually been
  // pushed into it is `numRowsReserved_` and not its size. It is `false` before
  // the first push, and after a complete block has been handed to the `sink_`,
  // so that no memory is allocated for a block that nobody pushes into.
  bool blockIsResized_ = false;
  // The number of rows of `block_` that have been handed out to pushes, which
  // is also the row at which the next push may start copying.
  size_t numRowsReserved_ = 0;
  // The number of copy operations into `block_` that are currently running
  // outside of the strand. The block may only be handed to the `sink_` once
  // this has dropped to zero.
  size_t numOutstandingCopies_ = 0;
  // The pushes that found `block_` full while copies into it were still
  // running. They are resumed as soon as the block has been handed over.
  std::vector<OperationPtr> waitingForNextBlock_;
  // The first exception that was thrown, either by the `sink_` or while
  // preparing a block. All pushes that are in flight or started afterwards
  // complete with it.
  std::exception_ptr exception_;

 public:
  // Construct a pusher that runs its work on the `executor` and hands blocks of
  // `blocksize` rows with `numColumns` columns (which are allocated via the
  // `allocator`) to the `sink`. The execution context behind the `executor`
  // has to outlive this object, which holds a strand on the `executor`.
  AsyncIdTablePusher(ql::any_io_executor executor, size_t numColumns,
                     size_t blocksize, Allocator allocator, Sink sink)
      : executor_{std::move(executor)},
        strand_{boost::asio::make_strand(executor_)},
        numColumns_{numColumns},
        blocksize_{blocksize},
        allocator_{std::move(allocator)},
        sink_{std::move(sink)},
        block_{numColumns_, allocator_} {
    AD_CONTRACT_CHECK(blocksize_ > 0);
  }

  // Terminate the program if an `asyncPushBlock` is still in flight, because
  // it would then access this destroyed object.
  ~AsyncIdTablePusher() {
    ad_utility::terminateIfThrows(
        [this]() { checkNoOperationInFlight(); },
        "An `AsyncIdTablePusher` was destroyed while an `asyncPushBlock` was "
        "still in flight.");
  }

  // Asynchronously push all the rows of the `table`. Accept any Asio
  // completion token (e.g. `boost::asio::use_future` or a plain callable). The
  // completion signature is `void(std::exception_ptr)`, where a non-null
  // `exception_ptr` means that the rows could not be pushed. The completion
  // handler is posted to its associated executor, or to the executor of this
  // class if it has none. Concurrent calls are allowed and are the intended
  // way of using this class.
  //
  // IMPORTANT: The `table` is a non-owning view. The `shared_ptr` keeps the
  // view itself alive, but the rows that it refers to must stay alive and
  // unchanged until the operation has completed, because they are only copied
  // while it is in flight. To guarantee this without further synchronization,
  // the `table` can be an aliasing `shared_ptr` that shares the ownership of
  // the rows, e.g. `TablePtr{owner, &owner->view_}`, where `owner` is a
  // `shared_ptr` to a struct that holds both an `IdTable` and a view `view_`
  // of it.
  template <typename CompletionToken>
  auto asyncPushBlock(TablePtr table, CompletionToken&& completionToken) {
    AD_CONTRACT_CHECK(table != nullptr);
    AD_CONTRACT_CHECK(table->numColumns() == numColumns_);
    auto operation = std::make_shared<PushOperation>();
    operation->table_ = std::move(table);
    auto initiate = [this,
                     operation = std::move(operation)](auto handler) mutable {
      operation->handler_ =
          makeHandlerExecutorAware(std::move(handler), executor_);
      // NOTE: Set before the counter is incremented, so that whoever sees the
      // counter also sees the flag (both are read by `mayHavePendingRows`).
      pushedSinceFinish_.store(true, std::memory_order_relaxed);
      numOperationsInFlight_.fetch_add(1);
      boost::asio::dispatch(strand_,
                            [this, operation = std::move(operation)]() mutable {
                              pushNextChunk(std::move(operation));
                            });
    };
    return boost::asio::async_initiate<CompletionToken,
                                       void(std::exception_ptr)>(
        std::move(initiate), completionToken);
  }

  // Return the number of pushed rows that have not yet been handed to the
  // `sink_`, which `finish` below then returns.
  //
  // PRECONDITION: No `asyncPushBlock` is in flight, which is checked.
  size_t numPendingRows() const {
    checkNoOperationInFlight();
    return blockIsResized_ ? numRowsReserved_ : 0;
  }

  // Return `false` if no `asyncPushBlock` has been started since the
  // construction or the last `finish`, in which case there are no pending rows
  // and no push in flight. Otherwise return `true`, which does not mean that
  // there are pending rows. This is a single relaxed load, so it can be called
  // for every row of a row-wise push; the exact check is `numPendingRows`.
  bool mayHavePendingRows() const {
    return pushedSinceFinish_.load(std::memory_order_relaxed);
  }

  // Return the rows that have been pushed but not yet handed to the `sink_`,
  // because they don't form a complete block, and reset this pusher, so that
  // it can be used again. After a push has completed with an exception, the
  // pushed rows are unspecified.
  //
  // PRECONDITION: No `asyncPushBlock` is in flight (which is checked), in
  // particular all of them have completed and the threads that started them
  // have been joined.
  Block finish() {
    checkNoOperationInFlight();
    AD_CORRECTNESS_CHECK(numOutstandingCopies_ == 0 &&
                         waitingForNextBlock_.empty());
    pushedSinceFinish_.store(false, std::memory_order_relaxed);
    exception_ = nullptr;
    Block result{numColumns_, allocator_};
    if (blockIsResized_) {
      block_.resize(numRowsReserved_);
      std::swap(result, block_);
      blockIsResized_ = false;
      numRowsReserved_ = 0;
    }
    return result;
  }

 private:
  // Throw if an `asyncPushBlock` is still in flight.
  void checkNoOperationInFlight() const {
    AD_CONTRACT_CHECK(
        numOperationsInFlight_.load() == 0,
        "An `asyncPushBlock` is still in flight, all of them have to be "
        "completed first.");
  }

  // Reserve the next range of rows of `block_` for the `operation` and copy
  // them outside of the strand, or complete the `operation` if all its rows
  // have been pushed (or an exception occurred). Must be called on the strand.
  //
  // Every call ends in exactly one of the following three ways:
  // 1. The `operation` is completed by `completeIfDone`. This is the only
  //    place where its `handler_` is called, and the call is immediately
  //    followed by a `return`.
  // 2. The `operation` is parked in `waitingForNextBlock_`. It is resumed (via
  //    another call to `pushNextChunk`) by `handOverBlockIfComplete`, which
  //    runs as soon as the last outstanding copy into the full `block_` has
  //    finished.
  // 3. A single copy is posted for the `operation`, which afterwards calls
  //    `pushNextChunk` exactly once (via `onCopyFinished`).
  // As every `operation` is at any time owned by exactly one pending call to
  // `pushNextChunk`, one parked entry, or one posted copy, its `handler_` is
  // called exactly once, also in the presence of an exception: Once
  // `exception_` is set, no `operation` is parked anymore (the check at the
  // beginning comes first), and the already parked ones are resumed by the
  // hand-over after the last outstanding copy, and then complete in 1.
  void pushNextChunk(OperationPtr operation) {
    // Complete the `operation` and return `true` if there is nothing more to
    // do for it.
    auto completeIfDone = [this, &operation]() {
      if (exception_ || operation->numPushed_ == operation->table_->numRows()) {
        // NOTE: The decrement happens before the `handler_` is called (which
        // posts it), so that everyone who observes the completion also
        // observes the decremented counter. The exception is copied before the
        // decrement, so that nothing of `*this` is accessed afterwards.
        auto exception = exception_;
        numOperationsInFlight_.fetch_sub(1);
        std::move(operation->handler_)(std::move(exception));
        return true;
      }
      return false;
    };
    if (completeIfDone()) {
      return;
    }
    resizeBlockIfNecessary();
    if (completeIfDone()) {
      return;
    }
    if (numRowsReserved_ == blocksize_) {
      // A full block whose copies had all finished would already have been
      // handed over by `handOverBlockIfComplete`.
      AD_CORRECTNESS_CHECK(numOutstandingCopies_ > 0);
      waitingForNextBlock_.push_back(std::move(operation));
      return;
    }
    const size_t targetRow = numRowsReserved_;
    const size_t numToPush =
        std::min(blocksize_ - targetRow,
                 operation->table_->numRows() - operation->numPushed_);
    numRowsReserved_ += numToPush;
    ++numOutstandingCopies_;
    // NOTE: This is the expensive part, and it deliberately runs outside of
    // the strand, so that it runs concurrently with the copies of the other
    // pushes.
    boost::asio::post(executor_, [this, operation = std::move(operation),
                                  targetRow, numToPush]() mutable {
      copyColumnsOnExecutor(*operation, targetRow, numToPush);
      boost::asio::dispatch(strand_, [this, operation = std::move(operation),
                                      numToPush]() mutable {
        onCopyFinished(std::move(operation), numToPush);
      });
    });
  }

  // Resize `block_` to a complete block, unless this has already happened.
  // Must be called on the strand.
  void resizeBlockIfNecessary() {
    recordException([this]() {
      if (!blockIsResized_) {
        // `blockIsResized_` is only `false` before the first push, after
        // `finish`, and after a hand-over, so no copy into `block_` can be
        // running, which would be invalidated by the `resize`.
        AD_CORRECTNESS_CHECK(numOutstandingCopies_ == 0);
        block_.resize(blocksize_);
        blockIsResized_ = true;
        numRowsReserved_ = 0;
      }
    });
  }

  // Copy the next `numToPush` rows of the `operation` into the rows of
  // `block_` that start at `targetRow`. This runs outside of the strand.
  void copyColumnsOnExecutor(const PushOperation& operation, size_t targetRow,
                             size_t numToPush) {
    const size_t beginRow = operation.numPushed_;
    for (size_t col = 0; col < numColumns_; ++col) {
      auto source =
          operation.table_->getColumn(col).subspan(beginRow, numToPush);
      // Neither `block_` nor its buffers change while copies are outstanding,
      // so it is safe to access them here.
      auto target = block_.getColumn(col).subspan(targetRow, numToPush);
      // NOTE: Deliberately use `std::copy` and not `ql::ranges::copy`,
      // because only the former is reliably turned into a `std::memmove`,
      // see the detailed note in `IdTable::insertAtEnd`.
      std::copy(source.begin(), source.end(), target.begin());
    }
  }

  // Record that a copy of `numToPush` rows of the `operation` has finished,
  // hand over `block_` if it is now complete, and continue with the next chunk
  // of the `operation`. Must be called on the strand.
  void onCopyFinished(OperationPtr operation, size_t numToPush) {
    AD_CORRECTNESS_CHECK(numOutstandingCopies_ > 0);
    --numOutstandingCopies_;
    operation->numPushed_ += numToPush;
    handOverBlockIfComplete();
    pushNextChunk(std::move(operation));
  }

  // Call `function()`. If it throws, store the exception in `exception_`,
  // unless an earlier exception has already been stored there. Must be called
  // on the strand.
  template <typename Function>
  void recordException(Function function) {
    try {
      function();
    } catch (...) {
      if (!exception_) {
        exception_ = std::current_exception();
      }
    }
  }

  // If `block_` is full and no copies into it are outstanding anymore, hand it
  // to the `sink_` and resume the pushes that have been waiting for the next
  // block. Must be called on the strand.
  void handOverBlockIfComplete() {
    if (!blockIsResized_ || numRowsReserved_ < blocksize_ ||
        numOutstandingCopies_ > 0) {
      return;
    }
    // The early return above guarantees that no copy into `block_` is running
    // anymore, so it may be swapped out.
    Block complete{numColumns_, allocator_};
    std::swap(complete, block_);
    blockIsResized_ = false;
    numRowsReserved_ = 0;
    recordException([this, &complete]() { sink_(std::move(complete)); });
    auto waiting = std::move(waitingForNextBlock_);
    waitingForNextBlock_.clear();
    for (auto& operation : waiting) {
      pushNextChunk(std::move(operation));
    }
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_ENGINE_IDTABLE_ASYNCIDTABLEPUSHER_H
