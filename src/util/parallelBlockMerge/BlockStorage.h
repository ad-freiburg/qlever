// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_BLOCKSTORAGE_H
#define QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_BLOCKSTORAGE_H

#include <absl/functional/any_invocable.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/strand.hpp>
#include <boost/system/error_code.hpp>
#include <cstddef>
#include <exception>
#include <memory>
#include <optional>
#include <utility>

#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/NoCopyNoMove.h"

// The intermediate storage of the output blocks of the parallel block merge:
// the abstract `BlockStorage` interface, plus the `InMemoryBlockStorage` that
// simply keeps the blocks in RAM. See `InOrderBlockSink.h`, which is the only
// user of this interface.
namespace ad_utility::parallelBlockMerge {

namespace net = boost::asio;

// The place where the `InOrderBlockSink` keeps the output blocks between the
// producer that has finished a block and the consumer that will eventually
// yield it. A storage holds one independent FIFO queue per *run* (the sink
// calls a run a chunk), and a queue transports the blocks of that run plus a
// single end-of-run sentinel (an empty `OptionalBlock`) that terminates it.
//
// The storage is thereby responsible for three things at once: the buffering,
// the FIFO order within a run, and the rendezvous between the producer and the
// consumer of a run. That last point is why storing and retrieving are
// *asynchronous* operations: a consumer that asks for a block that has not been
// produced yet has to be able to wait, and an implementation that bounds its
// buffer (such as `InMemoryBlockStorage`) also makes a producer wait once that
// bound is reached. Which of the two happens is exactly the difference between
// the implementations: `InMemoryBlockStorage` applies back-pressure, whereas an
// implementation that spills to disk lets the producer run ahead.
//
// INTERFACE: The two operations do not take a completion token but a
// type-erased handler, because they are `virtual` and a completion token would
// require a template. The `InOrderBlockSink` wraps them back into ordinary
// Boost.Asio operations, so its own users are unaffected.
//
// CONTRACT: All the member functions of this class
// * run on the single executor that the storage was constructed with (which is
//   the strand of the sink, see `InOrderBlockSink`),
// * invoke their handler exactly once, on that same executor,
// * and may throw only *before* they have consumed their handler, in which case
//   the caller is responsible for completing its own operation (the sink does
//   just that). An implementation therefore has to report a failure of its own
//   bookkeeping through the handler and not by throwing.
//
// PRECONDITIONS: For each run, at most one `storeBlock` and at most one
// `getBlock` may be in flight at any time, because two concurrent operations on
// the same queue could complete in either order and the FIFO order would be
// lost. Both hold for the sink: a run has a single producer that pushes its
// blocks in a handler chain, and the sink has a single consumer. Furthermore no
// operation may be initiated anymore once `cancelAll` was called.
//
// NOTE: `Block` need not be default-constructible (an `IdTable` for example is
// not), which is why the blocks travel as an `std::optional` throughout. That
// `std::optional` doubles as the end-of-run sentinel.
template <typename Block>
class BlockStorage : public NoCopyNoMove {
 public:
  // The value that is stored and retrieved. An empty `OptionalBlock` is the
  // end-of-run sentinel.
  using OptionalBlock = std::optional<Block>;
  // The result of `getBlock`: an empty `GetResult` means that the operation was
  // cancelled (see `cancelAll`), whereas an empty `OptionalBlock` inside it is
  // the end-of-run sentinel.
  using GetResult = std::optional<OptionalBlock>;
  // The handler of `storeBlock`, which is invoked with the exception that made
  // the operation fail (or `nullptr`) and with `true` if the value was actually
  // stored.
  using StoreHandler = absl::AnyInvocable<void(std::exception_ptr, bool) &&>;
  // The handler of `getBlock`, see `GetResult`.
  using GetHandler = absl::AnyInvocable<void(std::exception_ptr, GetResult) &&>;
  // The executor that all the operations of a storage are confined to.
  using Strand = net::strand<net::any_io_executor>;

  virtual ~BlockStorage() = default;

  // Append `block` (or the end-of-run sentinel) to the queue of the run with
  // index `runIndex`, creating that queue if it does not exist yet. Complete
  // with `false` if the value was *not* stored, in which case the caller has to
  // drop it.
  //
  // NOTE: An implementation reports `false` whenever nothing will ever consume
  // that value anymore, and the exact conditions under which that happens are
  // up to it: `InMemoryBlockStorage` reports it when the storage was cancelled
  // while the operation was in flight, whereas `CompressedIdTableBlockStorage`
  // also reports it when the run was erased in the meantime, but *not* for a
  // write that was already in flight when the storage was cancelled.
  virtual void storeBlock(size_t runIndex, OptionalBlock block,
                          StoreHandler handler) = 0;

  // Remove the front of the queue of the run with index `runIndex` and complete
  // with it, waiting until that queue is non-empty. Complete with an empty
  // `GetResult` if the storage was cancelled while the operation was in flight.
  //
  // NOTE: A queue that does not exist yet is created (and is then simply
  // empty), because the consumer of a run may well be faster than its producer.
  virtual void getBlock(size_t runIndex, GetHandler handler) = 0;

  // Drop everything that belongs to the run with index `runIndex`. Call this
  // once the end-of-run sentinel of that run was retrieved, so that the memory
  // (and the disk space) that a storage occupies is proportional to the number
  // of runs that are in flight and not to their total number.
  virtual void eraseRun(size_t runIndex) noexcept = 0;

  // Wake up every operation that is currently suspended, completing it as "not
  // stored" respectively "cancelled", and remember that the storage was
  // cancelled. After this, no further operation may be initiated, see the
  // PRECONDITIONS above.
  //
  // NOTE: An operation whose blocking work has already begun (such as a write
  // to disk) does not have to be cancelled and may run to completion; the point
  // of this function is only that no caller is left suspended forever.
  virtual void cancelAll() noexcept = 0;
};

// The `BlockStorage` that simply keeps the blocks in memory, with a fixed
// number of blocks that are buffered per run. A producer that has finished a
// block while that buffer is full has to wait, so this implementation is the
// back-pressure that bounds the memory consumption of the merge.
//
// Each run owns a single channel of capacity `maxBufferedBlocksPerRun`, which
// is the bounded FIFO queue that the `BlockStorage` interface asks for; the
// end-of-run sentinel travels through the same channel, so storing it can
// suspend as well.
//
// NOTE: A plain (non-concurrent) channel suffices, and the `HashMap` needs no
// synchronization, because every operation runs on `strand_`, see the CONTRACT
// of `BlockStorage`.
template <typename Block>
class InMemoryBlockStorage : public BlockStorage<Block> {
 public:
  using Base = BlockStorage<Block>;
  using OptionalBlock = typename Base::OptionalBlock;
  using GetResult = typename Base::GetResult;
  using StoreHandler = typename Base::StoreHandler;
  using GetHandler = typename Base::GetHandler;
  using Strand = typename Base::Strand;
  // The channel that transports the blocks of a single run from its producer to
  // the consumer.
  using BlockChannel = net::experimental::channel<void(
      boost::system::error_code, OptionalBlock)>;
  // The channels are shared, because both the producer of a run and the
  // consumer may hold on to one across a suspension, while `eraseRun` removes
  // the map entry as soon as that run is done.
  using SharedBlockChannel = std::shared_ptr<BlockChannel>;

 private:
  Strand strand_;
  size_t maxBufferedBlocksPerRun_;
  HashMap<size_t, SharedBlockChannel> runs_;
  // Set by `cancelAll`, only to check the precondition that no operation is
  // initiated afterwards. That check matters, because the teardown of the sink
  // is only airtight as long as no channel is created after the cancellation,
  // see the class comment of `InOrderBlockSink`.
  bool wasCancelled_ = false;

 public:
  // Construct from the `strand` that all the operations of this storage are
  // confined to, and the number of blocks that are buffered per run (which has
  // to be at least one).
  InMemoryBlockStorage(Strand strand, size_t maxBufferedBlocksPerRun)
      : strand_{std::move(strand)},
        maxBufferedBlocksPerRun_{maxBufferedBlocksPerRun} {
    AD_CONTRACT_CHECK(maxBufferedBlocksPerRun > 0);
  }

  // Store `block` in the channel of the run, see `BlockStorage::storeBlock`.
  void storeBlock(size_t runIndex, OptionalBlock block,
                  StoreHandler handler) override {
    SharedBlockChannel channel;
    try {
      channel = getOrCreateChannel(runIndex);
    } catch (...) {
      std::move(handler)(std::current_exception(), false);
      return;
    }
    channel->async_send(boost::system::error_code{}, std::move(block),
                        [handler = std::move(handler)](
                            boost::system::error_code errorCode) mutable {
                          // NOTE: This runs on `strand_`, because the channel
                          // was created with `strand_` as its executor and this
                          // handler has no executor of its own that would
                          // override that. The only error that can occur is
                          // that the channel was cancelled, see `cancelAll`.
                          std::move(handler)(std::exception_ptr{}, !errorCode);
                        });
  }

  // Receive the next value from the channel of the run, see
  // `BlockStorage::getBlock`.
  void getBlock(size_t runIndex, GetHandler handler) override {
    SharedBlockChannel channel;
    try {
      channel = getOrCreateChannel(runIndex);
    } catch (...) {
      std::move(handler)(std::current_exception(), GetResult{});
      return;
    }
    channel->async_receive([handler = std::move(handler)](
                               boost::system::error_code errorCode,
                               OptionalBlock block) mutable {
      // NOTE: This runs on `strand_`, see `storeBlock` above.
      std::move(handler)(std::exception_ptr{},
                         errorCode ? GetResult{} : GetResult{std::move(block)});
    });
  }

  // Destroy the channel of the run, see `BlockStorage::eraseRun`.
  void eraseRun(size_t runIndex) noexcept override {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    // NOTE: Erasing the entry is safe even if the producer of that run still
    // holds the channel, because the channels are shared.
    runs_.erase(runIndex);
  }

  // Cancel all the channels, see `BlockStorage::cancelAll`.
  void cancelAll() noexcept override {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    wasCancelled_ = true;
    // Wake up everybody who is currently suspended. NOTE: There is no need to
    // run this sweep more than once, because no channel is ever created
    // afterwards, so a later sweep would find nothing new (see the
    // PRECONDITIONS of `BlockStorage` and the class comment of
    // `InOrderBlockSink`).
    //
    // IMPORTANT: `cancel()` is also the *only* primitive that may be used here.
    // `close()` alone does not wake an operation that is already suspended, and
    // a `cancel()` after a `close()` is outright undefined behavior, because
    // `cancel()` tells a suspended send from a suspended receive by the
    // internal send state that `close()` overwrites.
    for (const auto& run : runs_) {
      run.second->cancel();
    }
  }

 private:
  // Return the channel of the run with the given `runIndex`, creating it if it
  // does not exist yet.
  SharedBlockChannel getOrCreateChannel(size_t runIndex) {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    AD_CORRECTNESS_CHECK(!wasCancelled_);
    auto& channel = runs_[runIndex];
    if (channel == nullptr) {
      // NOTE: The channel lives on the strand as well, so that the operations
      // on it are dispatched through the strand no matter which executor the
      // initiating caller reports.
      channel =
          std::make_shared<BlockChannel>(strand_, maxBufferedBlocksPerRun_);
    }
    return channel;
  }
};

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_BLOCKSTORAGE_H
