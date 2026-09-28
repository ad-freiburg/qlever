// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_COMPRESSEDIDTABLECHUNKQUEUE_H
#define QLEVER_SRC_ENGINE_IDTABLE_COMPRESSEDIDTABLECHUNKQUEUE_H

// The queue of a single chunk of a `CompressedIdTableBlockStorage`, which is
// where all the actual work of that storage happens (see
// `CompressedIdTableBlockStorage.h`). Both are coroutine-based, so this whole
// header is empty when `QLEVER_REDUCED_FEATURE_SET_FOR_CPP17` is set, see
// `util/parallelBlockMerge/BlockStorage.h`.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/experimental/channel_error.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/error_code.hpp>
#include <cstddef>
#include <deque>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

#include "engine/idTable/CompressedIdTableBlocks.h"
#include "engine/idTable/IdTable.h"
#include "util/AsioHelpers.h"
#include "util/CompressedBlockFile.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Forward.h"
#include "util/NoCopyNoMove.h"
#include "util/parallelBlockMerge/BlockStorage.h"

namespace ad_utility::compressedIdTable {

// The FIFO queue of the blocks of a single chunk of a
// `CompressedIdTableBlockStorage`, which keeps only a bounded number of them in
// memory and spills the rest to a file of its own, compressed.
//
// The blocks keep their order no matter whether they were spilled, because a
// single FIFO holds the blocks that are still in memory *and* the metadata of
// the spilled ones, see `Entry`. That FIFO is a `net::experimental::channel`,
// which also provides the rendezvous between the producer and the consumer. It
// is deliberately *unbounded*, because a spilled entry is only a handful of
// bytes of metadata; the blocks in memory are counted separately in
// `numBlocksInMemory_`.
//
// READ-AHEAD: The consumer of a merge reads the blocks of a chunk strictly one
// after the other, so without a read-ahead every spilled block would be
// decompressed only once the consumer asks for it, and all those
// decompressions would happen one at a time. For a merge phase that spills most
// of its output that single-threaded decompression is the hard ceiling on the
// throughput of the whole merge, no matter how many threads produce the blocks.
// Whenever the consumer asks for a block, this queue therefore also moves the
// entries that are already in the channel into `readAhead_` and starts the
// reads of the next `maxReadAheadBlocks_` spilled ones among them, which run
// concurrently on the `ioExecutor_`. Such a block occupies its place in the
// FIFO, so the order is unaffected, and a consumer that reaches that place
// either finds the block already there or waits for exactly that one read.
//
// FINALIZATION: A block that is read back from the file arrives in the layout
// that the consumer of the merge wants, because `BlockCodec::read` produces it.
// A block that is never spilled does not go through that codec at all, and for
// a block type whose layout inside the merge differs from the one the consumer
// wants (as the row-major block of the merge phase does, see
// `RowMajorMergeBlock.h`) it therefore has to be converted separately. This
// queue does that for every such block right when it is stored, on the
// `ioExecutor_` and hence on the same worker threads that also decompress the
// spilled blocks, so that the consumer never converts anything itself. A block
// type that needs no such conversion (the column-major `IdTableStatic`, in
// particular) is stored as it is and never leaves the strand, see
// `BlockCodec::needsFinalization`.
//
// NOTE: A finalization allocates the converted block while the original one is
// still alive, so a chunk transiently needs the memory of one extra block while
// it stores one.
//
// THREAD SAFETY: All the state of this queue is confined to a strand of its
// own, onto which its operations schedule themselves, so they may be initiated
// from anywhere. Nothing ever blocks that strand, as the compression, the
// decompression and the I/O all run on the `ioExecutor`.
//
// LIFETIME: This queue and its spill file have to outlive every operation of
// them that is in flight. The operations of this class itself use a raw `this`
// and rely on the `CompressedIdTableBlockStorage`, whose operations hold a
// `shared_ptr` to the queue for their whole duration. The read-ahead is the
// exception: it has no operation of the storage behind it and therefore holds a
// `shared_ptr` of its own. The spill file is shared with the I/O that runs on
// the `ioExecutor` and may outlive the queue.
template <size_t NumCols = 0, typename BlockType = void>
class ChunkQueue
    : public NoCopyNoMove,
      public std::enable_shared_from_this<ChunkQueue<NumCols, BlockType>> {
 public:
  // NOTE: The default of the `BlockType` is spelled `void` (and not
  // `IdTableStatic<NumCols>`) on purpose: a default that depends on `NumCols`
  // would make `NumCols` undeducible for every function template that takes
  // such a queue as an argument, because `IdTableStatic` is parameterized by an
  // `int` and not by a `size_t`.
  using Block = std::conditional_t<std::is_void_v<BlockType>,
                                   IdTableStatic<NumCols>, BlockType>;
  using Codec = BlockCodec<Block>;
  using OptionalBlock = parallelBlockMerge::OptionalBlock<Block>;
  using GetResult = parallelBlockMerge::GetResult<Block>;
  using Strand = parallelBlockMerge::Strand;

 private:
  // The channel on which a block whose preparation is in flight signals that it
  // is done, see `PendingBlock`.
  using DoneChannel =
      net::experimental::channel<void(boost::system::error_code)>;

  // A block whose preparation on the `ioExecutor_` is in flight and that has
  // not been consumed yet. There are two kinds of those: a spilled block whose
  // read back from the file the read-ahead has started (see the READ-AHEAD note
  // above), and a block that stays in memory and is currently being brought
  // into the layout of the consumer (see the FINALIZATION note above). It is
  // held by a `shared_ptr`, because the operation refers to it while its place
  // in the FIFO may already have been handed to a consumer.
  struct PendingBlock {
    // Both are only set once `isDone_` is true, and exactly one of them.
    OptionalBlock block_;
    std::exception_ptr exception_;
    bool isDone_ = false;
    // Whether this block counts towards `numBlocksInMemory_` (a finalization)
    // or towards `numPendingReads_` (a read-ahead) of its queue.
    bool isInMemory_;
    // The rendezvous with a consumer that reached this block before its
    // preparation was done. Its capacity is one, so the signal is buffered if
    // there is no such consumer (yet).
    DoneChannel done_;

    PendingBlock(const Strand& strand, bool isInMemory)
        : isInMemory_{isInMemory}, done_{strand, 1} {}
  };
  using SharedPendingBlock = std::shared_ptr<PendingBlock>;

  // A single value in the FIFO: a block that is still in memory (where an empty
  // `OptionalBlock` is the end-of-chunk sentinel), the metadata of a block that
  // was spilled to the file of this queue, or a block whose preparation is
  // still in flight, see `PendingBlock`.
  using Entry = std::variant<OptionalBlock, BlockMetadata, SharedPendingBlock>;

  // The FIFO itself, see the class comment above. It is a plain (and not a
  // `concurrent_channel`), because all of its uses are confined to `strand_`.
  using EntryChannel =
      net::experimental::channel<void(boost::system::error_code, Entry)>;

  // The file that this queue spills to. It is shared, because an operation that
  // runs on the `ioExecutor_` holds on to it while this queue may already be
  // done with it, see the LIFETIME note above.
  using SharedSpillFile = std::shared_ptr<CompressedBlockFile>;

  net::any_io_executor ioExecutor_;
  Strand strand_;
  AllocatorWithLimit<Id> allocator_;
  std::string filename_;
  CompressedBlockFile::CompressionLevel compressionLevel_;
  size_t maxBufferedBlocks_;
  size_t maxReadAheadBlocks_;
  EntryChannel entries_;
  // The entries that were taken out of `entries_` in advance, so that the
  // read-ahead could start the reads of the spilled blocks among them, see the
  // READ-AHEAD note above. The consumer always takes its next entry from here
  // if this is not empty, so the order of the entries is unaffected.
  std::deque<Entry> readAhead_;
  // The number of entries in `entries_` and `readAhead_` that are blocks which
  // are still in memory (which includes the ones that are being finalized). The
  // end-of-chunk sentinel does not count, because it occupies no memory and
  // therefore is never spilled.
  size_t numBlocksInMemory_ = 0;
  // The number of entries in `readAhead_` that the read-ahead has turned into a
  // `SharedPendingBlock`, no matter whether that read is already done. It is
  // decremented when such an entry is handed out, so that it bounds the blocks
  // that the read-ahead holds and not merely the concurrent I/O.
  size_t numPendingReads_ = 0;
  // The file of this queue, created with its first spilled block and null for a
  // queue that has not spilled anything (yet).
  SharedSpillFile spillFile_;
  // True while a consumer is suspended on `entries_`. This is only used to
  // check the PRECONDITION that at most one consumer of a chunk waits at any
  // time (the channel itself would happily queue up several of them, and the
  // FIFO order would then be lost), see `BlockStorageConcept::getBlock`.
  bool hasWaitingConsumer_ = false;
  // Set by `finish`, so that a spill which completes afterwards knows that
  // nobody is left who could care about its block.
  bool wasFinished_ = false;

 public:
  // Construct from the `ioExecutor` on which the compression, the
  // decompression and the I/O are run and from which the strand of this queue
  // is derived, the `allocator` for the blocks that are read back, the name of
  // the file to spill to (which is overwritten if it already exists and deleted
  // again as soon as this queue is done with it), the `compressionLevel` that
  // the spilled blocks are stored with, the number of blocks that are kept in
  // memory before this queue starts spilling, and the number of spilled blocks
  // that are read back concurrently (see the READ-AHEAD note above). The former
  // may be zero, in which case every block is spilled; the latter may be zero
  // as well, in which case a spilled block is only read once the consumer asks
  // for it.
  ChunkQueue(net::any_io_executor ioExecutor, AllocatorWithLimit<Id> allocator,
             std::string filename,
             CompressedBlockFile::CompressionLevel compressionLevel,
             size_t maxBufferedBlocks, size_t maxReadAheadBlocks)
      : ioExecutor_{std::move(ioExecutor)},
        strand_{net::make_strand(ioExecutor_)},
        allocator_{std::move(allocator)},
        filename_{std::move(filename)},
        compressionLevel_{compressionLevel},
        maxBufferedBlocks_{maxBufferedBlocks},
        maxReadAheadBlocks_{maxReadAheadBlocks},
        entries_{strand_, std::numeric_limits<size_t>::max()} {}

  // Append the `block` (or the end-of-chunk sentinel) to this queue, spilling
  // it if this queue already buffers `maxBufferedBlocks` blocks. Complete with
  // whether the block was stored at all, see
  // `BlockStorageConcept::storeBlock`.
  template <typename CompletionToken>
  auto storeBlock(OptionalBlock block, CompletionToken&& completionToken) {
    return net::co_spawn(
        strand_,
        [](ChunkQueue* self, OptionalBlock block) -> net::awaitable<bool> {
          AD_CORRECTNESS_CHECK(self->strand_.running_in_this_thread());
          // The end-of-chunk sentinel is never spilled, because it occupies no
          // memory and the consumer needs it to make progress.
          if (!block.has_value() ||
              self->numBlocksInMemory_ < self->maxBufferedBlocks_) {
            self->enqueueBlockWithoutSpilling(std::move(block));
            co_return true;
          }
          // NOTE: The result is stored in a variable instead of being
          // `co_return`ed directly, because GCC 11 miscompiles a `co_return
          // co_await`.
          bool wasStored = co_await self->spillBlock(std::move(block).value());
          co_return wasStored;
        }(this, std::move(block)),
        AD_FWD(completionToken));
  }

  // Remove the front of this queue, reading it back from the file if it was
  // spilled, and suspend if this queue is currently empty, see
  // `BlockStorageConcept::getBlock`.
  template <typename CompletionToken>
  auto getBlock(CompletionToken&& completionToken) {
    return net::co_spawn(strand_, getBlockImpl(), AD_FWD(completionToken));
  }

  // Wake up the consumer that currently waits for the next entry of this queue,
  // if there is one, and complete it as cancelled, see
  // `BlockStorageConcept::cancelAll`. Callable from anywhere, because the
  // actual cancellation is scheduled onto `strand_`.
  //
  // NOTE: There is nothing to do for the producer: it never waits for the
  // consumer, only for its own I/O, and such a write is not cancelled but runs
  // to completion and is then still reported as stored. That is harmless,
  // because `InOrderBlockSink` combines that result with its own stop flag and
  // therefore tells the producer to stop anyway.
  //
  // NOTE: A consumer that waits for a block whose preparation is in flight is
  // not woken up either; it sees that one last block instead of the end of the
  // range. Both are legal outcomes of a race between the consumer and the
  // abort, and a storage that keeps its blocks in memory is only more eager and
  // not more deterministic here.
  //
  // NOTE: This only cancels the operations that are currently suspended and
  // does not close the channel, so a spill that completes afterwards can still
  // append its metadata.
  void cancelWaitingConsumer() noexcept {
    ad_utility::terminateIfThrows(
        [this] {
          net::post(strand_, [self = this->shared_from_this()] {
            self->entries_.cancel();
          });
        },
        "Cancelling the consumer of a chunk failed.");
  }

  // Complete with the number of blocks of this queue that the read-ahead has
  // claimed and that have not been consumed yet, see the READ-AHEAD note at the
  // class comment above. Only used for testing. Asynchronous, because that
  // number lives on `strand_`.
  template <typename CompletionToken>
  auto asyncNumPendingReadsForTesting(CompletionToken&& completionToken) {
    return ad_utility::runFunctionOnExecutor(
        strand_,
        [self = this->shared_from_this()] { return self->numPendingReads_; },
        AD_FWD(completionToken));
  }

 private:
  // The body of `getBlock`.
  //
  // PRECONDITION: This runs on `strand_`.
  net::awaitable<GetResult> getBlockImpl() {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    AD_CORRECTNESS_CHECK(!hasWaitingConsumer_);
    // This chunk is the one that the consumer is currently reading, so it is
    // the one (and the only one) that reads ahead.
    refillReadAhead();
    if (readAhead_.empty()) {
      hasWaitingConsumer_ = true;
      // NOTE: The result is bound to a single variable which is only then
      // destructured, because GCC 15 and 16 miscompile a structured binding by
      // value in a coroutine.
      auto received =
          co_await entries_.async_receive(net::as_tuple(net::use_awaitable));
      auto& [errorCode, entry] = received;
      hasWaitingConsumer_ = false;
      if (errorCode) {
        // The channel is never closed, so the only way a receive can fail is
        // that it was cancelled, see `cancelWaitingConsumer`.
        AD_CORRECTNESS_CHECK(errorCode ==
                             net::experimental::error::channel_cancelled);
        co_return GetResult::cancelled();
      }
      readAhead_.push_back(std::move(entry));
      // The chunk is not empty anymore, so the following blocks can now be read
      // ahead as well.
      refillReadAhead();
    }
    Entry entry = std::move(readAhead_.front());
    readAhead_.pop_front();
    // NOTE: See the NOTE at `storeBlock` for why this is not `co_return
    // co_await`.
    GetResult result = co_await serveEntry(std::move(entry));
    co_return result;
  }

  // Turn a single `entry` that was just removed from the FIFO into the result
  // of a `getBlock`, reading it back from the file or waiting for its
  // preparation if necessary.
  //
  // PRECONDITION: This runs on `strand_`.
  net::awaitable<GetResult> serveEntry(Entry entry) {
    if (std::holds_alternative<BlockMetadata>(entry)) {
      // The read-ahead did not get to this block, so it is read now and the
      // consumer waits for exactly that read. See the NOTE at `storeBlock` for
      // why this is not `co_return co_await`.
      GetResult result =
          co_await readSpilledBlock(std::get<BlockMetadata>(std::move(entry)));
      co_return result;
    }
    if (std::holds_alternative<SharedPendingBlock>(entry)) {
      // The preparation of this block has already been started (and possibly
      // finished): either the read-ahead reads it back, or it stays in memory
      // and is being finalized, see `PendingBlock`.
      auto pending = std::get<SharedPendingBlock>(std::move(entry));
      if (pending->isInMemory_) {
        AD_CORRECTNESS_CHECK(numBlocksInMemory_ > 0);
        --numBlocksInMemory_;
      } else {
        AD_CORRECTNESS_CHECK(numPendingReads_ > 0);
        --numPendingReads_;
      }
      GetResult result = co_await servePendingBlock(std::move(pending));
      co_return result;
    }
    OptionalBlock block = std::get<OptionalBlock>(std::move(entry));
    if (!block.has_value()) {
      finish();
      co_return GetResult::endOfChunk();
    }
    AD_CORRECTNESS_CHECK(numBlocksInMemory_ > 0);
    --numBlocksInMemory_;
    co_return GetResult::fromBlock(std::move(block).value());
  }

  // Move the entries that are already in `entries_` into `readAhead_` and start
  // the read of every spilled block among them, see the READ-AHEAD note at the
  // class comment above. Best effort: a read that cannot be started is simply
  // not started, and the consumer reads that block itself later on.
  //
  // PRECONDITION: This runs on `strand_`.
  void refillReadAhead() noexcept {
    // The number of entries that are inspected per call. Only the blocks that
    // the consumer will ask for soon are worth reading ahead, and this keeps
    // the cost of this function constant no matter how many blocks this queue
    // currently buffers in memory.
    static constexpr size_t scanLimit = 64;
    size_t numInspected = 0;
    while (numPendingReads_ < maxReadAheadBlocks_ && numInspected < scanLimit) {
      Entry entry;
      bool wasReceived = entries_.try_receive(
          [&entry](boost::system::error_code errorCode, Entry received) {
            AD_CORRECTNESS_CHECK(!errorCode);
            entry = std::move(received);
          });
      if (!wasReceived) {
        return;
      }
      ++numInspected;
      if (std::holds_alternative<BlockMetadata>(entry)) {
        startReadAhead(std::get<BlockMetadata>(entry), entry);
      }
      // NOTE: This cannot throw in practice (the deque only grows by the
      // entries that were just taken out of the channel), and if it did, the
      // entry would be lost and the consumer of this chunk would hang. This
      // whole function is therefore `noexcept`.
      readAhead_.push_back(std::move(entry));
    }
  }

  // Replace the spilled `entry` (whose `metadata` it also gets, because the
  // entry is moved from) by a `PendingBlock` and start the read of that block
  // on the `ioExecutor_`. Does nothing if the read cannot be started, in which
  // case the `entry` is left alone and the consumer reads that block itself.
  //
  // PRECONDITION: This runs on `strand_`, and `entry` holds the `metadata`.
  void startReadAhead(BlockMetadata metadata, Entry& entry) noexcept {
    // NOTE: A spilled entry can only exist if this queue has a file, and that
    // file is passed on as a `shared_ptr`, so finishing this queue concurrently
    // cannot delete it while it is being read.
    AD_CORRECTNESS_CHECK(spillFile_ != nullptr);
    SharedPendingBlock pending;
    try {
      pending = std::make_shared<PendingBlock>(strand_, false);
    } catch (...) {
      // Only an exhausted memory can get us here, and the read-ahead is the
      // first thing that may then be dropped.
      return;
    }
    ad_utility::terminateIfThrows(
        [this, &metadata, &pending] {
          startPreparation(
              pending, [file = spillFile_, metadata = std::move(metadata),
                        allocator = allocator_] {
                // NOTE: This runs on the plain `ioExecutor_` and may therefore
                // overlap with other reads and with a write of the same file,
                // which is safe, because a `CompressedBlockFile` synchronizes
                // its operations internally.
                return OptionalBlock{Codec::read(*file, metadata, allocator)};
              });
        },
        "Starting the read-ahead of a chunk failed.");
    entry = Entry{std::move(pending)};
    ++numPendingReads_;
  }

  // Run the `prepare` function on the `ioExecutor_` and store its result in the
  // `pending` block back on `strand_`, see `finishPendingBlock`.
  //
  // PRECONDITION: This runs on `strand_`.
  template <typename Prepare>
  void startPreparation(SharedPendingBlock pending, Prepare prepare) {
    ad_utility::runFunctionOnExecutor(
        ioExecutor_, std::move(prepare),
        net::bind_executor(
            strand_,
            [self = this->shared_from_this(), pending = std::move(pending)](
                std::exception_ptr exception, OptionalBlock block) mutable {
              self->finishPendingBlock(std::move(pending), std::move(exception),
                                       std::move(block));
            }));
  }

  // Store the result of a preparation that was started before, and wake up the
  // consumer that has meanwhile reached that block, if there is one.
  //
  // PRECONDITION: This runs on `strand_`.
  void finishPendingBlock(SharedPendingBlock pending,
                          std::exception_ptr exception,
                          OptionalBlock block) noexcept {
    ad_utility::terminateIfThrows(
        [&pending, &exception, &block] {
          AD_CORRECTNESS_CHECK(!pending->isDone_);
          pending->exception_ = std::move(exception);
          pending->block_ = std::move(block);
          pending->isDone_ = true;
          // The channel has a capacity of one and is never closed, so this
          // either completes a waiting consumer or buffers the signal for the
          // consumer that will come.
          bool wasSent = pending->done_.try_send(boost::system::error_code{});
          AD_CORRECTNESS_CHECK(wasSent);
        },
        "Finishing a block of a chunk failed.");
  }

  // Turn a `pending` block into the result of a `getBlock`, waiting for its
  // preparation if that is not done yet.
  //
  // PRECONDITION: This runs on `strand_`, and the `pending` block was already
  // removed from the FIFO.
  net::awaitable<GetResult> servePendingBlock(SharedPendingBlock pending) {
    if (!pending->isDone_) {
      // The preparation completes this receive, see `finishPendingBlock`. It is
      // never cancelled, see the NOTE at `cancelWaitingConsumer`.
      co_await pending->done_.async_receive(net::use_awaitable);
      AD_CORRECTNESS_CHECK(pending->isDone_);
    }
    if (pending->exception_ != nullptr) {
      std::rethrow_exception(pending->exception_);
    }
    AD_CORRECTNESS_CHECK(pending->block_.has_value());
    co_return GetResult::fromBlock(std::move(pending->block_).value());
  }

  // The body of `storeBlock` for a value that is kept in memory: append it to
  // the FIFO, and account for it unless it is the end-of-chunk sentinel. A
  // block whose layout the consumer cannot use is finalized first, see the
  // FINALIZATION note at the class comment above.
  //
  // PRECONDITION: This runs on `strand_`.
  void enqueueBlockWithoutSpilling(OptionalBlock block) {
    bool isBlock = block.has_value();
    Entry entry{std::move(block)};
    if constexpr (Codec::needsFinalization) {
      if (isBlock) {
        entry = startFinalization(
            std::get<OptionalBlock>(std::move(entry)).value());
      }
    }
    bool wasSent =
        entries_.try_send(boost::system::error_code{}, std::move(entry));
    // The channel is unbounded and is never closed, so the only way a send can
    // fail is a failed allocation, which throws instead.
    AD_CORRECTNESS_CHECK(wasSent);
    if (isBlock) {
      ++numBlocksInMemory_;
    }
  }

  // Start the finalization of a `block` that stays in memory on the
  // `ioExecutor_` and return the FIFO entry that stands for it, see the
  // FINALIZATION note at the class comment above.
  //
  // PRECONDITION: This runs on `strand_`.
  Entry startFinalization(Block block) {
    auto pending = std::make_shared<PendingBlock>(strand_, true);
    startPreparation(
        pending, [block = std::move(block), allocator = allocator_]() mutable {
          return OptionalBlock{Codec::finalize(std::move(block), allocator)};
        });
    return Entry{std::move(pending)};
  }

  // The body of `storeBlock` for a block that has to be spilled: compress
  // the block and write it to the file of this queue on the `ioExecutor_`, then
  // append its metadata to the FIFO back on the `strand_`. Create the file
  // first if this is the first block that this queue spills.
  net::awaitable<bool> spillBlock(Block block) {
    SharedSpillFile file = getOrCreateSpillFile();
    // NOTE: The function that runs on the `ioExecutor_` is a named variable and
    // not a temporary inside the `co_await` expression, because GCC 11 destroys
    // such a temporary twice (a double free of the columns of the `block`).
    auto writeToFile = [file = std::move(file), block = std::move(block),
                        allocator = allocator_] {
      // NOTE: This runs on the plain `ioExecutor_` and may therefore
      // overlap with a read of the same file (never with another write, as
      // a chunk has a single producer). That is safe, because a
      // `CompressedBlockFile` synchronizes its operations internally. The
      // block becomes readable as soon as `BlockCodec::write` has returned,
      // because that file flushes every append, and its chunk may indeed be
      // consumed while further blocks are still being written.
      return Codec::write(*file, block, allocator);
    };
    BlockMetadata metadata = co_await runFunctionOnExecutor(
        ioExecutor_, std::move(writeToFile), net::use_awaitable);
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    if (wasFinished_) {
      // This chunk was finished while the block was being written, so there is
      // no consumer left that could care about that block.
      co_return false;
    }
    bool wasSent = entries_.try_send(boost::system::error_code{},
                                     Entry{std::move(metadata)});
    // See the corresponding note in `enqueueBlockWithoutSpilling`.
    AD_CORRECTNESS_CHECK(wasSent);
    co_return true;
  }

  // The body of `serveEntry` for an entry that was spilled and that the
  // read-ahead did not claim: read that block back from the file of this queue
  // on the `ioExecutor_`.
  net::awaitable<GetResult> readSpilledBlock(BlockMetadata metadata) {
    // NOTE: A spilled entry can only exist if this queue has a file, and that
    // file is passed on as a `shared_ptr`, so finishing this queue concurrently
    // cannot delete it while it is being read.
    AD_CORRECTNESS_CHECK(spillFile_ != nullptr);
    // NOTE: A named variable and not a temporary, see the NOTE at `spillBlock`.
    auto readFromFile = [file = spillFile_, metadata = std::move(metadata),
                         allocator = allocator_] {
      // NOTE: This runs on the plain `ioExecutor_` and may therefore
      // overlap with other reads and with a write of the same file, which
      // is safe, because a `CompressedBlockFile` synchronizes its
      // operations internally.
      //
      // NOTE: The block is wrapped in an `std::optional`, because
      // `runFunctionOnExecutor` requires a default-constructible result and
      // an `IdTable` is not default-constructible.
      return OptionalBlock{Codec::read(*file, metadata, allocator)};
    };
    OptionalBlock block = co_await runFunctionOnExecutor(
        ioExecutor_, std::move(readFromFile), net::use_awaitable);
    co_return GetResult::fromBlock(std::move(block).value());
  }

  // Return the file that this queue spills to, creating it if this is its first
  // spilled block.
  //
  // PRECONDITION: This runs on `strand_`.
  SharedSpillFile getOrCreateSpillFile() {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    if (spillFile_ == nullptr) {
      spillFile_ =
          std::make_shared<CompressedBlockFile>(filename_, compressionLevel_);
    }
    return spillFile_;
  }

  // Remember that this queue is done and destroy its file. This is called
  // exactly when the end-of-chunk sentinel is handed out, from which point
  // nothing will ever be read from this queue again, so no consumer can be
  // waiting on it either.
  //
  // IMPORTANT: The last reference to the file must not die on `strand_`, which
  // nothing may block: closing and unlinking it is expensive, and doing so on
  // the strand instead of on the `ioExecutor_` cost 14 % of a whole merge in a
  // benchmark.
  //
  // PRECONDITION: This runs on `strand_`.
  void finish() noexcept {
    // NOTE: The whole body is guarded, not only the deletion of the file:
    // failing to schedule that deletion would leak the file, and there is
    // nobody left who could handle that, as this is called from the middle of
    // a coroutine that is about to hand out the end-of-chunk sentinel.
    ad_utility::terminateIfThrows(
        [this] {
          wasFinished_ = true;
          if (spillFile_ == nullptr) {
            return;
          }
          net::post(ioExecutor_, [file = std::move(spillFile_)]() mutable {
            // NOTE: The handler runs long after this function has returned and
            // is therefore guarded separately. A handler must not throw, and
            // the destructor of a `CompressedBlockFile` may (it deletes the
            // file).
            ad_utility::terminateIfThrows([&file] { file.reset(); },
                                          "Deleting the spill file of a chunk "
                                          "failed.");
          });
        },
        "Finishing a chunk of a `CompressedIdTableBlockStorage` failed.");
  }
};

}  // namespace ad_utility::compressedIdTable

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_ENGINE_IDTABLE_COMPRESSEDIDTABLECHUNKQUEUE_H
