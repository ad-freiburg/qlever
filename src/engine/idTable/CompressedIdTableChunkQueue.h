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
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/error_code.hpp>
#include <cstddef>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <variant>

#include "engine/idTable/CompressedIdTableBlocks.h"
#include "engine/idTable/IdTable.h"
#include "util/AsioHelpers.h"
#include "util/CompressedBlockFile.h"
#include "util/Exception.h"
#include "util/NoCopyNoMove.h"
#include "util/parallelBlockMerge/BlockStorage.h"

namespace ad_utility::compressedIdTable {

// The FIFO queue of the blocks of a single chunk of a
// `CompressedIdTableBlockStorage`, which keeps only a bounded number of them in
// memory and spills the rest to a file of its own, compressed. Almost all of
// the logic of that storage lives here, because a chunk is the unit that
// everything is bounded and accounted per: the blocks that stay in memory, the
// file that is spilled to, and the single producer and single consumer that
// meet in this queue.
//
// The blocks of a chunk keep their order no matter whether they were spilled or
// not, because a single FIFO holds the blocks that are still in memory *and*
// the metadata of the ones that were spilled, see `Entry`. That FIFO is an
// unbounded `net::experimental::channel`, which also provides the rendezvous
// between the two parties: a consumer that finds the queue empty suspends on
// the channel until the producer sends the next entry, and
// `cancelWaitingConsumer` wakes it up again. The channel is deliberately
// *unbounded*, because the number of blocks that this queue keeps in memory is
// not the number of entries that the FIFO holds: a spilled entry is a handful
// of bytes of metadata and hence not worth applying back-pressure for, so
// `maxBufferedBlocks` is accounted for explicitly in `numBlocksInMemory_`.
//
// THREAD SAFETY: All the member functions of this class have to run on the
// `strand` that it was constructed with, and none of them ever blocks that
// strand. The compression, the decompression and the I/O all run on the
// `ioExecutor`, from which the awaiting coroutine resumes back on the strand,
// see `runFunctionOnExecutor`.
//
// LIFETIME: This queue has to outlive every operation of it that is in flight,
// because such an operation refers to it by plain pointer while its blocking
// part runs on the `ioExecutor`. Its owner guarantees this by holding it in a
// `shared_ptr` of which every operation keeps a copy, see
// `CompressedIdTableBlockStorage`. The file is the exception: it is shared, so
// that `close`ing this queue cannot delete a file out from under an operation
// that is still writing to or reading from it.
template <size_t NumCols = 0>
class ChunkQueue : public NoCopyNoMove {
 public:
  using Block = IdTableStatic<NumCols>;
  using OptionalBlock = parallelBlockMerge::OptionalBlock<Block>;
  using GetResult = parallelBlockMerge::GetResult<Block>;
  using Strand = parallelBlockMerge::Strand;

  // The file that this queue spills to. It is shared, because an operation that
  // runs on the `ioExecutor_` holds on to it while this queue may already have
  // been closed, see the LIFETIME note above. The file is deleted as soon as
  // the last of those references is gone.
  using SharedSpillFile = std::shared_ptr<CompressedBlockFile>;

 private:
  // A single value in the FIFO: either a block that is still in memory (where
  // an empty `OptionalBlock` is the end-of-chunk sentinel), or the metadata of
  // a block that was spilled to the file of this queue.
  using Entry = std::variant<OptionalBlock, BlockMetadata>;

  // The FIFO itself, see the class comment above. It is a plain (and not a
  // `concurrent_channel`), because all of its uses are confined to `strand_`.
  using EntryChannel =
      net::experimental::channel<void(boost::system::error_code, Entry)>;

  Strand strand_;
  net::any_io_executor ioExecutor_;
  AllocatorWithLimit<Id> allocator_;
  std::string filename_;
  CompressedBlockFile::Compression compression_;
  size_t maxBufferedBlocks_;
  EntryChannel entries_;
  // The number of entries in `entries_` that are blocks which are still in
  // memory. The end-of-chunk sentinel does not count, because it occupies no
  // memory and therefore is never spilled.
  size_t numBlocksInMemory_ = 0;
  // The file of this queue, created with its first spilled block and null for a
  // queue that has not spilled anything (yet).
  SharedSpillFile spillFile_;
  // True while a consumer is suspended on `entries_`. This is only used to
  // check the PRECONDITION that at most one consumer of a chunk waits at any
  // time (the channel itself would happily queue up several of them, and the
  // FIFO order would then be lost), see `BlockStorageConcept::getBlock`.
  bool hasWaitingConsumer_ = false;
  // Set by `close`, so that a spill which completes afterwards knows that
  // nobody is left who could care about its block.
  bool wasClosed_ = false;

 public:
  // Construct from the `strand` that all the operations of this queue are
  // confined to, the `ioExecutor` on which the compression, the decompression
  // and the I/O are run, the `allocator` for the blocks that are read back, the
  // name of the file to spill to (which is overwritten if it already exists and
  // deleted again as soon as this queue is done with it, see `close`), the
  // `compression` that the spilled blocks are stored with, and the number of
  // blocks that are kept in memory before this queue starts spilling. That
  // number may be zero, in which case every block is spilled.
  ChunkQueue(Strand strand, net::any_io_executor ioExecutor,
             AllocatorWithLimit<Id> allocator, std::string filename,
             CompressedBlockFile::Compression compression,
             size_t maxBufferedBlocks)
      : strand_{std::move(strand)},
        ioExecutor_{std::move(ioExecutor)},
        allocator_{std::move(allocator)},
        filename_{std::move(filename)},
        compression_{compression},
        maxBufferedBlocks_{maxBufferedBlocks},
        entries_{strand_, std::numeric_limits<size_t>::max()} {}

  // Append the `block` (or the end-of-chunk sentinel) to this queue, spilling
  // it if this queue already buffers `maxBufferedBlocks` blocks. Return whether
  // the block was stored at all, see `BlockStorageConcept::storeBlock`.
  net::awaitable<bool> storeBlock(OptionalBlock block) {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    // The end-of-chunk sentinel is never spilled, because it occupies no memory
    // and the consumer needs it to make progress.
    if (!block.has_value() || numBlocksInMemory_ < maxBufferedBlocks_) {
      enqueueBlockWithoutSpilling(std::move(block));
      co_return true;
    }
    co_return co_await spillBlock(std::move(block).value());
  }

  // Remove the front of this queue, reading it back from the file if it was
  // spilled, and suspend if this queue is currently empty, see
  // `BlockStorageConcept::getBlock`.
  net::awaitable<GetResult> getBlock() {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    AD_CORRECTNESS_CHECK(!hasWaitingConsumer_);
    hasWaitingConsumer_ = true;
    auto [errorCode, entry] =
        co_await entries_.async_receive(net::as_tuple(net::use_awaitable));
    hasWaitingConsumer_ = false;
    if (errorCode) {
      // The channel is never closed, so the only way a receive can fail is that
      // it was cancelled, see `cancelWaitingConsumer`.
      //
      // IMPORTANT: The `entry` has to be ignored on this path, because a
      // default-constructed one looks exactly like the end-of-chunk sentinel.
      co_return GetResult{};
    }
    if (std::holds_alternative<BlockMetadata>(entry)) {
      co_return co_await readSpilledBlock(
          std::get<BlockMetadata>(std::move(entry)));
    }
    OptionalBlock block = std::get<OptionalBlock>(std::move(entry));
    if (!block.has_value()) {
      co_return GetResult::endOfChunk();
    }
    AD_CORRECTNESS_CHECK(numBlocksInMemory_ > 0);
    --numBlocksInMemory_;
    co_return GetResult::fromBlock(std::move(block).value());
  }

  // Wake up the consumer that currently waits for the next entry of this queue,
  // if there is one, and complete it as cancelled, see
  // `BlockStorageConcept::cancelAll`.
  //
  // NOTE: There is nothing to do for the producer: it never waits for the
  // consumer, only for its own I/O, and such a write is not cancelled but runs
  // to completion and is then still reported as stored. That is harmless,
  // because `InOrderBlockSink` combines that result with its own stop flag and
  // therefore tells the producer to stop anyway.
  //
  // NOTE: This only cancels the operations that are currently suspended and
  // does not close the channel, so a spill that completes afterwards can still
  // append its metadata.
  void cancelWaitingConsumer() { entries_.cancel(); }

  // Remember that this queue is done and hand its file (if it has one) to the
  // caller, who is thereby responsible for destroying it.
  //
  // NOTE: Destroying the file closes and unlinks it, which blocks, so this
  // deliberately does not do it here: nothing may block the `strand_`, see
  // `CompressedIdTableBlockStorage::eraseChunk`.
  SharedSpillFile close() noexcept {
    // NOTE: A queue is only closed once its end-of-chunk sentinel was handed
    // out to the consumer, so that consumer cannot be waiting at the same time.
    AD_CORRECTNESS_CHECK(!hasWaitingConsumer_);
    wasClosed_ = true;
    return std::move(spillFile_);
  }

 private:
  // The body of `storeBlock` for a value that is kept in memory: append it to
  // the FIFO, and account for it unless it is the end-of-chunk sentinel.
  void enqueueBlockWithoutSpilling(OptionalBlock block) {
    bool isBlock = block.has_value();
    bool wasSent =
        entries_.try_send(boost::system::error_code{}, Entry{std::move(block)});
    // The channel is unbounded and is never closed, so the only way a send can
    // fail is a failed allocation, which throws instead.
    AD_CORRECTNESS_CHECK(wasSent);
    if (isBlock) {
      ++numBlocksInMemory_;
    }
  }

  // The body of `storeBlock` for a block that has to be spilled: compress the
  // block and write it to the file of this queue on the `ioExecutor_`, then
  // append its metadata to the FIFO back on the `strand_`. Create the file
  // first if this is the first block that this queue spills.
  net::awaitable<bool> spillBlock(Block block) {
    SharedSpillFile file = getOrCreateSpillFile();
    BlockMetadata metadata = co_await runFunctionOnExecutor(
        ioExecutor_,
        [file = std::move(file), block = std::move(block)] {
          BlockMetadata metadata = writeBlock(*file, block, 0, block.numRows());
          // The block has to become readable immediately, because its chunk may
          // be consumed while further blocks are still being written.
          file->flush();
          return metadata;
        },
        net::use_awaitable);
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    if (wasClosed_) {
      // This queue was closed while the block was being written, so there is no
      // consumer left that could care about that block.
      co_return false;
    }
    bool wasSent = entries_.try_send(boost::system::error_code{},
                                     Entry{std::move(metadata)});
    // See the corresponding note in `enqueueBlockWithoutSpilling`.
    AD_CORRECTNESS_CHECK(wasSent);
    co_return true;
  }

  // The body of `getBlock` for an entry that was spilled: read that block back
  // from the file of this queue on the `ioExecutor_`.
  net::awaitable<GetResult> readSpilledBlock(BlockMetadata metadata) {
    // NOTE: A spilled entry can only exist if this queue has a file, and that
    // file is passed on as a `shared_ptr`, so closing this queue concurrently
    // cannot delete it while it is being read.
    AD_CORRECTNESS_CHECK(spillFile_ != nullptr);
    OptionalBlock block = co_await runFunctionOnExecutor(
        ioExecutor_,
        [file = spillFile_, metadata = std::move(metadata),
         allocator = allocator_] {
          // NOTE: The block is wrapped in an `std::optional`, because
          // `runFunctionOnExecutor` requires a default-constructible result and
          // an `IdTable` is not default-constructible.
          return OptionalBlock{readBlock<NumCols>(*file, metadata, allocator)};
        },
        net::use_awaitable);
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
          std::make_shared<CompressedBlockFile>(filename_, compression_);
    }
    return spillFile_;
  }
};

}  // namespace ad_utility::compressedIdTable

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_ENGINE_IDTABLE_COMPRESSEDIDTABLECHUNKQUEUE_H
