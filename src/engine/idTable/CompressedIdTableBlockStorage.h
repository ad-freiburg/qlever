// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_COMPRESSEDIDTABLEBLOCKSTORAGE_H
#define QLEVER_SRC_ENGINE_IDTABLE_COMPRESSEDIDTABLEBLOCKSTORAGE_H

// A model of the `parallelBlockMerge::BlockStorageConcept` that spills the
// output blocks of the merge to disk. Its only user is the `InOrderBlockSink`,
// which is coroutine-based, so this whole header is empty when
// `QLEVER_REDUCED_FEATURE_SET_FOR_CPP17` is set, see
// `util/parallelBlockMerge/BlockStorage.h`.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <absl/strings/str_cat.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include "engine/idTable/CompressedIdTableChunkQueue.h"
#include "engine/idTable/IdTable.h"
#include "util/AsioHelpers.h"
#include "util/CompressedBlockFile.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Forward.h"
#include "util/HashMap.h"
#include "util/NoCopyNoMove.h"
#include "util/parallelBlockMerge/BlockStorage.h"

namespace ad_utility {

namespace net = boost::asio;

// A `parallelBlockMerge::BlockStorageConcept` for blocks of type
// `IdTableStatic`, which keeps only a bounded number of blocks per chunk in
// memory and spills the rest to a temporary file, compressed. A producer
// therefore never waits (except for the duration of the I/O), so a chunk that
// is far ahead of the consumer can be merged to completion; the price is that
// its blocks have to be compressed, written, read back and decompressed again.
//
// This class is deliberately nothing but the owner of one queue per chunk: all
// the actual work happens in the `compressedIdTable::ChunkQueue` (see
// `CompressedIdTableChunkQueue.h`), because a chunk is the unit that everything
// is bounded and accounted per. Each chunk that actually spills owns a file of
// its own, which is created with its first spilled block and deleted again as
// soon as that chunk is done. The disk space that this storage occupies is
// therefore proportional to the chunks that are in flight and not to their
// total number, exactly like the memory that it occupies.
//
// THREAD SAFETY: The two asynchronous operations may be initiated from
// anywhere, because they schedule themselves onto a strand of this storage;
// only the PRECONDITIONS of the `BlockStorageConcept` apply. Nothing ever
// blocks that strand, and the chunks do not serialize with each other, because
// each of them has a strand of its own.
//
// LIFETIME: This storage has to outlive every operation of it that is in
// flight, because such an operation refers to it by plain pointer. The parallel
// merge guarantees this, because the handler of every operation transitively
// holds a `shared_ptr` to the `ParallelMergeState` that owns the sink and
// thereby this storage. The queues and the strand-confined state are the
// exception: they are shared, see `State`.
//
// NOTE: A `getBlock` whose block has to be read back from the file is not
// cancelled by `cancelAll`, so a consumer that aborts the merge while such a
// read is in flight sees that one last block instead of the end of the range.
// Both are legal outcomes of a race between the consumer and the abort.
template <size_t NumCols = 0>
class CompressedIdTableBlockStorage : public NoCopyNoMove {
 public:
  using Block = IdTableStatic<NumCols>;
  using OptionalBlock = parallelBlockMerge::OptionalBlock<Block>;
  using GetResult = parallelBlockMerge::GetResult<Block>;
  using Strand = parallelBlockMerge::Strand;

 private:
  using ChunkQueue = compressedIdTable::ChunkQueue<NumCols>;

  // A queue is held by `shared_ptr`, because every operation of it keeps a copy
  // for its whole duration, so that a chunk which is erased while one of its
  // blocks is still on its way to the file does not leave that operation with a
  // dangling queue, see the LIFETIME note above.
  using SharedChunkQueue = std::shared_ptr<ChunkQueue>;

  // Everything that is confined to `strand_`. It is held by a `shared_ptr`,
  // because `cancelAll` only *schedules* its work onto that strand, so the
  // handler that does it may well run once this storage is gone.
  struct State {
    HashMap<size_t, SharedChunkQueue> chunks_;
    // Set by `cancelAll`, only to check the PRECONDITION that no operation is
    // initiated afterwards, see the `BlockStorageConcept`.
    bool wasCancelled_ = false;
  };

  net::any_io_executor ioExecutor_;
  Strand strand_;
  AllocatorWithLimit<Id> allocator_;
  size_t maxBufferedBlocksPerChunk_;
  std::string filenamePrefix_;
  CompressedBlockFile::Compression compression_;
  std::shared_ptr<State> state_ = std::make_shared<State>();

 public:
  // Construct from the `ioExecutor` on which the compression, the decompression
  // and the I/O are run and from which the strands of this storage and of its
  // chunks are derived, the name of the file to spill to, the `allocator` for
  // the blocks that are read back, and the number of blocks that are kept in
  // memory per chunk before that chunk starts spilling. That number may be
  // zero, in which case every block is spilled. The `compression` decides how
  // the spilled blocks are stored, see `CompressedBlockFile::Compression`; a
  // spill file is short-lived and read back almost immediately, so a low level
  // (or `NO_BLOCK_COMPRESSION`) is often faster than the default.
  //
  // NOTE: The `filenamePrefix` is not a filename but the prefix of one per
  // chunk, see `spillFilename`. It has to be unique among all the storages that
  // exist at the same time, because those files are overwritten if they exist
  // and deleted when the chunk that owns them is done.
  CompressedIdTableBlockStorage(
      net::any_io_executor ioExecutor, std::string filenamePrefix,
      AllocatorWithLimit<Id> allocator, size_t maxBufferedBlocksPerChunk,
      CompressedBlockFile::Compression compression = ZSTD_DEFAULT_LEVEL)
      : ioExecutor_{std::move(ioExecutor)},
        strand_{net::make_strand(ioExecutor_)},
        allocator_{std::move(allocator)},
        maxBufferedBlocksPerChunk_{maxBufferedBlocksPerChunk},
        filenamePrefix_{std::move(filenamePrefix)},
        compression_{compression} {}

  // The common prefix of the names of all the files of this storage.
  const std::string& filenamePrefix() const { return filenamePrefix_; }

  // The name of the file that the chunk with the given `chunkIndex` spills to.
  // That file only exists while the chunk has spilled at least one block and
  // has not been erased yet.
  std::string spillFilename(size_t chunkIndex) const {
    return absl::StrCat(filenamePrefix_, ".", chunkIndex);
  }

  // Append the `block` to the queue of the chunk, spilling it if that chunk
  // already buffers `maxBufferedBlocksPerChunk` blocks, see
  // `BlockStorageConcept::storeBlock`.
  //
  // NOTE: `co_spawn` completes with `void(std::exception_ptr, bool)`, which is
  // exactly the completion signature that the `BlockStorageConcept` requires,
  // so a failure of the bookkeeping below is reported through the completion
  // and not by throwing at the caller, as that CONTRACT demands. The same holds
  // for `getBlock`.
  template <typename CompletionToken>
  auto storeBlock(size_t chunkIndex, OptionalBlock block,
                  CompletionToken&& completionToken) {
    return net::co_spawn(
        strand_,
        [this, chunkIndex, block = std::move(block),
         state = state_]() mutable -> net::awaitable<bool> {
          AD_CORRECTNESS_CHECK(!state->wasCancelled_);
          SharedChunkQueue chunk = getOrCreateChunk(*state, chunkIndex);
          bool wasStored =
              co_await chunk->storeBlock(std::move(block), net::use_awaitable);
          co_return wasStored;
        },
        AD_FWD(completionToken));
  }

  // Remove the front of the queue of the chunk, reading it back from the file
  // if it was spilled, see `BlockStorageConcept::getBlock`.
  template <typename CompletionToken>
  auto getBlock(size_t chunkIndex, CompletionToken&& completionToken) {
    return net::co_spawn(
        strand_,
        [this, chunkIndex, state = state_]() -> net::awaitable<GetResult> {
          AD_CORRECTNESS_CHECK(!state->wasCancelled_);
          // NOTE: The queue of a chunk that does not exist yet is created,
          // because the consumer of a chunk may well be faster than its
          // producer, see `BlockStorageConcept::getBlock`.
          SharedChunkQueue chunk = getOrCreateChunk(*state, chunkIndex);
          GetResult result = co_await chunk->getBlock(net::use_awaitable);
          if (result.isEndOfChunk()) {
            // This chunk is done, so its queue may be dropped, see
            // `BlockStorageConcept::getBlock`. The queue has already deleted
            // its file, see `ChunkQueue::finish`.
            state->chunks_.erase(chunkIndex);
          }
          co_return std::move(result);
        },
        AD_FWD(completionToken));
  }

  // Complete every waiting consumer with a cancelled `GetResult`, see
  // `BlockStorageConcept::cancelAll` and `ChunkQueue::cancelWaitingConsumer`.
  //
  // NOTE: The sweep over the chunks is scheduled onto `strand_` and hence
  // happens after this has returned. That is enough, because the only thing
  // that has to be immediate is that no caller is left suspended forever, and
  // the `InOrderBlockSink` initiates no further operation once it has stopped
  // the merge, see its class comment.
  void cancelAll() noexcept {
    ad_utility::terminateIfThrows(
        [this] {
          net::post(strand_, [state = state_] {
            state->wasCancelled_ = true;
            for (auto& [chunkIndex, queue] : state->chunks_) {
              queue->cancelWaitingConsumer();
            }
          });
        },
        "Cancelling a `CompressedIdTableBlockStorage` failed.");
  }

  // Complete with the number of chunks for which a queue currently exists. Only
  // used to test that a chunk is indeed dropped as soon as its end-of-chunk
  // sentinel was handed out. Asynchronous, because that number lives on
  // `strand_`.
  template <typename CompletionToken>
  auto asyncNumLiveChunksForTesting(CompletionToken&& completionToken) {
    return ad_utility::runFunctionOnExecutor(
        strand_, [state = state_] { return state->chunks_.size(); },
        AD_FWD(completionToken));
  }

 private:
  // Return the queue of the chunk with the given `chunkIndex`, creating it if
  // that chunk has none yet.
  //
  // PRECONDITION: This runs on `strand_`.
  SharedChunkQueue getOrCreateChunk(State& state, size_t chunkIndex) const {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    SharedChunkQueue& chunk = state.chunks_[chunkIndex];
    if (chunk == nullptr) {
      chunk = std::make_shared<ChunkQueue>(
          ioExecutor_, allocator_, spillFilename(chunkIndex), compression_,
          maxBufferedBlocksPerChunk_);
    }
    return chunk;
  }
};

// A factory for a `CompressedIdTableBlockStorage`, for the constructor of
// `InOrderBlockSink`. The arguments are those of the constructor of that class.
template <size_t NumCols>
auto makeCompressedIdTableStorageFactory(
    net::any_io_executor ioExecutor, std::string filenamePrefix,
    AllocatorWithLimit<Id> allocator, size_t maxBufferedBlocksPerChunk,
    CompressedBlockFile::Compression compression = ZSTD_DEFAULT_LEVEL) {
  return
      [ioExecutor = std::move(ioExecutor),
       filenamePrefix = std::move(filenamePrefix),
       allocator = std::move(allocator), maxBufferedBlocksPerChunk,
       compression]([[maybe_unused]] const parallelBlockMerge::Strand& strand) {
        // NOTE: This storage brings a strand of its own, so the one that the
        // sink offers is not needed.
        return CompressedIdTableBlockStorage<NumCols>{
            ioExecutor, filenamePrefix, allocator, maxBufferedBlocksPerChunk,
            compression};
      };
}

}  // namespace ad_utility

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_ENGINE_IDTABLE_COMPRESSEDIDTABLEBLOCKSTORAGE_H
