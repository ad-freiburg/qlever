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
#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include "engine/idTable/CompressedIdTableChunkQueue.h"
#include "engine/idTable/IdTable.h"
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
// memory and spills the rest to a temporary file, compressed.
//
// This is the alternative to a storage that keeps all its blocks in memory (see
// `test/parallelBlockMerge/InMemoryBlockStorage.h`), which bounds the memory
// consumption of the merge by making a producer wait once the consumer has
// fallen behind. Here a producer never waits (except for the duration of the
// I/O), so a chunk that is far ahead of the consumer can be merged to
// completion; the price is that its blocks have to be compressed, written, read
// back and decompressed again.
//
// This class is deliberately nothing but the owner of one queue per chunk: all
// the actual work happens in the `compressedIdTable::ChunkQueue` (see
// `CompressedIdTableChunkQueue.h`), because a chunk is the unit that everything
// is bounded and accounted per. What is left here is the mapping from a chunk
// index to its queue, and the two operations of the `BlockStorageConcept`,
// which are ordinary Boost.Asio operations that `co_spawn` the coroutine of the
// respective queue.
//
// Each chunk that actually spills owns a file of its own, which is created with
// its first spilled block and deleted again as soon as that chunk is done (see
// `eraseChunk`). The disk space that this storage occupies is therefore
// proportional to the chunks that are in flight and not to their total number,
// exactly like the memory that it occupies. A chunk that never spills never
// creates a file at all. Per-chunk files are also faster than one shared file,
// because appending to them no longer contends for a single exclusive lock.
//
// NOTE: Once the storage was cancelled nothing is erased anymore (see the class
// comment of `InOrderBlockSink`), so the files of the chunks that were still in
// flight live until the storage itself is destroyed.
//
// THREAD SAFETY: This class implements the CONTRACT of the
// `parallelBlockMerge::BlockStorageConcept`: all of its member functions have
// to run on the `strand` that it was constructed with, and none of them ever
// blocks that strand. The compression, the decompression and the I/O all run on
// the `ioExecutor`, from which the coroutine of the queue resumes back on the
// strand. The `ioExecutor` is typically the very executor that the strand was
// derived from (the thread pool of the merge): work that is posted to that
// executor directly does not go through the strand and therefore does not
// serialize with it.
//
// LIFETIME: This storage has to outlive every operation of it that is in
// flight, because such an operation refers to it by plain pointer. The parallel
// merge guarantees this, because the handler of every operation transitively
// holds a `shared_ptr` to the `ParallelMergeState` that owns the sink and
// thereby this storage. The queues are the exception: they are shared, so that
// erasing a chunk cannot destroy a queue out from under an operation of it that
// is still in flight, see `ChunkQueue`.
//
// NOTE: A `getBlock` whose block has to be read back from the file is not
// cancelled by `cancelAll`, so a consumer that aborts the merge while such a
// read is in flight sees that one last block instead of the end of the range.
// Both are legal outcomes of a race between the consumer and the abort, and a
// storage that keeps its blocks in memory is only more eager and not more
// deterministic here.
template <size_t NumCols = 0>
class CompressedIdTableBlockStorage : public NoCopyNoMove {
 public:
  using Block = IdTableStatic<NumCols>;
  using OptionalBlock = parallelBlockMerge::OptionalBlock<Block>;
  using GetResult = parallelBlockMerge::GetResult<Block>;
  using Strand = parallelBlockMerge::Strand;

 private:
  using ChunkQueue = compressedIdTable::ChunkQueue<NumCols>;
  using SharedSpillFile = typename ChunkQueue::SharedSpillFile;

  // A queue is held by `shared_ptr`, because every operation of it keeps a copy
  // for its whole duration, so that a chunk which is erased while one of its
  // blocks is still on its way to the file does not leave that operation with a
  // dangling queue, see the LIFETIME note above.
  using SharedChunkQueue = std::shared_ptr<ChunkQueue>;

  Strand strand_;
  net::any_io_executor ioExecutor_;
  AllocatorWithLimit<Id> allocator_;
  size_t maxBufferedBlocksPerChunk_;
  std::string filenamePrefix_;
  CompressedBlockFile::Compression compression_;
  HashMap<size_t, SharedChunkQueue> chunks_;
  // Set by `cancelAll`, only to check the precondition that no operation is
  // initiated afterwards, see the PRECONDITIONS of the
  // `BlockStorageConcept`.
  bool wasCancelled_ = false;

 public:
  // Construct from the `strand` that all the operations of this storage are
  // confined to, the `ioExecutor` on which the compression, the decompression
  // and the I/O are run, the name of the file to spill to (which is overwritten
  // if it exists and deleted when this storage is destroyed), the `allocator`
  // for the blocks that are read back, and the number of blocks that are kept
  // in memory per chunk before that chunk starts spilling. That number may be
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
      Strand strand, net::any_io_executor ioExecutor,
      std::string filenamePrefix, AllocatorWithLimit<Id> allocator,
      size_t maxBufferedBlocksPerChunk,
      CompressedBlockFile::Compression compression = ZSTD_DEFAULT_LEVEL)
      : strand_{std::move(strand)},
        ioExecutor_{std::move(ioExecutor)},
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
  template <typename CompletionToken>
  auto storeBlock(size_t chunkIndex, OptionalBlock block,
                  CompletionToken&& completionToken) {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    AD_CORRECTNESS_CHECK(!wasCancelled_);
    return net::co_spawn(strand_, storeBlockImpl(chunkIndex, std::move(block)),
                         AD_FWD(completionToken));
  }

  // Remove the front of the queue of the chunk, reading it back from the file
  // if it was spilled, see `BlockStorageConcept::getBlock`.
  template <typename CompletionToken>
  auto getBlock(size_t chunkIndex, CompletionToken&& completionToken) {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    AD_CORRECTNESS_CHECK(!wasCancelled_);
    return net::co_spawn(strand_, getBlockImpl(chunkIndex),
                         AD_FWD(completionToken));
  }

  // Complete every waiting consumer with a cancelled `GetResult`, see
  // `BlockStorageConcept::cancelAll` and `ChunkQueue::cancelWaitingConsumer`.
  void cancelAll() noexcept {
    ad_utility::terminateIfThrows(
        [this] {
          AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
          wasCancelled_ = true;
          for (auto& chunk : chunks_) {
            // NOTE: Cancelling completes a suspended consumer via `post` and
            // therefore never inline, so no handler of one of these consumers
            // runs while `chunks_` is being iterated here.
            chunk.second->cancelWaitingConsumer();
          }
        },
        "Cancelling a `CompressedIdTableBlockStorage` failed.");
  }

  // The number of chunks for which a queue currently exists. Only used to test
  // that a chunk is indeed dropped as soon as its end-of-chunk sentinel was
  // handed out.
  size_t numLiveChunksForTesting() const noexcept { return chunks_.size(); }

 private:
  // The body of `storeBlock`.
  //
  // NOTE: `co_spawn` completes with `void(std::exception_ptr, bool)`, which is
  // exactly the completion signature that the `BlockStorageConcept` requires,
  // so a failure of the bookkeeping below is reported through the completion
  // and not by throwing at the caller, as that CONTRACT demands.
  net::awaitable<bool> storeBlockImpl(size_t chunkIndex, OptionalBlock block) {
    SharedChunkQueue chunk = getOrCreateChunk(chunkIndex);
    co_return co_await chunk->storeBlock(std::move(block));
  }

  // The body of `getBlock`, see the NOTE at `storeBlockImpl` for the
  // exceptions.
  net::awaitable<GetResult> getBlockImpl(size_t chunkIndex) {
    // NOTE: The queue of a chunk that does not exist yet is created, because
    // the consumer of a chunk may well be faster than its producer, see
    // `BlockStorageConcept::getBlock`.
    SharedChunkQueue chunk = getOrCreateChunk(chunkIndex);
    GetResult result = co_await chunk->getBlock();
    if (result.isEndOfChunk()) {
      // This chunk is done, so everything that belongs to it may be dropped,
      // see `BlockStorageConcept::getBlock`.
      eraseChunk(chunkIndex);
    }
    co_return std::move(result);
  }

  // Return the queue of the chunk with the given `chunkIndex`, creating it if
  // that chunk has none yet.
  //
  // PRECONDITION: This runs on `strand_`.
  SharedChunkQueue getOrCreateChunk(size_t chunkIndex) {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    SharedChunkQueue& chunk = chunks_[chunkIndex];
    if (chunk == nullptr) {
      chunk = std::make_shared<ChunkQueue>(
          strand_, ioExecutor_, allocator_, spillFilename(chunkIndex),
          compression_, maxBufferedBlocksPerChunk_);
    }
    return chunk;
  }

  // Drop the queue of the chunk and delete its file. This is what makes the
  // disk space that this storage occupies proportional to the chunks that are
  // in flight.
  //
  // PRECONDITION: This runs on `strand_`.
  void eraseChunk(size_t chunkIndex) noexcept {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    auto iterator = chunks_.find(chunkIndex);
    if (iterator == chunks_.end()) {
      return;
    }
    // NOTE: The queue itself may well outlive this call, because an operation
    // of it that is still in flight holds a reference to it. Only its file is
    // taken away from it right here, which is what `close` is for.
    SharedSpillFile file = iterator->second->close();
    chunks_.erase(iterator);
    if (file == nullptr) {
      return;
    }
    // IMPORTANT: Closing and unlinking the file blocks, and the cost of the
    // unlink grows with the number of page-cache pages that the file still
    // holds (measured at roughly 78 microseconds per megabyte), so the last
    // reference to it must not die on the strand, which nothing may block. For
    // uniformly distributed `Id`s, whose blocks hardly compress, doing this
    // here instead of on the `ioExecutor_` costs 14 % of the whole merge.
    ad_utility::terminateIfThrows(
        [this, file = std::move(file)]() mutable {
          try {
            net::post(ioExecutor_, [file = std::move(file)]() mutable {
              // NOTE: A handler must not throw, and the destructor of a
              // `CompressedBlockFile` may (it deletes the file).
              ad_utility::terminateIfThrows(
                  [&file] { file.reset(); },
                  "Deleting the spill file of a chunk failed.");
            });
          } catch (...) {
            // The `post` could not be allocated, so the file is deleted right
            // here after all, which blocks the strand. That can only happen
            // once memory is exhausted.
          }
        },
        "Deleting the spill file of a chunk failed.");
  }
};

// A factory for a `CompressedIdTableBlockStorage`, for the constructor of
// `InOrderBlockSink`. The arguments are those of the constructor of that class,
// minus the strand, which the sink supplies.
template <size_t NumCols>
auto makeCompressedIdTableStorageFactory(
    net::any_io_executor ioExecutor, std::string filenamePrefix,
    AllocatorWithLimit<Id> allocator, size_t maxBufferedBlocksPerChunk,
    CompressedBlockFile::Compression compression = ZSTD_DEFAULT_LEVEL) {
  return [ioExecutor = std::move(ioExecutor),
          filenamePrefix = std::move(filenamePrefix),
          allocator = std::move(allocator), maxBufferedBlocksPerChunk,
          compression](const parallelBlockMerge::Strand& strand) {
    // NOTE: The storage is neither copyable nor movable, so it is returned as a
    // prvalue and thereby constructed directly into the sink.
    return CompressedIdTableBlockStorage<NumCols>{strand,
                                                  ioExecutor,
                                                  filenamePrefix,
                                                  allocator,
                                                  maxBufferedBlocksPerChunk,
                                                  compression};
  };
}

}  // namespace ad_utility

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_ENGINE_IDTABLE_COMPRESSEDIDTABLEBLOCKSTORAGE_H
