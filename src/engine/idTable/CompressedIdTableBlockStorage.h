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

#include <absl/functional/any_invocable.h>
#include <absl/strings/str_cat.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/post.hpp>
#include <cstddef>
#include <deque>
#include <exception>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "engine/idTable/CompressedIdTableBlocks.h"
#include "engine/idTable/IdTable.h"
#include "util/CompressedBlockFile.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
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
// back and decompressed again. The blocks of a chunk keep their order no matter
// whether they were spilled or not, because the queue of a chunk stores the
// blocks that are still in memory and the metadata of the spilled ones in a
// single FIFO.
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
// the `ioExecutor`, from which the result is posted back onto the strand. The
// `ioExecutor` is typically the very executor that the strand was derived from
// (the thread pool of the merge): work that is posted to that executor directly
// does not go through the strand and therefore does not serialize with it.
//
// LIFETIME: This storage has to outlive every operation of it that is in
// flight, because such an operation refers to it by plain pointer while its
// blocking part runs on the `ioExecutor`. The parallel merge guarantees this,
// because the handler of every operation transitively holds a `shared_ptr` to
// the `ParallelMergeState` that owns the sink and thereby this storage. The
// files are the exception: they are shared, so that erasing a chunk cannot
// delete a file out from under an operation that is still writing to or reading
// from it.
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
  // The completion handlers of the two asynchronous operations. In contrast to
  // the operations themselves, which are ordinary Boost.Asio operations that
  // take a completion token, these are type-erased, because the handler of a
  // consumer that has to wait is *stored* in the `Chunk` below.
  using StoreHandler = absl::AnyInvocable<void(std::exception_ptr, bool) &&>;
  using GetHandler = absl::AnyInvocable<void(std::exception_ptr, GetResult) &&>;

  // A single value in the FIFO queue of a chunk: either a block that is still
  // in memory (where an empty `OptionalBlock` is the end-of-chunk sentinel), or
  // the metadata of a block that was spilled to the file of that chunk.
  using Entry = std::variant<OptionalBlock, compressedIdTable::BlockMetadata>;

  // The file that a single chunk spills to. It is shared, because an operation
  // that runs on the `ioExecutor` holds on to it while the chunk may already
  // have been erased, see the LIFETIME note above. The file is deleted as soon
  // as the last of those references is gone.
  using SharedSpillFile = std::shared_ptr<CompressedBlockFile>;

  // The state of a single chunk.
  struct Chunk {
    std::deque<Entry> queue_;
    // The number of values in `queue_` that are blocks that are still in
    // memory. The end-of-chunk sentinel does not count, because it occupies no
    // memory and therefore is never spilled.
    size_t numBlocksInMemory_ = 0;
    // The consumer that waits for the next value of this chunk, if there is
    // one.
    GetHandler waitingConsumer_;
    // The file of this chunk, created with its first spilled block and null for
    // a chunk that has not spilled anything (yet).
    SharedSpillFile spillFile_;
  };

  Strand strand_;
  net::any_io_executor ioExecutor_;
  AllocatorWithLimit<Id> allocator_;
  size_t maxBufferedBlocksPerChunk_;
  std::string filenamePrefix_;
  CompressedBlockFile::CompressionLevel compression_;
  HashMap<size_t, Chunk> chunks_;
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
  // the spilled blocks are stored, see `CompressedBlockFile::CompressionLevel`;
  // a spill file is short-lived and read back almost immediately, so a low
  // level (or `NO_BLOCK_COMPRESSION`) is often faster than the default.
  //
  // NOTE: The `filenamePrefix` is not a filename but the prefix of one per
  // chunk, see `spillFilename`. It has to be unique among all the storages that
  // exist at the same time, because those files are overwritten if they exist
  // and deleted when the chunk that owns them is done.
  CompressedIdTableBlockStorage(
      Strand strand, net::any_io_executor ioExecutor,
      std::string filenamePrefix, AllocatorWithLimit<Id> allocator,
      size_t maxBufferedBlocksPerChunk,
      CompressedBlockFile::CompressionLevel compression = ZSTD_DEFAULT_LEVEL)
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
    return net::async_initiate<CompletionToken, void(std::exception_ptr, bool)>(
        [this, chunkIndex, block = std::move(block)](auto handler) mutable {
          storeBlockImpl(chunkIndex, std::move(block),
                         StoreHandler{std::move(handler)});
        },
        completionToken);
  }

  // Remove the front of the queue of the chunk, reading it back from the file
  // if it was spilled, see `BlockStorageConcept::getBlock`.
  template <typename CompletionToken>
  auto getBlock(size_t chunkIndex, CompletionToken&& completionToken) {
    return net::async_initiate<CompletionToken,
                               void(std::exception_ptr, GetResult)>(
        [this, chunkIndex](auto handler) mutable {
          getBlockImpl(chunkIndex, GetHandler{std::move(handler)});
        },
        completionToken);
  }

  // Complete every waiting consumer with a cancelled `GetResult`, see
  // `BlockStorageConcept::cancelAll`.
  //
  // NOTE: There is nothing to do for the producers: they never wait for a
  // consumer, only for their own I/O, and such a write is not cancelled but
  // runs to completion and is then still reported as stored (see
  // `finishSpill`). That is harmless, because `InOrderBlockSink` combines that
  // result with its own stop flag and therefore tells the producer to stop
  // anyway.
  void cancelAll() noexcept {
    ad_utility::terminateIfThrows(
        [this] {
          AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
          wasCancelled_ = true;
          // Collect the handlers before invoking any of them, so that none of
          // them runs while `chunks_` is being iterated.
          std::vector<GetHandler> waitingConsumers;
          for (auto& chunk : chunks_) {
            if (chunk.second.waitingConsumer_) {
              waitingConsumers.push_back(
                  std::move(chunk.second.waitingConsumer_));
            }
          }
          for (auto& consumer : waitingConsumers) {
            std::move(consumer)(std::exception_ptr{}, GetResult{});
          }
        },
        "Cancelling a `CompressedIdTableBlockStorage` failed.");
  }

  // The number of chunks for which a queue currently exists. Only used to test
  // that a chunk is indeed dropped as soon as its end-of-chunk sentinel was
  // handed out.
  size_t numLiveChunksForTesting() const noexcept { return chunks_.size(); }

 private:
  // The body of `storeBlock`, on the type-erased handler.
  void storeBlockImpl(size_t chunkIndex, OptionalBlock block,
                      StoreHandler handler) {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    AD_CORRECTNESS_CHECK(!wasCancelled_);
    Chunk* chunk = nullptr;
    try {
      chunk = &chunks_[chunkIndex];
    } catch (...) {
      std::move(handler)(std::current_exception(), false);
      return;
    }
    // The end-of-chunk sentinel is never spilled, because it occupies no memory
    // and the consumer needs it to make progress.
    if (!block.has_value() ||
        chunk->numBlocksInMemory_ < maxBufferedBlocksPerChunk_) {
      bool isBlock = block.has_value();
      try {
        chunk->queue_.push_back(Entry{std::move(block)});
      } catch (...) {
        std::move(handler)(std::current_exception(), false);
        return;
      }
      if (isBlock) {
        ++chunk->numBlocksInMemory_;
      }
      std::move(handler)(std::exception_ptr{}, true);
      // IMPORTANT: Serve the consumer only after the producer was completed and
      // after all the state was updated, because the handler of the consumer
      // may call right back into this storage.
      serveWaitingConsumer(chunkIndex);
      return;
    }
    spillBlock(chunkIndex, *chunk, std::move(block).value(),
               std::move(handler));
  }

  // The body of `getBlock`, on the type-erased handler.
  void getBlockImpl(size_t chunkIndex, GetHandler handler) {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    AD_CORRECTNESS_CHECK(!wasCancelled_);
    try {
      // NOTE: The queue of a chunk that does not exist yet is created, because
      // the consumer of a chunk may well be faster than its producer, see
      // `BlockStorageConcept::getBlock`.
      static_cast<void>(chunks_[chunkIndex]);
    } catch (...) {
      std::move(handler)(std::current_exception(), GetResult{});
      return;
    }
    serveConsumer(chunkIndex, std::move(handler));
  }

  // The body of `getBlock`, which is also how a consumer that had to wait is
  // served as soon as its chunk has become non-empty.
  //
  // PRECONDITION: This runs on `strand_`, the chunk exists, and no other
  // consumer of it is currently waiting.
  void serveConsumer(size_t chunkIndex, GetHandler handler) {
    Chunk& chunk = chunks_.at(chunkIndex);
    AD_CORRECTNESS_CHECK(!chunk.waitingConsumer_);
    if (chunk.queue_.empty()) {
      chunk.waitingConsumer_ = std::move(handler);
      return;
    }
    Entry entry = std::move(chunk.queue_.front());
    chunk.queue_.pop_front();
    if (std::holds_alternative<OptionalBlock>(entry)) {
      auto block = std::get<OptionalBlock>(std::move(entry));
      if (!block.has_value()) {
        // The end-of-chunk sentinel, so this chunk is done and everything that
        // belongs to it may be dropped, see `BlockStorageConcept::getBlock`.
        //
        // IMPORTANT: This invalidates `chunk`, which must therefore not be
        // touched afterwards.
        eraseChunk(chunkIndex);
        completeOnStrand(std::move(handler), std::exception_ptr{},
                         GetResult::endOfChunk());
        return;
      }
      --chunk.numBlocksInMemory_;
      // NOTE: The completion is posted and never inline, because the handler of
      // the consumer may call right back into this storage, which must not
      // happen while an operation of it is still running.
      completeOnStrand(std::move(handler), std::exception_ptr{},
                       GetResult::fromBlock(std::move(block).value()));
      return;
    }
    // NOTE: A spilled entry can only exist if the chunk has a file, and the
    // file is passed on as a `shared_ptr`, so erasing the chunk concurrently
    // cannot delete it while it is being read.
    AD_CORRECTNESS_CHECK(chunk.spillFile_ != nullptr);
    readSpilledBlock(
        chunk.spillFile_,
        std::get<compressedIdTable::BlockMetadata>(std::move(entry)),
        std::move(handler));
  }

  // If a consumer of the chunk with the given `chunkIndex` is waiting, and that
  // chunk is not empty anymore, serve that consumer now.
  //
  // PRECONDITION: This runs on `strand_`.
  void serveWaitingConsumer(size_t chunkIndex) {
    auto iterator = chunks_.find(chunkIndex);
    if (iterator == chunks_.end() || !iterator->second.waitingConsumer_) {
      return;
    }
    GetHandler handler = std::move(iterator->second.waitingConsumer_);
    serveConsumer(chunkIndex, std::move(handler));
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
    // NOTE: A chunk is only erased once its end-of-chunk sentinel was handed
    // out to the consumer, so that consumer cannot be waiting at the same time.
    AD_CORRECTNESS_CHECK(!iterator->second.waitingConsumer_);
    SharedSpillFile file = std::move(iterator->second.spillFile_);
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

  // Compress the `block` and write it to the file of its chunk on
  // `ioExecutor_`, then append its metadata to the queue of that chunk on
  // `strand_`, see `finishSpill`. Create the file first if this is the chunk's
  // first spill.
  //
  // PRECONDITION: This runs on `strand_`, and the chunk exists.
  void spillBlock(size_t chunkIndex, Chunk& chunk, Block block,
                  StoreHandler handler) {
    SharedSpillFile file;
    try {
      file = getOrCreateSpillFile(chunkIndex, chunk);
    } catch (...) {
      std::move(handler)(std::current_exception(), false);
      return;
    }
    net::post(ioExecutor_, [this, chunkIndex, file = std::move(file),
                            block = std::move(block),
                            handler = std::move(handler)]() mutable {
      std::exception_ptr exception;
      compressedIdTable::BlockMetadata metadata;
      try {
        // NOTE: The block becomes readable immediately, because
        // `CompressedBlockFile::appendBlock` flushes the file; its chunk may be
        // consumed while further blocks are still being written.
        metadata =
            compressedIdTable::writeBlock(*file, block, 0, block.numRows());
      } catch (...) {
        exception = std::current_exception();
      }
      net::post(strand_, [this, chunkIndex, exception = std::move(exception),
                          metadata = std::move(metadata),
                          handler = std::move(handler)]() mutable {
        finishSpill(chunkIndex, std::move(exception), std::move(metadata),
                    std::move(handler));
      });
    });
  }

  // Return the file that the chunk with the given `chunkIndex` spills to,
  // creating it if this is that chunk's first spilled block.
  //
  // PRECONDITION: This runs on `strand_`.
  SharedSpillFile getOrCreateSpillFile(size_t chunkIndex, Chunk& chunk) {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    if (chunk.spillFile_ == nullptr) {
      chunk.spillFile_ = std::make_shared<CompressedBlockFile>(
          spillFilename(chunkIndex), compression_);
    }
    return chunk.spillFile_;
  }

  // Append the `metadata` of a block that was just spilled to the queue of its
  // chunk and complete the producer.
  //
  // PRECONDITION: This runs on `strand_`.
  void finishSpill(size_t chunkIndex, std::exception_ptr exception,
                   compressedIdTable::BlockMetadata metadata,
                   StoreHandler handler) {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    if (exception != nullptr) {
      std::move(handler)(std::move(exception), false);
      return;
    }
    auto iterator = chunks_.find(chunkIndex);
    if (iterator == chunks_.end()) {
      // The chunk was erased while the block was being written, so there is no
      // consumer left that could care about that block.
      std::move(handler)(std::exception_ptr{}, false);
      return;
    }
    try {
      iterator->second.queue_.push_back(Entry{std::move(metadata)});
    } catch (...) {
      std::move(handler)(std::current_exception(), false);
      return;
    }
    std::move(handler)(std::exception_ptr{}, true);
    // IMPORTANT: See the corresponding note in `storeBlockImpl`.
    serveWaitingConsumer(chunkIndex);
  }

  // Read a block that was spilled back from the `file` of its chunk on
  // `ioExecutor_` and complete the `handler` with it on `strand_`. The `file`
  // is held for the duration of the read, see `SharedSpillFile`.
  //
  // PRECONDITION: This runs on `strand_`.
  void readSpilledBlock(SharedSpillFile file,
                        compressedIdTable::BlockMetadata metadata,
                        GetHandler handler) {
    net::post(ioExecutor_, [this, file = std::move(file),
                            metadata = std::move(metadata),
                            handler = std::move(handler)]() mutable {
      std::exception_ptr exception;
      OptionalBlock block;
      try {
        block =
            compressedIdTable::readBlock<NumCols>(*file, metadata, allocator_);
      } catch (...) {
        exception = std::current_exception();
      }
      // NOTE: The two cases are spelled out, because passing both
      // `std::move(exception)` and something that inspects `exception` to the
      // same call would depend on the unspecified order in which the arguments
      // of a call are evaluated.
      if (exception != nullptr) {
        completeOnStrand(std::move(handler), std::move(exception), GetResult{});
      } else {
        completeOnStrand(std::move(handler), std::exception_ptr{},
                         GetResult::fromBlock(std::move(block).value()));
      }
    });
  }

  // Invoke the `handler` with the given arguments, but only after a hop onto
  // `strand_`, as the CONTRACT of the `BlockStorageConcept` requires.
  void completeOnStrand(GetHandler handler, std::exception_ptr exception,
                        GetResult result) {
    net::post(strand_,
              [handler = std::move(handler), exception = std::move(exception),
               result = std::move(result)]() mutable {
                std::move(handler)(std::move(exception), std::move(result));
              });
  }
};

// A factory for a `CompressedIdTableBlockStorage`, for the constructor of
// `InOrderBlockSink`. The arguments are those of the constructor of that class,
// minus the strand, which the sink supplies.
template <size_t NumCols>
auto makeCompressedIdTableStorageFactory(
    net::any_io_executor ioExecutor, std::string filenamePrefix,
    AllocatorWithLimit<Id> allocator, size_t maxBufferedBlocksPerChunk,
    CompressedBlockFile::CompressionLevel compression = ZSTD_DEFAULT_LEVEL) {
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
