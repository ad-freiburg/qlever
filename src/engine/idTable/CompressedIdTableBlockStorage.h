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

#include <absl/strings/str_cat.h>

#include <boost/asio/any_io_executor.hpp>
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
#include "util/parallelBlockMerge/BlockStorage.h"

namespace ad_utility {

namespace net = boost::asio;

// A `parallelBlockMerge::BlockStorage` for blocks of type `IdTableStatic`,
// which keeps only a bounded number of blocks per run in memory and spills the
// rest to a temporary file, compressed.
//
// This is the alternative to the `InMemoryBlockStorage`, which bounds the
// memory consumption of the merge by making a producer wait once the consumer
// has fallen behind. Here a producer never waits (except for the duration of
// the I/O), so a chunk that is far ahead of the consumer can be merged to
// completion; the price is that its blocks have to be compressed, written, read
// back and decompressed again. The blocks of a run keep their order no matter
// whether they were spilled or not, because the queue of a run stores the
// blocks that are still in memory and the metadata of the spilled ones in a
// single FIFO.
//
// Each run that actually spills owns a file of its own, which is created with
// its first spilled block and deleted again by `eraseRun`. The disk space that
// this storage occupies is therefore proportional to the runs that are in
// flight and not to their total number, exactly like the memory that it
// occupies. A run that never spills never creates a file at all. Per-run files
// are also faster than one shared file, because appending to them no longer
// contends for a single exclusive lock.
//
// NOTE: Once the storage was cancelled nothing calls `eraseRun` anymore (see
// the class comment of `InOrderBlockSink`), so the files of the runs that were
// still in flight live until the storage itself is destroyed.
//
// THREAD SAFETY: This class implements the CONTRACT of
// `parallelBlockMerge::BlockStorage`: all of its member functions have to run
// on the `strand` that it was constructed with, and none of them ever blocks
// that strand. The compression, the decompression and the I/O all run on the
// `ioExecutor`, from which the result is posted back onto the strand. The
// `ioExecutor` is typically the very executor that the strand was derived from
// (the thread pool of the merge): work that is posted to that executor directly
// does not go through the strand and therefore does not serialize with it.
//
// LIFETIME: This storage has to outlive every operation of it that is in
// flight, because such an operation refers to it by plain pointer while its
// blocking part runs on the `ioExecutor`. The parallel merge guarantees this,
// because the handler of every operation transitively holds a `shared_ptr` to
// the `ParallelMergeState` that owns the sink and thereby this storage. The
// files are the exception: they are shared, so that an `eraseRun` cannot delete
// a file out from under an operation that is still writing to or reading from
// it.
//
// NOTE: A `getBlock` whose block has to be read back from the file is not
// cancelled by `cancelAll`, so a consumer that aborts the merge while such a
// read is in flight sees that one last block instead of the end of the range.
// Both are legal outcomes of a race between the consumer and the abort, and the
// `InMemoryBlockStorage` is only more eager and not more deterministic here.
template <size_t NumCols = 0>
class CompressedIdTableBlockStorage
    : public parallelBlockMerge::BlockStorage<IdTableStatic<NumCols>> {
 public:
  using Block = IdTableStatic<NumCols>;
  using Base = parallelBlockMerge::BlockStorage<Block>;
  using OptionalBlock = typename Base::OptionalBlock;
  using GetResult = typename Base::GetResult;
  using StoreHandler = typename Base::StoreHandler;
  using GetHandler = typename Base::GetHandler;
  using Strand = typename Base::Strand;
  using StorageFactory = parallelBlockMerge::BlockStorageFactory<Block>;

 private:
  // A single value in the FIFO queue of a run: either a block that is still in
  // memory (where an empty `OptionalBlock` is the end-of-run sentinel), or the
  // metadata of a block that was spilled to the file of that run.
  using Entry = std::variant<OptionalBlock, compressedIdTable::BlockMetadata>;

  // The file that a single run spills to. It is shared, because an operation
  // that runs on the `ioExecutor` holds on to it while `eraseRun` may already
  // have dropped the run, see the LIFETIME note above. The file is deleted as
  // soon as the last of those references is gone.
  using SharedSpillFile = std::shared_ptr<CompressedBlockFile>;

  // The state of a single run.
  struct Run {
    std::deque<Entry> queue_;
    // The number of values in `queue_` that are blocks that are still in
    // memory. The end-of-run sentinel does not count, because it occupies no
    // memory and therefore is never spilled.
    size_t numBlocksInMemory_ = 0;
    // The consumer that waits for the next value of this run, if there is one.
    GetHandler waitingConsumer_;
    // The file of this run, created with its first spilled block and null for a
    // run that has not spilled anything (yet).
    SharedSpillFile spillFile_;
  };

  Strand strand_;
  net::any_io_executor ioExecutor_;
  AllocatorWithLimit<Id> allocator_;
  size_t maxBufferedBlocksPerRun_;
  std::string filenamePrefix_;
  CompressedBlockFile::Compression compression_;
  HashMap<size_t, Run> runs_;
  // Set by `cancelAll`, see `InMemoryBlockStorage` for why this is checked.
  bool wasCancelled_ = false;

 public:
  // Construct from the `strand` that all the operations of this storage are
  // confined to, the `ioExecutor` on which the compression, the decompression
  // and the I/O are run, the name of the file to spill to (which is overwritten
  // if it exists and deleted when this storage is destroyed), the `allocator`
  // for the blocks that are read back, and the number of blocks that are kept
  // in memory per run before that run starts spilling. That number may be zero,
  // in which case every block is spilled. The `compression` decides how the
  // spilled blocks are stored, see `CompressedBlockFile::Compression`; a spill
  // file is short-lived and read back almost immediately, so a low level (or
  // `NO_BLOCK_COMPRESSION`) is often faster than the default.
  //
  // NOTE: The `filenamePrefix` is not a filename but the prefix of one per run,
  // see `spillFilename`. It has to be unique among all the storages that exist
  // at the same time, because those files are overwritten if they exist and
  // deleted when the run that owns them is done.
  CompressedIdTableBlockStorage(
      Strand strand, net::any_io_executor ioExecutor,
      std::string filenamePrefix, AllocatorWithLimit<Id> allocator,
      size_t maxBufferedBlocksPerRun,
      CompressedBlockFile::Compression compression = ZSTD_DEFAULT_LEVEL)
      : strand_{std::move(strand)},
        ioExecutor_{std::move(ioExecutor)},
        allocator_{std::move(allocator)},
        maxBufferedBlocksPerRun_{maxBufferedBlocksPerRun},
        filenamePrefix_{std::move(filenamePrefix)},
        compression_{compression} {}

  // The `StorageFactory` that creates such a storage, to be passed to the
  // corresponding constructor of `InOrderBlockSink`. The arguments are those of
  // the constructor above, minus the strand, which the sink supplies.
  static StorageFactory makeStorageFactory(
      net::any_io_executor ioExecutor, std::string filenamePrefix,
      AllocatorWithLimit<Id> allocator, size_t maxBufferedBlocksPerRun,
      CompressedBlockFile::Compression compression = ZSTD_DEFAULT_LEVEL) {
    return [ioExecutor = std::move(ioExecutor),
            filenamePrefix = std::move(filenamePrefix),
            allocator = std::move(allocator), maxBufferedBlocksPerRun,
            compression](const Strand& strand) mutable {
      return std::make_unique<CompressedIdTableBlockStorage>(
          strand, std::move(ioExecutor), std::move(filenamePrefix),
          std::move(allocator), maxBufferedBlocksPerRun, compression);
    };
  }

  // The common prefix of the names of all the files of this storage.
  const std::string& filenamePrefix() const { return filenamePrefix_; }

  // The name of the file that the run with the given `runIndex` spills to. That
  // file only exists while the run has spilled at least one block and has not
  // been erased yet.
  std::string spillFilename(size_t runIndex) const {
    return absl::StrCat(filenamePrefix_, ".", runIndex);
  }

  // Append the `block` to the queue of the run, spilling it if that run already
  // buffers `maxBufferedBlocksPerRun` blocks, see `BlockStorage::storeBlock`.
  void storeBlock(size_t runIndex, OptionalBlock block,
                  StoreHandler handler) override {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    AD_CORRECTNESS_CHECK(!wasCancelled_);
    Run* run = nullptr;
    try {
      run = &runs_[runIndex];
    } catch (...) {
      std::move(handler)(std::current_exception(), false);
      return;
    }
    // The end-of-run sentinel is never spilled, because it occupies no memory
    // and the consumer needs it to make progress.
    if (!block.has_value() ||
        run->numBlocksInMemory_ < maxBufferedBlocksPerRun_) {
      bool isBlock = block.has_value();
      try {
        run->queue_.push_back(Entry{std::move(block)});
      } catch (...) {
        std::move(handler)(std::current_exception(), false);
        return;
      }
      if (isBlock) {
        ++run->numBlocksInMemory_;
      }
      std::move(handler)(std::exception_ptr{}, true);
      // IMPORTANT: Serve the consumer only after the producer was completed and
      // after all the state was updated, because the handler of the consumer
      // may call right back into this storage.
      serveWaitingConsumer(runIndex);
      return;
    }
    spillBlock(runIndex, *run, std::move(block).value(), std::move(handler));
  }

  // Remove the front of the queue of the run, reading it back from the file if
  // it was spilled, see `BlockStorage::getBlock`.
  void getBlock(size_t runIndex, GetHandler handler) override {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    AD_CORRECTNESS_CHECK(!wasCancelled_);
    Run* run = nullptr;
    try {
      run = &runs_[runIndex];
    } catch (...) {
      std::move(handler)(std::current_exception(), GetResult{});
      return;
    }
    serveConsumer(*run, std::move(handler));
  }

  // Drop the queue of the run and delete its file, see
  // `BlockStorage::eraseRun`. This is what makes the disk space that this
  // storage occupies proportional to the runs that are in flight.
  void eraseRun(size_t runIndex) noexcept override {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    // NOTE: The consumer only erases a run once it has retrieved that run's
    // end-of-run sentinel, so it cannot be waiting for it at the same time.
    auto iterator = runs_.find(runIndex);
    if (iterator == runs_.end()) {
      return;
    }
    AD_CORRECTNESS_CHECK(!iterator->second.waitingConsumer_);
    SharedSpillFile file = std::move(iterator->second.spillFile_);
    runs_.erase(iterator);
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
                  "Deleting the spill file of a run failed.");
            });
          } catch (...) {
            // The `post` could not be allocated, so the file is deleted right
            // here after all, which blocks the strand. That can only happen
            // once memory is exhausted.
          }
        },
        "Deleting the spill file of a run failed.");
  }

  // Complete every waiting consumer with a cancelled `GetResult`, see
  // `BlockStorage::cancelAll`.
  //
  // NOTE: There is nothing to do for the producers: they never wait for a
  // consumer, only for their own I/O, and such a write is not cancelled but
  // runs to completion and is then still reported as stored (see
  // `finishSpill`). That is harmless, because `InOrderBlockSink` combines that
  // result with its own stop flag and therefore tells the producer to stop
  // anyway.
  void cancelAll() noexcept override {
    ad_utility::terminateIfThrows(
        [this] {
          AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
          wasCancelled_ = true;
          // Collect the handlers before invoking any of them, so that none of
          // them runs while `runs_` is being iterated.
          std::vector<GetHandler> waitingConsumers;
          for (auto& run : runs_) {
            if (run.second.waitingConsumer_) {
              waitingConsumers.push_back(
                  std::move(run.second.waitingConsumer_));
            }
          }
          for (auto& consumer : waitingConsumers) {
            std::move(consumer)(std::exception_ptr{}, GetResult{});
          }
        },
        "Cancelling a `CompressedIdTableBlockStorage` failed.");
  }

 private:
  // The body of `getBlock`, which is also how a consumer that had to wait is
  // served as soon as its run has become non-empty.
  //
  // PRECONDITION: This runs on `strand_`, and no other consumer of this run is
  // currently waiting.
  void serveConsumer(Run& run, GetHandler handler) {
    AD_CORRECTNESS_CHECK(!run.waitingConsumer_);
    if (run.queue_.empty()) {
      run.waitingConsumer_ = std::move(handler);
      return;
    }
    Entry entry = std::move(run.queue_.front());
    run.queue_.pop_front();
    if (std::holds_alternative<OptionalBlock>(entry)) {
      auto block = std::get<OptionalBlock>(std::move(entry));
      if (block.has_value()) {
        --run.numBlocksInMemory_;
      }
      // NOTE: The completion is posted and never inline, because the handler of
      // the consumer may call right back into this storage, which must not
      // happen while an operation of it is still running.
      completeOnStrand(std::move(handler), std::exception_ptr{},
                       GetResult{std::move(block)});
      return;
    }
    // NOTE: A spilled entry can only exist if the run has a file, and the file
    // is passed on as a `shared_ptr`, so a concurrent `eraseRun` cannot delete
    // it while it is being read.
    AD_CORRECTNESS_CHECK(run.spillFile_ != nullptr);
    readSpilledBlock(
        run.spillFile_,
        std::get<compressedIdTable::BlockMetadata>(std::move(entry)),
        std::move(handler));
  }

  // If a consumer of the run with the given `runIndex` is waiting, and that run
  // is not empty anymore, serve that consumer now.
  //
  // PRECONDITION: This runs on `strand_`.
  void serveWaitingConsumer(size_t runIndex) {
    auto iterator = runs_.find(runIndex);
    if (iterator == runs_.end() || !iterator->second.waitingConsumer_) {
      return;
    }
    Run& run = iterator->second;
    GetHandler handler = std::move(run.waitingConsumer_);
    serveConsumer(run, std::move(handler));
  }

  // Compress the `block` and write it to the file of its run on `ioExecutor_`,
  // then append its metadata to the queue of that run on `strand_`, see
  // `finishSpill`. Create the file first if this is the run's first spill.
  //
  // PRECONDITION: This runs on `strand_`, and the run exists.
  void spillBlock(size_t runIndex, Run& run, Block block,
                  StoreHandler handler) {
    SharedSpillFile file;
    try {
      file = getOrCreateSpillFile(runIndex, run);
    } catch (...) {
      std::move(handler)(std::current_exception(), false);
      return;
    }
    net::post(ioExecutor_, [this, runIndex, file = std::move(file),
                            block = std::move(block),
                            handler = std::move(handler)]() mutable {
      std::exception_ptr exception;
      compressedIdTable::BlockMetadata metadata;
      try {
        metadata =
            compressedIdTable::writeBlock(*file, block, 0, block.numRows());
        // The block has to become readable immediately, because its run may be
        // consumed while further blocks are still being written.
        file->flush();
      } catch (...) {
        exception = std::current_exception();
      }
      net::post(strand_, [this, runIndex, exception = std::move(exception),
                          metadata = std::move(metadata),
                          handler = std::move(handler)]() mutable {
        finishSpill(runIndex, std::move(exception), std::move(metadata),
                    std::move(handler));
      });
    });
  }

  // Return the file that the run with the given `runIndex` spills to, creating
  // it if this is that run's first spilled block.
  //
  // PRECONDITION: This runs on `strand_`.
  SharedSpillFile getOrCreateSpillFile(size_t runIndex, Run& run) {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    if (run.spillFile_ == nullptr) {
      run.spillFile_ = std::make_shared<CompressedBlockFile>(
          spillFilename(runIndex), compression_);
    }
    return run.spillFile_;
  }

  // Append the `metadata` of a block that was just spilled to the queue of its
  // run and complete the producer.
  //
  // PRECONDITION: This runs on `strand_`.
  void finishSpill(size_t runIndex, std::exception_ptr exception,
                   compressedIdTable::BlockMetadata metadata,
                   StoreHandler handler) {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    if (exception != nullptr) {
      std::move(handler)(std::move(exception), false);
      return;
    }
    auto iterator = runs_.find(runIndex);
    if (iterator == runs_.end()) {
      // The run was erased while the block was being written, so there is no
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
    // IMPORTANT: See the corresponding note in `storeBlock`.
    serveWaitingConsumer(runIndex);
  }

  // Read a block that was spilled back from the `file` of its run on
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
                         GetResult{std::move(block)});
      }
    });
  }

  // Invoke the `handler` with the given arguments, but only after a hop onto
  // `strand_`, as the CONTRACT of `BlockStorage` requires.
  void completeOnStrand(GetHandler handler, std::exception_ptr exception,
                        GetResult result) {
    net::post(strand_,
              [handler = std::move(handler), exception = std::move(exception),
               result = std::move(result)]() mutable {
                std::move(handler)(std::move(exception), std::move(result));
              });
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_ENGINE_IDTABLE_COMPRESSEDIDTABLEBLOCKSTORAGE_H
