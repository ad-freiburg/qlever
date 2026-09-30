// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_INMEMORYBLOCKSTORAGE_H
#define QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_INMEMORYBLOCKSTORAGE_H

// A model of the `BlockStorageConcept` that simply keeps the blocks in memory.
// It only makes sense together with the `InOrderBlockSink`, which is
// coroutine-based, so this whole header is empty when
// `QLEVER_REDUCED_FEATURE_SET_FOR_CPP17` is set, see
// `util/parallelBlockMerge/BlockStorage.h`.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <boost/asio/async_result.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <boost/system/error_code.hpp>
#include <cstddef>
#include <exception>
#include <memory>
#include <utility>

#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/parallelBlockMerge/BlockStorage.h"

namespace ad_utility::parallelBlockMerge {

// The `BlockStorageConcept` that simply keeps the blocks in memory, with a
// fixed number of blocks that are buffered per chunk. A producer that has
// finished a block while that buffer is full has to wait, so this
// implementation is the back-pressure that bounds the memory consumption of the
// merge.
//
// Each chunk owns a single channel of capacity `maxBufferedBlocksPerChunk`,
// which is the bounded FIFO queue that the `BlockStorageConcept` asks for; the
// end-of-chunk sentinel travels through the same channel, so storing it can
// suspend as well.
//
// With `releaseChunkOnConsumption`, storing the sentinel only completes once
// the consumer has retrieved it, that is, once the whole output of the chunk
// has been consumed. A producer that holds a slot among the chunks in flight
// (see `ParallelMergeState`) for as long as its chunk runs then keeps that slot
// until its output is consumed, which bounds the number of chunks whose output
// is buffered, and hence the memory, to the chunks in flight. Without it, a
// merge that is faster than its consumer buffers its entire output.
//
// NOTE: A plain (non-concurrent) channel suffices, and the `HashMap` needs no
// synchronization, because every operation runs on `strand_`, see the CONTRACT
// of the `BlockStorageConcept`.
template <typename Block>
class InMemoryBlockStorage {
 public:
  using OptionalBlock = parallelBlockMerge::OptionalBlock<Block>;
  using GetResult = parallelBlockMerge::GetResult<Block>;
  // The channel that transports the blocks of a single chunk from its producer
  // to the consumer.
  using BlockChannel = net::experimental::channel<void(
      boost::system::error_code, OptionalBlock)>;
  // The channels are shared, because both the producer of a chunk and the
  // consumer hold on to one across a suspension, while the map entry is removed
  // as soon as that chunk is done (see `getBlock`).
  using SharedBlockChannel = std::shared_ptr<BlockChannel>;
  // The channel through which the consumer acknowledges the retrieval of the
  // end-of-chunk sentinel, see `releaseChunkOnConsumption` above.
  using AckChannel =
      net::experimental::channel<void(boost::system::error_code)>;
  using SharedAckChannel = std::shared_ptr<AckChannel>;
  struct Chunk {
    SharedBlockChannel blocks_;
    SharedAckChannel ack_;
  };

 private:
  Strand strand_;
  size_t maxBufferedBlocksPerChunk_;
  bool releaseChunkOnConsumption_;
  HashMap<size_t, Chunk> chunks_;
  // Set by `cancelAll`, only to check the precondition that no operation is
  // initiated afterwards. That check matters, because the teardown of the sink
  // is only airtight as long as no channel is created after the cancellation,
  // see the class comment of `InOrderBlockSink`.
  bool wasCancelled_ = false;

 public:
  // Construct from the `strand` that all the operations of this storage are
  // confined to, the number of blocks that are buffered per chunk (which has
  // to be at least one), and whether storing the end-of-chunk sentinel waits
  // for its consumption (see the class comment).
  InMemoryBlockStorage(Strand strand, size_t maxBufferedBlocksPerChunk,
                       bool releaseChunkOnConsumption = false)
      : strand_{std::move(strand)},
        maxBufferedBlocksPerChunk_{maxBufferedBlocksPerChunk},
        releaseChunkOnConsumption_{releaseChunkOnConsumption} {
    AD_CONTRACT_CHECK(maxBufferedBlocksPerChunk > 0);
  }

  // Store `block` in the channel of the chunk, see
  // `BlockStorageConcept::storeBlock`.
  template <typename CompletionToken>
  auto storeBlock(size_t chunkIndex, OptionalBlock block,
                  CompletionToken&& completionToken) {
    return net::async_initiate<CompletionToken, void(std::exception_ptr, bool)>(
        [this, chunkIndex, block = std::move(block)](auto handler) mutable {
          Chunk chunk;
          try {
            chunk = getOrCreateChunk(chunkIndex);
          } catch (...) {
            std::move(handler)(std::current_exception(), false);
            return;
          }
          // Whether the completion has to wait for the consumer to retrieve
          // this sentinel, see `releaseChunkOnConsumption`.
          bool waitForAck = releaseChunkOnConsumption_ && !block.has_value();
          // NOTE: The channels are moved into the completion handler, so that
          // they stay alive while this operation is suspended, no matter what
          // happens to the map entry in the meantime.
          auto* channelPtr = chunk.blocks_.get();
          channelPtr->async_send(
              boost::system::error_code{}, std::move(block),
              [chunk = std::move(chunk), handler = std::move(handler),
               waitForAck](boost::system::error_code errorCode) mutable {
                // NOTE: This runs on `strand_`, because the channel was created
                // with `strand_` as its executor and this handler has no
                // executor of its own that would override that. The only error
                // that can occur is that the channel was cancelled, see
                // `cancelAll`.
                if (errorCode || !waitForAck) {
                  std::move(handler)(std::exception_ptr{}, !errorCode);
                  return;
                }
                auto* ackPtr = chunk.ack_.get();
                ackPtr->async_receive(
                    [chunk = std::move(chunk), handler = std::move(handler)](
                        boost::system::error_code errorCode) mutable {
                      std::move(handler)(std::exception_ptr{}, !errorCode);
                    });
              });
        },
        completionToken);
  }

  // Receive the next value from the channel of the chunk, see
  // `BlockStorageConcept::getBlock`.
  template <typename CompletionToken>
  auto getBlock(size_t chunkIndex, CompletionToken&& completionToken) {
    return net::async_initiate<CompletionToken,
                               void(std::exception_ptr, GetResult)>(
        [this, chunkIndex](auto handler) mutable {
          Chunk chunk;
          try {
            chunk = getOrCreateChunk(chunkIndex);
          } catch (...) {
            std::move(handler)(std::current_exception(), GetResult{});
            return;
          }
          // NOTE: The channels are moved into the completion handler, see
          // `storeBlock` above. Here they also keep the channels alive while
          // the handler erases the very map entry that owns them.
          auto* channelPtr = chunk.blocks_.get();
          channelPtr->async_receive([this, chunkIndex, chunk = std::move(chunk),
                                     handler = std::move(handler)](
                                        boost::system::error_code errorCode,
                                        OptionalBlock block) mutable {
            // NOTE: This runs on `strand_`, see `storeBlock` above.
            if (errorCode) {
              std::move(handler)(std::exception_ptr{}, GetResult{});
              return;
            }
            if (!block.has_value()) {
              // The end-of-chunk sentinel, so this chunk is done and
              // everything that belongs to it may be dropped, see
              // `BlockStorageConcept::getBlock`.
              eraseChunk(chunkIndex);
              if (releaseChunkOnConsumption_) {
                // Let the producer of the sentinel complete, see
                // `releaseChunkOnConsumption`. The ack channel has room for
                // this one value, so this never suspends.
                chunk.ack_->try_send(boost::system::error_code{});
              }
              std::move(handler)(std::exception_ptr{}, GetResult::endOfChunk());
              return;
            }
            std::move(handler)(std::exception_ptr{},
                               GetResult::fromBlock(std::move(block).value()));
          });
        },
        completionToken);
  }

  // Cancel all the channels, see `BlockStorageConcept::cancelAll`.
  void cancelAll() noexcept {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    wasCancelled_ = true;
    // Wake up everybody who is currently suspended.
    //
    // NOTE: There is no need to run this sweep more than once, because no
    // channel is ever created afterwards, so a later sweep would find nothing
    // new (see the PRECONDITIONS of the `BlockStorageConcept` and the class
    // comment of `InOrderBlockSink`).
    //
    // IMPORTANT: `cancel()` is also the *only* primitive that may be used here.
    // `close()` alone does not wake an operation that is already suspended, and
    // a `cancel()` after a `close()` is outright undefined behavior, because
    // `cancel()` tells a suspended send from a suspended receive by the
    // internal send state that `close()` overwrites.
    for (const auto& chunk : chunks_) {
      chunk.second.blocks_->cancel();
      chunk.second.ack_->cancel();
    }
  }

  // The number of chunks for which a channel currently exists. Only used to
  // test that a chunk is indeed dropped as soon as its end-of-chunk sentinel
  // was handed out.
  size_t numLiveChunksForTesting() const noexcept { return chunks_.size(); }

 private:
  // Destroy the channel of the chunk with the given `chunkIndex`.
  void eraseChunk(size_t chunkIndex) noexcept {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    // NOTE: Erasing the entry is safe even if the producer or the consumer of
    // that chunk still holds the channel, because the channels are shared.
    chunks_.erase(chunkIndex);
  }

  // Return the channel of the chunk with the given `chunkIndex`, creating it if
  // it does not exist yet.
  Chunk getOrCreateChunk(size_t chunkIndex) {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    AD_CORRECTNESS_CHECK(!wasCancelled_);
    auto& chunk = chunks_[chunkIndex];
    if (chunk.blocks_ == nullptr) {
      chunk.blocks_ =
          std::make_shared<BlockChannel>(strand_, maxBufferedBlocksPerChunk_);
      chunk.ack_ = std::make_shared<AckChannel>(strand_, 1);
    }
    return chunk;
  }
};

// A factory for an `InMemoryBlockStorage` that buffers at most
// `maxBufferedBlocksPerChunk` blocks per chunk, for the constructor of
// `InOrderBlockSink`.
template <typename Block>
auto makeInMemoryStorageFactory(size_t maxBufferedBlocksPerChunk,
                                bool releaseChunkOnConsumption = false) {
  return [maxBufferedBlocksPerChunk,
          releaseChunkOnConsumption](const Strand& strand) {
    return InMemoryBlockStorage<Block>{strand, maxBufferedBlocksPerChunk,
                                       releaseChunkOnConsumption};
  };
}

}  // namespace ad_utility::parallelBlockMerge

#else

#include <cstddef>
#include <variant>

namespace ad_utility::parallelBlockMerge {
// The parallel merge always takes its serial path in C++17 mode, which ignores
// the storage factory, so this placeholder lets callers pass one
// unconditionally.
template <typename Block>
auto makeInMemoryStorageFactory(
    [[maybe_unused]] size_t maxBufferedBlocksPerChunk,
    [[maybe_unused]] bool releaseChunkOnConsumption = false) {
  return std::monostate{};
}
}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_INMEMORYBLOCKSTORAGE_H
