// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_PARALLELBLOCKMERGE_INMEMORYBLOCKSTORAGE_H
#define QLEVER_TEST_PARALLELBLOCKMERGE_INMEMORYBLOCKSTORAGE_H

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
// NOTE: This lives in `test/` because the real merges spill their blocks to
// disk; move it to `src/` as soon as there is a production user for it.
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
  // consumer may hold on to one across a suspension, while `eraseChunk` removes
  // the map entry as soon as that chunk is done.
  using SharedBlockChannel = std::shared_ptr<BlockChannel>;

 private:
  Strand strand_;
  size_t maxBufferedBlocksPerChunk_;
  HashMap<size_t, SharedBlockChannel> chunks_;
  // Set by `cancelAll`, only to check the precondition that no operation is
  // initiated afterwards. That check matters, because the teardown of the sink
  // is only airtight as long as no channel is created after the cancellation,
  // see the class comment of `InOrderBlockSink`.
  bool wasCancelled_ = false;

 public:
  // Construct from the `strand` that all the operations of this storage are
  // confined to, and the number of blocks that are buffered per chunk (which
  // has to be at least one).
  InMemoryBlockStorage(Strand strand, size_t maxBufferedBlocksPerChunk)
      : strand_{std::move(strand)},
        maxBufferedBlocksPerChunk_{maxBufferedBlocksPerChunk} {
    AD_CONTRACT_CHECK(maxBufferedBlocksPerChunk > 0);
  }

  // Store `block` in the channel of the chunk, see
  // `BlockStorageConcept::storeBlock`.
  template <typename CompletionToken>
  auto storeBlock(size_t chunkIndex, OptionalBlock block,
                  CompletionToken&& completionToken) {
    return net::async_initiate<CompletionToken, void(std::exception_ptr, bool)>(
        [this, chunkIndex, block = std::move(block)](auto handler) mutable {
          SharedBlockChannel channel;
          try {
            channel = getOrCreateChannel(chunkIndex);
          } catch (...) {
            std::move(handler)(std::current_exception(), false);
            return;
          }
          channel->async_send(boost::system::error_code{}, std::move(block),
                              [handler = std::move(handler)](
                                  boost::system::error_code errorCode) mutable {
                                // NOTE: This runs on `strand_`, because the
                                // channel was created with `strand_` as its
                                // executor and this handler has no executor of
                                // its own that would override that. The only
                                // error that can occur is that the channel was
                                // cancelled, see `cancelAll`.
                                std::move(handler)(std::exception_ptr{},
                                                   !errorCode);
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
          SharedBlockChannel channel;
          try {
            channel = getOrCreateChannel(chunkIndex);
          } catch (...) {
            std::move(handler)(std::current_exception(), GetResult{});
            return;
          }
          channel->async_receive([handler = std::move(handler)](
                                     boost::system::error_code errorCode,
                                     OptionalBlock block) mutable {
            // NOTE: This runs on `strand_`, see `storeBlock` above.
            std::move(handler)(
                std::exception_ptr{},
                errorCode ? GetResult{} : GetResult{std::move(block)});
          });
        },
        completionToken);
  }

  // Destroy the channel of the chunk, see `BlockStorageConcept::eraseChunk`.
  void eraseChunk(size_t chunkIndex) noexcept {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    // NOTE: Erasing the entry is safe even if the producer of that chunk still
    // holds the channel, because the channels are shared.
    chunks_.erase(chunkIndex);
  }

  // Cancel all the channels, see `BlockStorageConcept::cancelAll`.
  void cancelAll() noexcept {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    wasCancelled_ = true;
    // Wake up everybody who is currently suspended. NOTE: There is no need to
    // run this sweep more than once, because no channel is ever created
    // afterwards, so a later sweep would find nothing new (see the
    // PRECONDITIONS of the `BlockStorageConcept` and the class comment of
    // `InOrderBlockSink`).
    //
    // IMPORTANT: `cancel()` is also the *only* primitive that may be used here.
    // `close()` alone does not wake an operation that is already suspended, and
    // a `cancel()` after a `close()` is outright undefined behavior, because
    // `cancel()` tells a suspended send from a suspended receive by the
    // internal send state that `close()` overwrites.
    for (const auto& chunk : chunks_) {
      chunk.second->cancel();
    }
  }

 private:
  // Return the channel of the chunk with the given `chunkIndex`, creating it if
  // it does not exist yet.
  SharedBlockChannel getOrCreateChannel(size_t chunkIndex) {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    AD_CORRECTNESS_CHECK(!wasCancelled_);
    auto& channel = chunks_[chunkIndex];
    if (channel == nullptr) {
      // NOTE: The channel lives on the strand as well, so that the operations
      // on it are dispatched through the strand no matter which executor the
      // initiating caller reports.
      channel =
          std::make_shared<BlockChannel>(strand_, maxBufferedBlocksPerChunk_);
    }
    return channel;
  }
};

// A factory for an `InMemoryBlockStorage` that buffers at most
// `maxBufferedBlocksPerChunk` blocks per chunk, for the constructor of
// `InOrderBlockSink`.
template <typename Block>
auto makeInMemoryStorageFactory(size_t maxBufferedBlocksPerChunk) {
  return [maxBufferedBlocksPerChunk](const Strand& strand) {
    return InMemoryBlockStorage<Block>{strand, maxBufferedBlocksPerChunk};
  };
}

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_TEST_PARALLELBLOCKMERGE_INMEMORYBLOCKSTORAGE_H
