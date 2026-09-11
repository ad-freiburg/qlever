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

// The intermediate storage of the output blocks of the parallel block merge:
// the `BlockStorageConcept` that such a storage has to fulfill. Its only user
// is the `InOrderBlockSink` (see `util/parallelBlockMerge/InOrderBlockSink.h`),
// which is implemented with coroutines, so this whole header is only available
// in C++20 mode and empty when `QLEVER_REDUCED_FEATURE_SET_FOR_CPP17` is set,
// see `util/parallelBlockMerge/ParallelMergeState.h`.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/strand.hpp>
#include <cstddef>
#include <exception>
#include <optional>
#include <utility>

namespace ad_utility::parallelBlockMerge {

namespace net = boost::asio;

// The executor that a storage and the sink that owns it confine themselves to,
// see the CONTRACT below and the STRAND CONFINEMENT note of
// `InOrderBlockSink`.
using Strand = net::strand<net::any_io_executor>;

// The value that a storage stores and retrieves. An empty `OptionalBlock` is
// the end-of-chunk sentinel.
//
// NOTE: `Block` need not be default-constructible (an `IdTable` for example is
// not), which is why the blocks travel as an `std::optional` throughout. That
// `std::optional` doubles as the end-of-chunk sentinel.
template <typename Block>
using OptionalBlock = std::optional<Block>;

// The result of `getBlock`: an empty `GetResult` means that the operation was
// cancelled (see `cancelAll`), whereas an empty `OptionalBlock` inside it is
// the end-of-chunk sentinel.
template <typename Block>
using GetResult = std::optional<OptionalBlock<Block>>;

// The place where the `InOrderBlockSink` keeps the output blocks between the
// producer that has finished a block and the consumer that will eventually
// yield it. A storage holds one independent FIFO queue per chunk, and a queue
// transports the blocks of that chunk plus a single end-of-chunk sentinel (an
// empty `OptionalBlock`) that terminates it.
//
// The storage is thereby responsible for three things at once: the buffering,
// the FIFO order within a chunk, and the rendezvous between the producer and
// the consumer of a chunk. That last point is why storing and retrieving are
// *asynchronous* operations: a consumer that asks for a block that has not been
// produced yet has to be able to wait, and an implementation that bounds its
// buffer also makes a producer wait once that bound is reached. Which of the
// two happens is exactly the difference between the implementations: a storage
// that keeps the blocks in memory applies back-pressure, whereas one that
// spills them to disk lets the producer run ahead.
//
// The operations in detail:
//
// * `storeBlock(chunkIndex, block, token)` — append `block` (or the
//   end-of-chunk sentinel) to the queue of the chunk with the given
//   `chunkIndex`, creating that queue if it does not exist yet. Complete with
//   `false` if the value was *not* stored, in which case the caller has to drop
//   it. An implementation reports `false` whenever nothing will ever consume
//   that value anymore, and the exact conditions under which that happens are
//   up to it: a storage may report it when it was cancelled while the operation
//   was in flight, and one that spills the blocks to disk may also report it
//   for a chunk that was erased in the meantime.
//
// * `getBlock(chunkIndex, token)` — remove the front of the queue of the chunk
//   with the given `chunkIndex` and complete with it, waiting until that queue
//   is non-empty. Complete with an empty `GetResult` if the storage was
//   cancelled while the operation was in flight. A queue that does not exist
//   yet is created (and is then simply empty), because the consumer of a chunk
//   may well be faster than its producer.
//
// * `eraseChunk(chunkIndex) noexcept` — drop everything that belongs to the
//   chunk with the given `chunkIndex`. The sink calls this once the
//   end-of-chunk sentinel of that chunk was retrieved, so that the memory (and
//   the disk space) that a storage occupies is proportional to the number of
//   chunks that are in flight and not to their total number.
//
// * `cancelAll() noexcept` — wake up every operation that is currently
//   suspended, completing it as "not stored" respectively "cancelled", and
//   remember that the storage was cancelled. After this, no further operation
//   may be initiated, see the PRECONDITIONS below. An operation whose blocking
//   work has already begun (such as a write to disk) does not have to be
//   cancelled and may run to completion; the point of this function is only
//   that no caller is left suspended forever.
//
// INTERFACE: The two asynchronous operations are ordinary Boost.Asio operations
// that take a completion token, so the sink may simply `co_await` them. Their
// completion signature is `void(std::exception_ptr, bool)` respectively
// `void(std::exception_ptr, GetResult<Block>)`, so a token such as
// `net::use_awaitable` rethrows a failure on the executor of the caller.
//
// CONTRACT: All the operations of a storage
// * run on the single executor that the storage was constructed with (which is
//   the strand of the sink, see `InOrderBlockSink`),
// * complete their token exactly once, on that same executor,
// * and may throw only *before* they have consumed their handler, in which case
//   the caller is responsible for completing its own operation (the sink does
//   just that). An implementation therefore has to report a failure of its own
//   bookkeeping through the completion and not by throwing.
//
// PRECONDITIONS: For each chunk, at most one `storeBlock` and at most one
// `getBlock` may be in flight at any time, because two concurrent operations on
// the same queue could complete in either order and the FIFO order would be
// lost. Both hold for the sink: a chunk has a single producer that pushes its
// blocks one after the other, and the sink has a single consumer. Furthermore
// no operation may be initiated anymore once `cancelAll` was called.
template <typename T, typename Block>
concept BlockStorageConcept =
    requires(T& storage, size_t chunkIndex, OptionalBlock<Block> block) {
      // Append a block (or the end-of-chunk sentinel) to the queue of a chunk.
      storage.storeBlock(chunkIndex, std::move(block),
                         [](std::exception_ptr, bool) {});
      // Remove the front of the queue of a chunk and complete with it.
      storage.getBlock(chunkIndex, [](std::exception_ptr, GetResult<Block>) {});
      // Drop everything that belongs to a chunk.
      { storage.eraseChunk(chunkIndex) } noexcept;
      // Wake up every operation that is currently suspended.
      { storage.cancelAll() } noexcept;
    };

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_BLOCKSTORAGE_H
