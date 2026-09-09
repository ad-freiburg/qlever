// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_BLOCKSINKPOLICY_H
#define QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_BLOCKSINKPOLICY_H

#include <cstddef>
#include <exception>
#include <type_traits>
#include <utility>

#include "backports/concepts.h"

// The output policy of the parallel block merge: the `SinkConcept` that the
// sink of a merge has to fulfill. It is the counterpart of the `InputConcept`
// (see `util/parallelBlockMerge/RunsInputPolicy.h`). For the terminology (runs,
// blocks, and chunks) see `util/parallelBlockMerge/ParallelBlockMerge.h`, which
// is the header to read first.
namespace ad_utility::parallelBlockMerge {

namespace detail {
// The completion handlers with which the `SinkConcept` below probes the
// asynchronous operations of a sink, so that the concept pins down their
// completion signatures as well.
//
// NOTE: These are named types and not lambdas, because a lambda may not appear
// in an unevaluated context in C++17 mode, which is exactly where the concept
// puts them.
struct SinkBoolHandler {
  void operator()([[maybe_unused]] std::exception_ptr exception,
                  [[maybe_unused]] bool keepGoing) const {}
};

// ___________________________________________________________________________
struct SinkVoidHandler {
  void operator()([[maybe_unused]] std::exception_ptr exception) const {}
};
}  // namespace detail

// The requirements of the `SinkConcept` below, see there for the documentation.
template <typename T, typename Block>
CPP_requires(
    SinkConcept_,
    requires(T& sink, size_t chunkIndex, Block block,
             std::exception_ptr exception)(
        // Poll whether the merge was stopped.
        ql::concepts::convertible_to<decltype(sink.stopRequested()), bool>,
        // Push a finished output block of a chunk, and announce that a chunk
        // has no further blocks.
        sink.asyncPush(chunkIndex, std::move(block), detail::SinkBoolHandler{}),
        sink.asyncFinishChunk(chunkIndex, detail::SinkBoolHandler{}),
        // Forward an exception of a chunk to the consumer, and stop the merge
        // from the consuming side.
        sink.asyncPushException(std::move(exception),
                                detail::SinkVoidHandler{}),
        sink.asyncAbort(detail::SinkVoidHandler{})));

// The output policy of the merge: the sink to which the merge writes its
// output blocks. The merge itself only ever *pushes* into a sink, one block at
// a time and tagged with the index of the chunk that the block belongs to (see
// `parallelBlockMergeToSink`); everything else is up to the sink. In particular
// the merge knows nothing about the order in which the blocks of the individual
// chunks are handed on to a consumer, about the buffering of the blocks, or
// about the back-pressure that bounds the memory consumption of the whole
// merge. That is what makes a sink a policy and not an implementation detail:
// a sink that turns the blocks back into a single sequential range and a sink
// that writes them to (say) several files in parallel plug into the very same
// merge.
//
// INTERFACE: All the operations whose name starts with `async` are ordinary
// Boost.Asio operations that take a completion token, so a caller may await
// them, attach a callback, obtain a `std::future`, or detach them, whatever
// fits. All of them may be initiated from any thread and any executor. The
// completion signature is `void(std::exception_ptr, bool)` for `asyncPush` and
// `asyncFinishChunk`, where the `bool` says whether the merge should keep
// going, and `void(std::exception_ptr)` for `asyncPushException` and
// `asyncAbort`, so a token such as `net::use_awaitable` rethrows on the
// executor of the caller.
//
// The operations in detail:
//
// * `bool stopRequested() const noexcept` — return whether the merge was
//   stopped, either by `asyncAbort` or by `asyncPushException`. This is the
//   only synchronous operation of a sink, and the merge polls it before it
//   merges the next output block, so that it does no superfluous work. It has
//   to be callable from any thread at any time.
//
// * `asyncPush(chunkIndex, block, token)` — push a finished output `block` of
//   the chunk with the given `chunkIndex`. It completes with `false` if the
//   merge was stopped, in which case the `block` may be dropped and the
//   producer of the chunk stops producing. A sink that buffers only a bounded
//   number of blocks per chunk suspends here until there is room again, which
//   is the back-pressure that bounds the memory consumption of the merge.
//
// * `asyncFinishChunk(chunkIndex, token)` — announce that no further block
//   will be pushed for the chunk with the given `chunkIndex`. The merge calls
//   this exactly once for every chunk that it dispatches at all, and on every
//   path: also for a chunk that has no output block at all, and also for a
//   chunk that was stopped while it was already running. A merge that was
//   stopped in contrast does not dispatch its remaining chunks at all, so those
//   chunks never announce anything; a sink may therefore only wait for all of
//   its chunks if the merge was not stopped.
//
// * `asyncPushException(exception, token)` — forward an `exception` of a
//   chunk. Only the first pushed exception has to be kept, and pushing an
//   exception also has to stop the merge, so that the remaining chunks do not
//   keep producing blocks that nobody wants any more.
//
// * `asyncAbort(token)` — stop the merge from the consuming side, so that no
//   producer is left suspended forever once the consumer is gone.
//
// PRECONDITION: At most one `asyncPush` or `asyncFinishChunk` of a *given*
// chunk is in flight at a time, because two concurrent ones could complete in
// either order and the order within the chunk would be lost. The merge honors
// this, because each of its chunks has a single producer that pushes the blocks
// of that chunk in a handler chain. Operations of *different* chunks in
// contrast do run concurrently, and so do the other two operations, so a sink
// has to synchronize its own state.
//
// IMPORTANT: A sink must not invoke a completion handler *inline*, that is from
// within the initiating function, but always via a `net::post` to the executor
// that is associated with the completion token. Two reasons: the producer of a
// chunk continues in the completion handler of its `asyncPush` (so an inline
// completion would let the stack grow with every output block), and it merges
// the next output block right there (which is ordinary blocking work that may
// even do I/O, so it must not run on a strand that serializes the sink).
//
// LIFETIME: A sink has to stay alive until the last operation on it is
// complete. The merge takes care of that (it holds the sink by a `shared_ptr`
// that outlives all of its tasks and handlers, see
// `detail::ParallelMergeState`), so a caller may safely drop its own
// `shared_ptr` to the sink at any time.
template <typename T, typename Block>
CPP_concept SinkConcept = CPP_requires_ref(SinkConcept_, T, Block);

namespace detail {
// The type of the sink that a sink factory creates, see
// `parallelBlockMergeToSink`.
template <typename SinkFactory>
using SinkFromFactoryT =
    typename std::invoke_result_t<SinkFactory&, size_t>::element_type;
}  // namespace detail

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_BLOCKSINKPOLICY_H
