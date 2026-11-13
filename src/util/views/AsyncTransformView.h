// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_VIEWS_ASYNCTRANSFORMVIEW_H
#define QLEVER_SRC_UTIL_VIEWS_ASYNCTRANSFORMVIEW_H

#include <boost/asio/post.hpp>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <type_traits>
#include <utility>

#include "backports/algorithm.h"
#include "backports/asio.h"
#include "backports/atomic_flag.h"
#include "util/Exception.h"
#include "util/Iterators.h"
#include "util/Math.h"
#include "util/NoCopyNoMove.h"

namespace ad_utility {

// An input range that yields `transformation(element)` for each `element` of
// the `Range`, in the original order of the elements, like
// `ql::views::transform`. The difference is that up to `maxNumElementsInFlight`
// elements are read ahead and transformed on an `executor`, concurrently with
// the consumer of this range, which is still processing the previously yielded
// elements. This pays off if the `transformation` is expensive and independent
// for each element. In contrast to `streams::runStreamAsync`, which runs the
// whole range sequentially on a single background thread, the
// `transformation` of different elements runs in parallel.
//
// The elements that are read ahead are not handed to the `executor` one by
// one. Instead, at most `maxNumRunners` tasks ("runners") on the `executor`
// transform them one after the other, in their order. This keeps the elements
// of one range (for example the blocks of one index scan) on few threads, each
// of which works through consecutive elements, so that their data stays in the
// caches of that thread's core. Spreading them over all the threads of a busy
// shared pool, interleaved with the elements of other ranges, lowers the
// throughput of all of them considerably. When the consumer needs an element
// that no runner has started yet, it transforms that element itself instead of
// waiting for it, so the consumer never waits for a task that is still queued
// behind the tasks of other users of the `executor`.
//
// The elements of the `Range` are moved into the tasks that transform them,
// and each result is moved out of its task when it is yielded, so both the
// elements and the results may be move-only types. The `transformation` is
// invoked as a `const` callable from several threads at the same time, so it
// has to be thread-safe in that sense.
//
// NOTE 1: Up to `maxNumElementsInFlight` elements (or their results) are held
// in memory at the same time, in addition to the result that the consumer
// holds.
//
// NOTE 2: An exception that is thrown by the `transformation` is rethrown when
// the consumer reaches the result of the respective element. An exception
// that is thrown by the `Range` itself is thrown directly to the consumer.
//
// NOTE 3: When this range is destroyed, the elements that are still pending
// and that no runner has started yet are dropped without being transformed.
// The destructor waits for the transformations that are already running.
//
// IMPORTANT: The consumer blocks while it waits for an element that a runner
// is currently transforming, so this range must not be consumed from a thread
// of the `executor`. The execution context behind the `executor` has to
// outlive this range.
//
// NOTE 4: This range is neither copyable nor movable. Wrap it into an
// `InputRangeTypeErased` (via `std::make_unique`) if it has to be passed
// around.
//
// NOTE 5: The results are stored and yielded by value (`std::decay_t` of the
// return type of the `transformation`). In particular, a `transformation` that
// returns a reference to its argument (like `std::identity`) is fine, because
// the result is moved out of the element before the element is destroyed.
template <typename Range, typename Transformation>
class AsyncTransformView
    : public InputRangeFromGet<std::decay_t<std::invoke_result_t<
          const Transformation&, ql::ranges::range_value_t<Range>>>>,
      public NoCopyNoMove {
 public:
  using Element = ql::ranges::range_value_t<Range>;
  using Result =
      std::decay_t<std::invoke_result_t<const Transformation&, Element>>;
  static_assert(!std::is_void_v<Result>,
                "The transformation of an `AsyncTransformView` must return a "
                "value");

 private:
  Range range_;
  // The iterator into the `range_`, which is only obtained on the first call to
  // `get()`, because for some ranges (e.g. an `InputRangeFromGet`) `begin()`
  // already reads the first element.
  std::optional<ql::ranges::iterator_t<Range>> it_;
  size_t maxNumElementsInFlight_;

  // The state of one element that has been read from the `range_` but not yet
  // yielded. It is transformed exactly once: by a runner (see below) or, if no
  // runner has started on it when the consumer needs it, by the consumer.
  struct Slot {
    // Set by whoever transforms the element: a runner or the consumer.
    ql::atomic_flag claimed_;
    // Transforms the element with the transformation that is passed to it.
    std::packaged_task<Result(const Transformation&)> task_;

    explicit Slot(Element element)
        : task_{[element = std::move(element)](
                    const Transformation& transformation) mutable {
            // The task is stored in the state shared with the future, so the
            // element is moved into a temporary that is freed right after the
            // transformation.
            return std::invoke(transformation, Element(std::move(element)));
          }} {}

    // Transform the element, unless the slot was already claimed.
    void runIfUnclaimed(const Transformation& transformation) {
      if (!claimed_.test_and_set()) {
        task_(transformation);
      }
    }
  };

  // The state that is shared with the runners. A runner is a task on the
  // `executor_` that transforms the queued elements one after the other, in
  // their order, until none is left. There are at most `maxNumRunners_` of
  // them, so the elements of this range are processed by few threads, each of
  // which works through consecutive elements (which keeps their data and the
  // state of the transformation in the caches of that thread's core), instead
  // of being spread over all the threads of the `executor_`.
  //
  // NOTE: A runner that was posted but not yet started may outlive the view.
  // The destructor of the view therefore empties the queue and waits until no
  // runner holds a slot, so that nothing that refers to resources of the
  // caller (elements, results, or the transformation) survives the view.
  struct Shared {
    // Owned by the view. A runner only uses it while it holds a slot.
    const Transformation& transformation_;
    ql::any_io_executor executor_;
    size_t maxNumRunners_;
    // One more runner is started per this many queued slots.
    size_t numSlotsPerRunner_;

    std::mutex mutex_;
    // Notified when a runner releases its slot.
    std::condition_variable slotReleased_;
    // The members below are guarded by `mutex_`.
    std::deque<std::shared_ptr<Slot>> todo_;
    size_t numRunners_ = 0;
    // The number of runners that hold a slot (from popping it until releasing
    // it).
    size_t numBusyRunners_ = 0;

    Shared(const Transformation& transformation, ql::any_io_executor executor,
           size_t maxNumElementsInFlight, size_t maxNumRunners)
        : transformation_{transformation},
          executor_{std::move(executor)},
          maxNumRunners_{maxNumRunners} {
      AD_CONTRACT_CHECK(maxNumElementsInFlight > 0);
      AD_CONTRACT_CHECK(maxNumRunners > 0);
      numSlotsPerRunner_ =
          std::max<size_t>(1, maxNumElementsInFlight / maxNumRunners);
    }

    // Queue the `slots` and return the number of runners that the caller has
    // to start (they are already counted in `numRunners_`). The number of
    // runners is computed once for all the `slots`, because a runner may pop
    // a slot before the next one is queued.
    template <typename Slots>
    size_t enqueue(const Slots& slots) {
      std::lock_guard lock{mutex_};
      ql::ranges::copy(slots, std::back_inserter(todo_));
      size_t wanted =
          std::min(maxNumRunners_, ceilDiv(todo_.size(), numSlotsPerRunner_));
      size_t numNewRunners = wanted - std::min(wanted, numRunners_);
      numRunners_ += numNewRunners;
      return numNewRunners;
    }

    // Pop the next queued slot, which the calling runner then holds. If there
    // is none, retire the calling runner and return `nullptr`.
    std::shared_ptr<Slot> popOrRetire() {
      std::lock_guard lock{mutex_};
      if (todo_.empty()) {
        --numRunners_;
        return nullptr;
      }
      ++numBusyRunners_;
      auto slot = std::move(todo_.front());
      todo_.pop_front();
      return slot;
    }

    // Release the `slot` that the calling runner holds.
    void release(std::shared_ptr<Slot> slot) noexcept {
      slot.reset();
      std::lock_guard lock{mutex_};
      --numBusyRunners_;
      slotReleased_.notify_all();
    }

    // The body of a runner. It must not throw, because the destructor of the
    // view would then wait forever for the runner to release its slot. The
    // transformation can't throw here, because the `task_` stores its
    // exceptions in the future.
    void run() noexcept {
      while (auto slot = popOrRetire()) {
        slot->runIfUnclaimed(transformation_);
        release(std::move(slot));
      }
    }

    // Remove all the queued slots and wait until no runner holds a slot.
    void clearAndWaitUntilIdle() {
      std::unique_lock lock{mutex_};
      todo_.clear();
      slotReleased_.wait(lock, [this]() { return numBusyRunners_ == 0; });
    }
  };

  // A slot as seen by the consumer, together with the future of its result.
  struct Pending {
    std::shared_ptr<Slot> slot_;
    std::future<Result> future_;

    // NOTE: The future has to be obtained before the slot is queued, because
    // `get_future` must not run concurrently with the task itself.
    explicit Pending(std::shared_ptr<Slot> slot)
        : slot_{std::move(slot)}, future_{slot_->task_.get_future()} {}

    // Return the result, and transform the element right here if no runner has
    // started on it yet.
    Result get(const Transformation& transformation) {
      slot_->runIfUnclaimed(transformation);
      return future_.get();
    }
  };

  [[no_unique_address]] Transformation transformation_;
  std::shared_ptr<Shared> shared_;
  std::deque<Pending> pending_;

 public:
  // The default number of runners, which is the number of threads that a lazy
  // index scan had before its blocks were read on a shared thread pool.
  static constexpr size_t defaultMaxNumRunners = 2;

  // Construct from the `range` (which is stored by value, pass a
  // `ql::ranges::ref_view` to not take ownership), the `transformation`, the
  // maximal number of elements that are read ahead (which has to be at least
  // one), and the `executor` on which the transformation is run. At most
  // `maxNumRunners` threads of the `executor` work on the elements of this
  // range at the same time.
  AsyncTransformView(Range range, Transformation transformation,
                     size_t maxNumElementsInFlight,
                     ql::any_io_executor executor,
                     size_t maxNumRunners = defaultMaxNumRunners)
      : range_{std::move(range)},
        maxNumElementsInFlight_{maxNumElementsInFlight},
        transformation_{std::move(transformation)},
        shared_{std::make_shared<Shared>(transformation_, std::move(executor),
                                         maxNumElementsInFlight,
                                         maxNumRunners)} {}

  // Elements that no runner has started yet are dropped, and the running ones
  // are waited for (they use resources of the caller).
  ~AsyncTransformView() { shared_->clearAndWaitUntilIdle(); }

  // Return the result for the next element, or `std::nullopt` once all the
  // elements have been yielded.
  std::optional<Result> get() override {
    readAhead();
    if (pending_.empty()) {
      return std::nullopt;
    }
    // NOTE: The entry is popped before `get()` is called, so that an exception
    // that `get()` rethrows doesn't leave an invalid future behind.
    auto pending = std::move(pending_.front());
    pending_.pop_front();
    return pending.get(transformation_);
  }

 private:
  // Read elements from the `range_` until `maxNumElementsInFlight_` elements
  // are pending or the `range_` is exhausted, and make sure that enough
  // runners work on them.
  void readAhead() {
    if (!it_.has_value()) {
      it_ = ql::ranges::begin(range_);
    }
    auto& it = it_.value();
    size_t numPendingBefore = pending_.size();
    while (pending_.size() < maxNumElementsInFlight_ &&
           it != ql::ranges::end(range_)) {
      pending_.emplace_back(std::make_shared<Slot>(std::move(*it)));
      ++it;
    }
    size_t numNewRunners =
        shared_->enqueue(pending_ | ql::views::drop(numPendingBefore) |
                         ql::views::transform(&Pending::slot_));
    for (size_t i = 0; i < numNewRunners; ++i) {
      boost::asio::post(shared_->executor_,
                        [shared = shared_]() { shared->run(); });
    }
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_VIEWS_ASYNCTRANSFORMVIEW_H
