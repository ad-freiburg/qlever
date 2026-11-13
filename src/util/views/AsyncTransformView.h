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

#include <deque>
#include <functional>
#include <future>
#include <optional>
#include <type_traits>
#include <utility>

#include "backports/algorithm.h"
#include "backports/asio.h"
#include "util/Exception.h"
#include "util/Iterators.h"
#include "util/NoCopyNoMove.h"
#include "util/TaskQueueOnExecutor.h"

namespace ad_utility {

// An input range that yields `transformation(element)` for each `element` of
// the `Range`, in the original order of the elements, like
// `ql::views::transform`. The difference is that the `transformation` is
// applied on an `executor`, concurrently for up to `maxNumElementsInFlight`
// elements, and concurrently with the consumer of this range, which is still
// processing the previously yielded elements. This pays off if the
// `transformation` is expensive and independent for each element. In contrast
// to `streams::runStreamAsync`, which runs the whole range sequentially on a
// single background thread, the `transformation` of different elements runs
// in parallel.
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
// IMPORTANT: The consumer blocks while it waits for the next result, and while
// the maximal number of elements is in flight, so this range must not be
// consumed from a thread of the `executor` (see `TaskQueueOnExecutor` for
// details). The execution context behind the `executor` has to outlive this
// range.
//
// NOTE 3: This range is neither copyable nor movable, because the running tasks
// refer to its `transformation_`. Wrap it into an `InputRangeTypeErased` (via
// `std::make_unique`) if it has to be passed around.
//
// NOTE 4: The results are stored and yielded by value (`std::decay_t` of the
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
  [[no_unique_address]] Transformation transformation_;
  size_t maxNumElementsInFlight_;
  // The results of the elements that have been read from the `range_`, but not
  // yet yielded, in the order of the elements. Some of them may still be in
  // the process of being computed.
  std::deque<std::future<Result>> pending_;

  // NOTE: This member is deliberately declared last, so that its destructor
  // (which waits for all pending tasks) runs before the `transformation_`, to
  // which those tasks refer, is destroyed.
  TaskQueueOnExecutor queue_;

 public:
  // Construct from the `range` (which is stored by value, pass a
  // `ql::ranges::ref_view` to not take ownership), the `transformation`, the
  // maximal number of elements that are transformed concurrently (which has to
  // be at least one), and the `executor` on which the transformation is run.
  // No element of the `range` is read before the first element of this range
  // is requested.
  AsyncTransformView(Range range, Transformation transformation,
                     size_t maxNumElementsInFlight,
                     ql::any_io_executor executor)
      : range_{std::move(range)},
        transformation_{std::move(transformation)},
        maxNumElementsInFlight_{maxNumElementsInFlight},
        queue_{std::move(executor), maxNumElementsInFlight,
               "The tasks of an `AsyncTransformView`"} {}

  // Return the result for the next element, or `std::nullopt` once all the
  // elements have been yielded.
  std::optional<Result> get() override {
    readAhead();
    if (pending_.empty()) {
      return std::nullopt;
    }
    // NOTE: The entry is popped before `get()` is called, so that an exception
    // that `get()` rethrows doesn't leave an invalid future behind.
    auto future = std::move(pending_.front());
    pending_.pop_front();
    return future.get();
  }

 private:
  // Read elements from the `range_` and hand them to the `executor` until
  // `maxNumElementsInFlight_` elements are pending or the `range_` is
  // exhausted.
  //
  // TODO<joka921> The waiting for the next element of the `range_` (which
  // for example comes from an external sorter) still happens on the consuming
  // thread. As that thread currently does nothing else while it waits, this
  // is not a bottleneck, but it could be moved to the `executor` as well.
  void readAhead() {
    if (!it_.has_value()) {
      it_ = ql::ranges::begin(range_);
    }
    auto& it = it_.value();
    while (pending_.size() < maxNumElementsInFlight_ &&
           it != ql::ranges::end(range_)) {
      Element element = std::move(*it);
      ++it;
      pending_.push_back(
          queue_.submit([&transformation = std::as_const(transformation_),
                         element = std::move(element)]() mutable -> Result {
            return std::invoke(transformation, std::move(element));
          }));
    }
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_VIEWS_ASYNCTRANSFORMVIEW_H
