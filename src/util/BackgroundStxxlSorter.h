//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_BACKGROUNDSTXXLSORTER_H
#define QLEVER_BACKGROUNDSTXXLSORTER_H

#include <future>
#include <stxxl/sorter>

#include "./Exception.h"
#include "./Generator.h"
#include "./Log.h"
#include "./Views.h"

namespace ad_utility {

// This class has a similar interface to a `stxxl::sorter` and performs the same
// functionality, with the following difference: All expensive operations
// (sorting or merging a block) are performed on a background thread such that
// the calls to `push()` (add triples before the sort) and the usage of the
// view of the sorted elements obtained by `sort()` become much faster.
template <typename ValueType, typename Comparator>
class BackgroundStxxlSorter {
 private:
  // The underlying sorter.
  stxxl::sorter<ValueType, Comparator> _sorter;
  // Buffer for a block of elements that will be passed to the `_sorter`
  // on a background thread .
  std::vector<ValueType> _buffer;
  // Wait for the asynchronous background operations.
  std::future<void> _sortInBackgroundFuture;
  // The number of elements that the `_iterator` can sort in RAM.
  size_t _numElementsInRun;
  // Was the `sort()` function already called
  bool _sortWasCalled = false;

 public:
  using value_type = ValueType;

  // The BackgroundStxxlSorter will actually use 3 * memoryForStxxl bytes plus
  // some overhead.
  explicit BackgroundStxxlSorter(size_t memoryForStxxl,
                                 Comparator comparator = Comparator())
      : _sorter{comparator, memoryForStxxl} {
    _numElementsInRun = _sorter.num_els_in_run();
    _buffer.reserve(_numElementsInRun);
  }

  // In the input phase (before calling `sort()`), add another value to the
  // to-be-sorted input.
  void push(ValueType value) {
    _buffer.push_back(std::move(value));
    if (_buffer.size() < _numElementsInRun) {
      return;
    }

    // We have filled a block. First wait for the sorting of the last block to
    // finish, and then start sorting the new block in the background.
    if (_sortInBackgroundFuture.valid()) {
      _sortInBackgroundFuture.get();
    }
    auto sortRunInBackground = [this, buffer = std::move(_buffer)]() {
      for (const auto& element : buffer) {
        // TODO<joka921> extend the `stxxl::sorter` interface s.t. we can push a
        // whole block at once.
        _sorter.push(element);
      }
    };
    _sortInBackgroundFuture =
        std::async(std::launch::async, std::move(sortRunInBackground));
    _buffer.clear();
    _buffer.reserve(_numElementsInRun);
  }

  size_t size() const { return _sorter.size(); }

  /// Clear the underlying sorter and all buffers and reset to the input state.
  /// If a view was obtained via `sort()`, this generator becomes invalid
  /// and using it results in undefined behavior.
  void clear() {
    if (_sortInBackgroundFuture.valid()) {
      _sortInBackgroundFuture.get();
    }
    _sorter.clear();
    _numElementsInRun = _sorter.num_els_in_run();
    // Deallocate memory.
    _buffer = decltype(_buffer){};
    _sortWasCalled = false;
  }

  /// Return a lambda that takes a `ValueType` and calls `push` for that value.
  /// Note that `this` is captured by reference
  auto makePushCallback() {
    return [this](ValueType value) { push(std::move(value)); };
  }

  /// Transition from the input phase, where `push()` may be called, to the
  /// output phase and return a generator that yields the sorted elements. This
  /// function may be called exactly once.
  [[nodiscard]] cppcoro::generator<value_type> sortedView() {
    setupSort();
    return bufferedAsyncView(outputGeneratorImpl(), _numElementsInRun);
  }

 private:
  // Transition from the input to the output state.
  void setupSort() {
    AD_CHECK(!_sortWasCalled);
    _sortWasCalled = true;
    // First sort all remaining elements from the input phase.
    if (_sortInBackgroundFuture.valid()) {
      _sortInBackgroundFuture.get();
    }
    for (const auto& el : _buffer) {
      _sorter.push(el);
    }
    _sorter.sort();
    // Deallocate memory for `_buffer`, the output buffering is handled via a
    // `bufferedAsyncView` which owns its own buffer.
    _buffer = decltype(_buffer){};
  }

  // Yield all elements of the underlying sorter. Only valid after `setupSort`
  // was called.
  cppcoro::generator<value_type> outputGeneratorImpl() {
    for (; !_sorter.empty(); ++_sorter) {
      auto value = *_sorter;
      co_yield value;
    }
  }
};

}  // namespace ad_utility

#endif  // QLEVER_BACKGROUNDSTXXLSORTER_H
