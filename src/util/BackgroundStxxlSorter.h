//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_BACKGROUNDSTXXLSORTER_H
#define QLEVER_BACKGROUNDSTXXLSORTER_H

#include <future>
#include <stxxl/sorter>
#include "./Generator.h"

#include "./Exception.h"
#include "./Log.h"

namespace  ad_utility {
template<typename View>
cppcoro::generator<typename View::value_type> bufferedAsyncView(View view, uint64_t bufferSize) {
  using value_type = typename View::value_type;
  auto it = view.begin();
  auto end = view.end();
  auto getNextBlock = [&it, &end, bufferSize] {
    std::vector<value_type> buffer;
    buffer.reserve(bufferSize);
    size_t i;
    while (i < bufferSize && it != end) {
      buffer.push_back(*it);
      ++it;
      ++i;
    }
    return buffer;
  };

  auto buffer = getNextBlock();
  auto future = std::async(std::launch::async, getNextBlock);
  while (true) {
    for (auto& element : buffer) {
      co_yield element;
    }
    buffer = future.get();
    if (buffer.empty()) {
      co_return;
    }
    future = std::async(std::launch::async, getNextBlock);
  }
}

// This class has the same interface as a `stxxl::sorter` and performs the same
// functionality, with the following difference: All expensive operations
// (sorting or merging a block) are performed on a background thread such that
// the calls to `push()` (add triples before the sort) or `operator++` (iterate
// over the triples after the sort) return much faster.
template <typename ValueType, typename Comparator>
class BackgroundStxxlSorter {
 private:
  // The underlying sorter.
  stxxl::sorter<ValueType, Comparator> _sorter;
  // Buffer for a block of elements, which will be passed to the `_iterator`
  // (respectively: were retrieved from the `_iterator`) at once and
  // asynchronously.
  std::vector<ValueType> _buffer;
  // Wait for the asynchronous background operations.
  std::future<void> _sortInBackgroundFuture;
  std::future<std::vector<ValueType>> _mergeInBackgroundFuture;
  // The number of elements that the `_iterator` can sort in RAM.
  size_t _numElementsInRun;

  // Used in the output phase as an index into `_buffer`.
  size_t _outputIndex = 0;

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

  // Transition from the input phase, where new elements can be added via
  // `push()`, to the output phase, where `operator++`, `operator*` and
  // `empty()` can be called to retrieve elements in sorted order.

 public:
  size_t size() const { return _sorter.size(); }

  void clear() {
    if (_sortInBackgroundFuture.valid()) {
      _sortInBackgroundFuture.get();
    }
    if (_mergeInBackgroundFuture.valid()) {
      _mergeInBackgroundFuture.get();
    }

    _sorter.clear();
    _numElementsInRun = _sorter.num_els_in_run();
    _buffer.reserve(_numElementsInRun);
    _outputIndex = 0;
    _outputIndex = 0;
  }

  /// Return a lambda that takes a `ValueType` and calls `push` for that value.
  /// Note that `this` is captured by reference
  auto makePushCallback() {
    return [this](ValueType value) { push(std::move(value)); };
  }

  [[nodiscard]] cppcoro::generator<value_type> sort() {
    sortImpl();
    for (; !empty(); operator++()) {
      auto value = operator*();
      co_yield value;
    }
  }

 private:
  bool empty() {
    if (_outputIndex >= _buffer.size()) {
      refillOutputBuffer();
    }
    return (_outputIndex >= _buffer.size());
  }

  void sortImpl() {
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
    _buffer.clear();
    _buffer.reserve(_numElementsInRun);
    refillOutputBuffer();
  }

  decltype(auto) operator*() const { return _buffer[_outputIndex]; }

  void refillOutputBuffer() {
    _buffer.clear();
    _outputIndex = 0;

    auto getNextBlock = [this] {
      std::vector<ValueType> buffer;
      buffer.reserve(_numElementsInRun);
      for (size_t i = 0; i < _numElementsInRun; ++i) {
        if (_sorter.empty()) {
          break;
        }
        buffer.push_back(*_sorter);
        ++_sorter;
      }
      return buffer;
    };

    if (!_mergeInBackgroundFuture.valid() && !_sorter.empty()) {
      // If we have reached here, this is the first time we fill the buffer,
      // Fill the buffer synchronously and immediately start the next
      // asynchronous operation.
      _buffer = getNextBlock();
    }

    if (_mergeInBackgroundFuture.valid()) {
      _buffer = _mergeInBackgroundFuture.get();
    }

    if (_sorter.empty()) {
      return;
    }
    _mergeInBackgroundFuture = std::async(std::launch::async, getNextBlock);
  }

  BackgroundStxxlSorter& operator++() {
    _outputIndex++;
    if (_outputIndex >= _buffer.size()) {
      refillOutputBuffer();
    }
    return *this;
  }
};

template<typename SortedView>
cppcoro::generator<typename SortedView::value_type> uniqueView(SortedView view) {
  auto it = view.begin();
  if (it == view.end()) {
    co_return;
  }
  auto previousValue = std::move(*it);
  auto previousValueCopy = previousValue;
  co_yield previousValueCopy;
  ++it;

  for (; it != view.end(); ++it) {
    if (*it != previousValue) {
      previousValue = std::move(*it);
      previousValueCopy = previousValue;
      co_yield previousValueCopy;
    }
  }
}



}  // namespace ad_utility

#endif  // QLEVER_BACKGROUNDSTXXLSORTER_H
