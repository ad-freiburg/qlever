//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_BACKGROUNDSTXXLSORTER_H
#define QLEVER_BACKGROUNDSTXXLSORTER_H

#include <future>
#include <stxxl/sorter>

#include "./Exception.h"
#include "./Log.h"

namespace ad_utility {

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
  void sort() {
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

  // Was `sort()` already called?
  [[nodiscard]] bool wasSortCalled() const { return _sortWasCalled; }

  struct IteratorEnd {};
  struct Iterator {
   private:
    BackgroundStxxlSorter* _sorter;
    explicit Iterator(BackgroundStxxlSorter* sorter) : _sorter{sorter} {}
    friend class BackgroundStxxlSorter;

   public:
    decltype(auto) operator++() { return ++(*_sorter); }
    decltype(auto) operator*() { return **_sorter; }
    bool operator==(IteratorEnd) { return _sorter->empty(); }
  };

  /// After calling `sort()` this class is input-iterable
  Iterator begin() {
    AD_CHECK(wasSortCalled());
    return Iterator{this};
  }
  IteratorEnd end() {
    AD_CHECK(wasSortCalled());
    return {};
  }

 private:
  bool empty() {
    if (_outputIndex >= _buffer.size()) {
      refillOutputBuffer();
    }
    return (_outputIndex >= _buffer.size());
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

/// Takes a `stxxl::sorter` or a `StxxlBackgroundSorter` an filters out
/// consecutive duplicates.
template <typename InputSorter>
class StxxlUniqueSorter {
 private:
  typename InputSorter::Iterator _iterator;
  typename InputSorter::IteratorEnd _end;

 public:
  /// The `inputSorter` must be a `stxxl::sorter` or a `StxxlBackgroundSorter`
  /// for which `sort` has already been called.
  explicit StxxlUniqueSorter(InputSorter& inputSorter)
      : _iterator(inputSorter.begin()), _end(inputSorter.end()) {}

  /// This class is input-iterable, just like the underlying sorters.
  struct IteratorEnd {};
  friend class Iterator;
  class Iterator {
   private:
    StxxlUniqueSorter* _sorter;

   public:
    explicit Iterator(StxxlUniqueSorter* sorter) : _sorter{sorter} {}
    decltype(auto) operator++() { return ++(*_sorter); }
    decltype(auto) operator*() { return *_sorter->_iterator; }
    bool operator==(IteratorEnd) { return _sorter->_iterator == _sorter->_end; }
  };
  Iterator begin() { return Iterator{this}; }
  IteratorEnd end() { return {}; }

 private:
  StxxlUniqueSorter& operator++() {
    auto previousValue = *_iterator;
    ++_iterator;
    while (_iterator != _end && previousValue == *_iterator) {
      ++_iterator;
    }
    return *this;
  }
};
}  // namespace ad_utility

#endif  // QLEVER_BACKGROUNDSTXXLSORTER_H
