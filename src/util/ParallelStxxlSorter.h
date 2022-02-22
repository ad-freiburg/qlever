//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_PARALLELSTXXLSORTER_H
#define QLEVER_PARALLELSTXXLSORTER_H

#include <future>
#include <stxxl/sorter>

#include "./Log.h"

namespace ad_utility {

// This class has the same interface as a `stxxl::sorter` and performs the same
// functionality, with the following difference: All expensive operations
// (sorting or merging a block) are performed on a background thread, s.t. the
// calls to `push()` (input state) or `operator++` (output state) return much
// faster.
template <typename ValueType, typename Comparator>
class BackgroundStxxlSorter {
 private:
  // The underlying sorter
  stxxl::sorter<ValueType, Comparator> _sorter;
  // Buffer for a block of elements, which will be passed to the `_sorter`
  // (respectively: were retrieved from the `_sorter`) at once and
  // asynchronously.
  std::vector<ValueType> _buffer;
  // Wait for the asynchronous background operations.
  std::future<void> _sortInBackgroundFuture;
  std::future<std::vector<ValueType>> _mergeInBackgroundFuture;
  // The number of elements that the `_sorter` can sort in RAM
  size_t _numElsInRun;

  // Used in the output phase as an index into the `_buffer`
  size_t _outputIndex = 0;

 public:
  using value_type = ValueType;

  // The BackgroundStxxlSorter will acutally use 2 * memoryForStxxl plus some
  // overhead.
  explicit BackgroundStxxlSorter(size_t memoryForStxxl,
                                 Comparator comparator = Comparator())
      : _sorter{comparator, memoryForStxxl} {
    _numElsInRun = _sorter.num_els_in_run();
    _buffer.reserve(_numElsInRun);
  }

  // While in the input phase (before calling `sort()`), add another value that
  // is to be sorted.
  void push(ValueType value) {
    _buffer.push_back(std::move(value));
    if (_buffer.size() < _numElsInRun) {
      return;
    }

    // We have filled a block. First wait for the sorting of the last block to
    // finish, and then start sorting the new block in the background.
    if (_sortInBackgroundFuture.valid()) {
      _sortInBackgroundFuture.get();
    }
    auto sortRunInBackground = [this, buffer = std::move(_buffer)]() {
      for (const auto& el : buffer) {
        // TODO<joka921> extend the `stxxl::sorter` interface s.t. we can push a
        // whole block at once.
        _sorter.push(el);
      }
    };
    _sortInBackgroundFuture =
        std::async(std::launch::async, std::move(sortRunInBackground));
    _buffer.clear();
    _buffer.reserve(_numElsInRun);
  }

  // Transition from the input phase, where new elements can be added via
  // `push()` to the output phase, where `operator++`, `operator*` and `empty()`
  // can be called to retrieve the sorted elements.
  void sort() {
    // First sort all remaining elements from the input phase.
    if (_sortInBackgroundFuture.valid()) {
      _sortInBackgroundFuture.get();
    }
    for (const auto& el : _buffer) {
      _sorter.push(el);
    }
    _sorter.sort();
    _buffer.clear();
    _buffer.reserve(_numElsInRun);
    fill_output_buffer();
  }

  bool empty() {
    if (_outputIndex >= _buffer.size()) {
      fill_output_buffer();
    }
    return (_outputIndex >= _buffer.size());
  }

  decltype(auto) operator*() const { return _buffer[_outputIndex]; }

  void fill_output_buffer() {
    _buffer.clear();
    _outputIndex = 0;
    auto mergeNextBlock = [this] {
      std::vector<ValueType> buffer;
      buffer.reserve(_numElsInRun);
      for (size_t i = 0; i < _numElsInRun; ++i) {
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
      _buffer = mergeNextBlock();
    }

    if (_mergeInBackgroundFuture.valid()) {
      _buffer = _mergeInBackgroundFuture.get();
    }

    if (_sorter.empty()) {
      return;
    }
    _mergeInBackgroundFuture = std::async(std::launch::async, mergeNextBlock);
  }

  BackgroundStxxlSorter& operator++() {
    _outputIndex++;
    if (_outputIndex >= _buffer.size()) {
      fill_output_buffer();
    }
    return *this;
  }

  size_t size() const { return _sorter.size(); }

  void clear() {
    if (_sortInBackgroundFuture.valid()) {
      _sortInBackgroundFuture.get();
    }
    if (_mergeInBackgroundFuture.valid()) {
      _mergeInBackgroundFuture.get();
    }

    _sorter.clear();
    _numElsInRun = _sorter.num_els_in_run();
    _buffer.reserve(_numElsInRun);
    _outputIndex = 0;
    _outputIndex = 0;
  }
};

template <typename InputSorter>
class StxxlUniqueSorter {
 private:
  InputSorter& _inputSorter;
  uint64_t _numElementsYielded = 0;
  std::optional<typename InputSorter::value_type> _previousValue;

 public:
  explicit StxxlUniqueSorter(InputSorter& inputSorter)
      : _inputSorter{inputSorter} {}
  [[nodiscard]] bool empty() const {
    if (_inputSorter.empty()) {
      return true;
    } else {
      return false;
    }
  }

  StxxlUniqueSorter& operator++() {
    _numElementsYielded++;
    _previousValue = *_inputSorter;
    ++_inputSorter;
    while (!_inputSorter.empty() && _previousValue == *_inputSorter) {
      ++_inputSorter;
    }
    return *this;
  }

  decltype(auto) operator*() { return *_inputSorter; }
};
}  // namespace ad_utility

#endif  // QLEVER_PARALLELSTXXLSORTER_H
