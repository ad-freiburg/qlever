//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_PARALLELSTXXLSORTER_H
#define QLEVER_PARALLELSTXXLSORTER_H

#include <future>
#include <stxxl/sorter>

#include "./Log.h"

namespace ad_utility {

template <typename ValueType, typename Comparator>
class BackgroundStxxlSorter {
 private:
  stxxl::sorter<ValueType, Comparator> _sorter;
  std::vector<ValueType> _pushBuffer;
  std::future<void> _sortInBackgroundFuture;
  size_t _numElsInRun;
  size_t _outputIndex = 0;
  size_t _numPushed = 0;
  size_t _numPulled = 0;

 public:
  using value_type = ValueType;

  explicit BackgroundStxxlSorter(size_t memoryForStxxl,
                                 Comparator comparator = Comparator())
      : _sorter{comparator, memoryForStxxl} {
    _numElsInRun = _sorter.num_els_in_run();
    _pushBuffer.reserve(_numElsInRun);
  }

  void push(ValueType value) {
    _pushBuffer.push_back(std::move(value));
    _numPushed++;
    if (_pushBuffer.size() < _numElsInRun) {
      return;
    }
    if (_sortInBackgroundFuture.valid()) {
      _sortInBackgroundFuture.get();
    }
    auto sortRunInBackground = [this, buffer = std::move(_pushBuffer)]() {
      for (const auto& el : buffer) {
        // TODO<joka921> extend the sorter interface by pushing many elements;
        _sorter.push(el);
      }
    };
    _sortInBackgroundFuture =
        std::async(std::launch::async, std::move(sortRunInBackground));
    _pushBuffer.clear();
    _pushBuffer.reserve(_numElsInRun);
  }

  void sort() {
    if (_sortInBackgroundFuture.valid()) {
      _sortInBackgroundFuture.get();
    }
    for (const auto& el : _pushBuffer) {
      _sorter.push(el);
    }
    _sorter.sort();
    _pushBuffer.clear();
    _numElsInRun /= 2;
    _pushBuffer.reserve(_numElsInRun);
    fill_buffer();
  }

  bool empty() const {
    if (_outputIndex >= _pushBuffer.size() && _sorter.empty()) {
      return true;
    } else {
      return false;
    }
  }

  decltype(auto) operator*() const {
    AD_CHECK(!empty());
    AD_CHECK(_outputIndex < _pushBuffer.size());
    return _pushBuffer[_outputIndex];
  }

  void fill_buffer() {
    _pushBuffer.clear();
    _outputIndex = 0;
    for (size_t i = 0; i < _numElsInRun; ++i) {
      if (_sorter.empty()) {
        _numElsInRun = i;
        break;
      }
      _pushBuffer.push_back(*_sorter);
      ++_sorter;
    }
  }

  BackgroundStxxlSorter& operator++() {
    _outputIndex++;
    if (_outputIndex == _numElsInRun) {
      AD_CHECK(_outputIndex == _pushBuffer.size());
      fill_buffer();
    }
    _numPulled++;
    return *this;
  }

  size_t size() const { return _sorter.size(); }
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

class StxxlDummySorter {
 public:
  StxxlDummySorter() = default;
  void push(const auto&) {}
};
}  // namespace ad_utility

#endif  // QLEVER_PARALLELSTXXLSORTER_H
