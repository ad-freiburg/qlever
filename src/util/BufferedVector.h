// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <johannes.kalmbach@gmail.com>

#pragma once
#include <vector>
#include "MmapVector.h"

#include <gtest/gtest.h>

namespace ad_utility {

// A dynamic array that uses std::vector if it is small and the MmapVector if it
// is big The switching threshold can be set when creating the vector Currently
// only push_back and clear are supported (+ all kinds of access to existing
// elements) Can be trivially extended to complete the interface
// The backup file is temporary and will be deleted in the destructor
template <class T>
class BufferedVector {
 public:
  using const_iterator = const T*;
  using iterator = T*;

  // Constructor needs the wanted threshold (how many elements until we
  // externalize) and the filename Where we will initialize the mmapVector
  BufferedVector(size_t threshold, std::string extFilename)
      : _threshold(threshold), _extVec(0, std::move(extFilename)) {}

  // __________________________________________________________________
  size_t size() const { return _isInternal ? _vec.size() : _extVec.size(); }

  // standard iterator functions, each in a const and non-const version
  // begin
  T* begin() { return _isInternal ? _vec.data() : _extVec.begin(); }
  T* data() { return _isInternal ? _vec.data() : _extVec.begin(); }
  const T* begin() const { return _isInternal ? _vec.data() : _extVec.begin(); }
  const T* data() const { return _isInternal ? _vec.data() : _extVec.begin(); }

  // end
  T* end() { return _isInternal ? _vec.data() + _vec.size() : _extVec.end(); }
  const T* end() const {
    return _isInternal ? _vec.data() + _vec.size() : _extVec.end();
  }

  // cbegin and cend
  const T* cbegin() const {
    return _isInternal ? _vec.data() : _extVec.begin();
  }
  const T* cend() const {
    return _isInternal ? _vec.data() + _vec.size() : _extVec.end();
  }

  // Element access
  // without bounds checking
  T& operator[](size_t idx) { return _isInternal ? _vec[idx] : _extVec[idx]; }
  const T& operator[](size_t idx) const {
    return _isInternal ? _vec[idx] : _extVec[idx];
  }

  // with bounds checking
  T& at(size_t idx) { return _isInternal ? _vec.at(idx) : _extVec.at(idx); }
  const T& at(size_t idx) const {
    return _isInternal ? _vec.at(idx) : _extVec.at(idx);
  }

  // no copy construction/assignment since the MmapVector does not support this
  BufferedVector(const BufferedVector<T>&) = delete;
  BufferedVector& operator=(const BufferedVector<T>&) = delete;

  // move construction and assignment
  BufferedVector(BufferedVector<T>&& other) noexcept = default;
  BufferedVector& operator=(BufferedVector<T>&& other) noexcept = default;

  // _________________________________________________________________________
  void clear() {
    _vec.clear();
    // this is not necessary and typically slow
    //_extVec.clear();
    _isInternal = true;
  }

  // add element specified by arg el at the end of the array
  // possibly invalidates iterators
  void push_back(const T& el) {
    auto oldSize = size();
    if (!_isInternal) {
      _extVec.push_back(el);
    } else if (oldSize < _threshold) {
      _vec.push_back(el);
    } else {
      _extVec.resize(oldSize + 1);
      for (size_t i = 0; i < oldSize; ++i) {
        _extVec[i] = _vec[i];
      }
      _extVec[oldSize] = el;
      _isInternal = false;
      _vec.clear();
    }
  }

  // testing interface
  size_t threshold() const { return _threshold; }
  bool isInternal() const { return _isInternal; }

 private:
  // the externalization threshold
  const size_t _threshold = 2 << 25;
  // keep track on which of our data stores we are currently using.
  bool _isInternal = true;

  // the two possible data storages
  std::vector<T> _vec;
  ad_utility::MmapVectorTmp<T> _extVec;
};
}  // namespace ad_utility
