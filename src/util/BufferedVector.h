// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <johannes.kalmbach@gmail.com>

#pragma once
#include <gtest/gtest.h>

#include <vector>

#include "MmapVector.h"

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
      : _threshold(threshold), _extVec(std::move(extFilename)) {}

  // __________________________________________________________________
  size_t size() const { return _isInternal ? _vec.size() : _extVec.size(); }
  bool empty() const { return size() == 0; }

  // standard iterator functions, each in a const and non-const version
  // begin
  T* begin() { return _isInternal ? _vec.data() : _extVec.data(); }
  T* data() { return _isInternal ? _vec.data() : _extVec.data(); }
  const T* begin() const { return _isInternal ? _vec.data() : _extVec.data(); }
  const T* data() const { return _isInternal ? _vec.data() : _extVec.data(); }

  // end
  T* end() {
    return _isInternal ? _vec.data() + _vec.size()
                       : _extVec.data() + _extVec.size();
  }
  const T* end() const {
    return _isInternal ? _vec.data() + _vec.size()
                       : _extVec.data() + _extVec.size();
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

  // First element with bounds checking.
  const T& front() const { return at(0); }

  // Last element with bounds checking.
  const T& back() const { return at(size() - 1); }

  // no copy construction/assignment since the MmapVector does not support this
  BufferedVector(const BufferedVector&) = delete;
  BufferedVector& operator=(const BufferedVector&) = delete;

  // move construction and assignment
  BufferedVector& operator=(BufferedVector&& other) = default;
  BufferedVector(BufferedVector&& other) = default;

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

  // Change the size of the `BufferedVector` to `newSize`. This might move
  // the data from the internal to the external vector or vice versa. If
  // `newSize < size()` the `BufferedVector` will be truncated to the first
  // `newSize` elements.
  void resize(size_t newSize) {
    auto oldSize = size();
    if (!_isInternal && newSize > _threshold) {
      _extVec.resize(newSize);
    } else if (_isInternal && newSize < _threshold) {
      _vec.resize(newSize);
    } else if (_isInternal && newSize >= _threshold) {
      _extVec.resize(newSize);
      AD_CHECK(newSize > oldSize);
      std::copy(_vec.begin(), _vec.end(), _extVec.begin());
      _isInternal = false;
      _vec.clear();
    } else {
      AD_CHECK(!_isInternal && newSize < _threshold);
      AD_CHECK(newSize < oldSize);
      _vec.resize(newSize);
      std::copy(_extVec.begin(), _extVec.begin() + newSize, _vec.begin());
      _isInternal = true;
    }
  }

  // Get the underlying allocator of the vector.
  auto get_allocator() const { return _vec.get_allocator(); }

  // testing interface
  size_t threshold() const { return _threshold; }
  bool isInternal() const { return _isInternal; }

 private:
  // the externalization threshold
  size_t _threshold = 2 << 25;
  // keep track on which of our data stores we are currently using.
  bool _isInternal = true;

  // the two possible data storages
  std::vector<T> _vec;
  ad_utility::MmapVectorTmp<T> _extVec;
};
}  // namespace ad_utility
