// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_UTIL_BUFFEREDVECTOR_H
#define QLEVER_SRC_UTIL_BUFFEREDVECTOR_H

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
  using value_type = T;

 private:
  // the externalization threshold
  size_t _threshold = 2 << 25;
  // keep track on which of our data stores we are currently using.
  bool _isInternal = true;

  // the two possible data storages
  std::vector<T> _vec;
  ad_utility::MmapVectorTmp<T> _extVec;

 public:
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
 private:
  template <typename... Args>
  void push_backImpl(Args&&... args) {
    auto oldSize = size();
    if (!_isInternal) {
      _extVec.push_back(T{AD_FWD(args)...});
    } else if (oldSize < _threshold) {
      _vec.emplace_back(AD_FWD(args)...);
    } else {
      _extVec.resize(oldSize + 1);
      for (size_t i = 0; i < oldSize; ++i) {
        _extVec[i] = _vec[i];
      }
      _extVec[oldSize] = T{AD_FWD(args)...};
      _isInternal = false;
      _vec.clear();
    }
  }

 public:
  void push_back(const T& el) { push_backImpl(el); }
  template <typename... Args>
  void emplace_back(Args&&... args) {
    push_backImpl(AD_FWD(args)...);
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
      AD_CONTRACT_CHECK(newSize > oldSize);
      std::copy(_vec.begin(), _vec.end(), _extVec.begin());
      _isInternal = false;
      _vec.clear();
    } else {
      AD_CONTRACT_CHECK(!_isInternal && newSize < _threshold);
      AD_CONTRACT_CHECK(newSize < oldSize);
      _vec.resize(newSize);
      std::copy(_extVec.begin(), _extVec.begin() + newSize, _vec.begin());
      _isInternal = true;
    }
  }

  // Similar to `std::vector::insert`. Insert the elements in the iterator range
  // `[it1, it2)` into the vector at `target`. The `it1` and `it2` must not
  // overlap with `this` and `target` must be a valid iterator for `this`. Both
  // of these conditions are checked via an `AD_CONTRACT_CHECK`.
  template <typename It>
  void insert(T* target, It it1, It it2) {
    AD_CONTRACT_CHECK(target >= begin() && target <= end());
    AD_CONTRACT_CHECK(it2 >= it1);
    auto addr1 = &(*it1);
    auto addr2 = &(*it2);
    AD_CONTRACT_CHECK((addr1 < begin() || addr1 >= end()) &&
                      (addr2 < begin() || addr2 >= end()));
    size_t numInserted = it2 - it1;
    size_t offset = target - begin();
    resize(size() + numInserted);
    std::shift_right(begin() + offset, end(), numInserted);
    for (auto it = it1; it != it2; ++it) {
      (*this)[offset++] = *it;
    }
  }

  // Erase the elements between `it1` and `it2`. The remaining elements will
  // stay in the same order. `it1` and `it2` must be valid iterators for `this`
  // and `it1 <= `it2` must hold. Both of these conditions are checked via
  // `AD_CONTRACT_CHECK`.
  void erase(T* it1, T* it2) {
    AD_CONTRACT_CHECK(begin() <= it1 && it1 <= it2 && it2 <= end());
    size_t numErased = it2 - it1;
    std::shift_left(it1, end(), numErased);
    resize(size() - numErased);
  }

  // This stub implementation of `reserve` currently does nothing, but makes the
  // interface more consistent with `std::vector`
  void reserve([[maybe_unused]] size_t newCapacity) {}
  // Same goes for `shrink_to_fit`
  void shrink_to_fit() {}

  // Get the underlying allocator of the vector.
  auto get_allocator() const { return _vec.get_allocator(); }

  // testing interface
  size_t threshold() const { return _threshold; }
  bool isInternal() const { return _isInternal; }
};
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_BUFFEREDVECTOR_H
