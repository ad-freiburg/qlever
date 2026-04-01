// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#pragma once

#include <iterator>

namespace ad_utility::detail {

// A forward iterator that flattens a nested container (e.g., vector of vectors)
// into a single sequential iteration. Elements are yielded in order: all
// elements of the first inner container, then all of the second, etc.
//
// `OuterIt` iterates over inner containers. `InnerIt` iterates within them.
// The inner containers must support `.begin()` and `.end()` returning
// `InnerIt`.
template <typename OuterIt, typename InnerIt>
class FlattenIterator {
 public:
  using iterator_category = std::forward_iterator_tag;
  using value_type = typename std::iterator_traits<InnerIt>::value_type;
  using difference_type = std::ptrdiff_t;
  using pointer = typename std::iterator_traits<InnerIt>::pointer;
  using reference = typename std::iterator_traits<InnerIt>::reference;

 private:
  OuterIt outerIt_;
  OuterIt outerEnd_;
  InnerIt innerIt_;

  // Advance past empty inner containers to reach the next valid element.
  void skipEmpty() {
    while (outerIt_ != outerEnd_ && innerIt_ == outerIt_->end()) {
      ++outerIt_;
      if (outerIt_ != outerEnd_) {
        innerIt_ = outerIt_->begin();
      }
    }
  }

 public:
  // Construct a begin iterator.
  FlattenIterator(OuterIt outerBegin, OuterIt outerEnd)
      : outerIt_(outerBegin), outerEnd_(outerEnd) {
    if (outerIt_ != outerEnd_) {
      innerIt_ = outerIt_->begin();
      skipEmpty();
    }
  }

  // Default constructor (required for sentinel/end and default-constructible
  // iterators).
  FlattenIterator() = default;

  reference operator*() const { return *innerIt_; }
  pointer operator->() const { return &*innerIt_; }

  FlattenIterator& operator++() {
    ++innerIt_;
    skipEmpty();
    return *this;
  }

  FlattenIterator operator++(int) {
    FlattenIterator tmp = *this;
    ++(*this);
    return tmp;
  }

  bool operator==(const FlattenIterator& other) const {
    // Both at the end of the outer range.
    if (outerIt_ == outerEnd_ && other.outerIt_ == other.outerEnd_) {
      return true;
    }
    // One at end, the other not.
    if (outerIt_ == outerEnd_ || other.outerIt_ == other.outerEnd_) {
      return false;
    }
    return outerIt_ == other.outerIt_ && innerIt_ == other.innerIt_;
  }

  bool operator!=(const FlattenIterator& other) const {
    return !(*this == other);
  }
};

}  // namespace ad_utility::detail
