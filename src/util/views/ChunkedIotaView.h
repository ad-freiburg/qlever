// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_VIEWS_CHUNKEDIOTAVIEW_H
#define QLEVER_SRC_UTIL_VIEWS_CHUNKEDIOTAVIEW_H

#include <iterator>
#include <type_traits>
#include <utility>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "util/Exception.h"

namespace ad_utility {
// A view that splits the half-open integer range `[first, end)` into
// consecutive chunks of `blockSize` elements each (the last chunk may be
// smaller) and yields each chunk as a `std::pair{chunkBegin, chunkEnd}` of
// half-open bounds. For example, `ChunkedIotaView{0, 7, 3}` yields `{0, 3}`,
// `{3, 6}`, `{6, 7}`. This is the common pattern of "chunk an `iota` range and
// then only use the first element and the size of each chunk".
//
// NOTE: In contrast to `ql::views::iota`, the `first` and the `end` must have
// exactly the same type. `iota` silently accepts mixed types (e.g. `int` and
// `size_t`), which can lead to surprising results or to infinite ranges.
CPP_template(typename T)(requires std::is_integral_v<T>) class ChunkedIotaView
    : public ql::ranges::view_interface<ChunkedIotaView<T>> {
 private:
  T first_{};
  T end_{};
  T blockSize_{1};

 public:
  class Iterator {
   public:
    using value_type = std::pair<T, T>;
    using reference = value_type;
    using difference_type = std::ptrdiff_t;
    using iterator_category = std::input_iterator_tag;
    using iterator_concept = std::forward_iterator_tag;

   private:
    T current_{};
    T end_{};
    T blockSize_{1};

   public:
    Iterator() = default;
    Iterator(T current, T end, T blockSize)
        : current_{current}, end_{end}, blockSize_{blockSize} {}

    // Return the end of the chunk that starts at `current_`. This is written
    // such that it cannot overflow, even if `current_ + blockSize_` would.
    T chunkEnd() const {
      return end_ - current_ <= blockSize_ ? end_ : current_ + blockSize_;
    }

    // _________________________________________________________________________
    value_type operator*() const { return {current_, chunkEnd()}; }

    // _________________________________________________________________________
    Iterator& operator++() {
      current_ = chunkEnd();
      return *this;
    }

    // _________________________________________________________________________
    Iterator operator++(int) {
      auto copy = *this;
      ++*this;
      return copy;
    }

    // _________________________________________________________________________
    friend bool operator==(const Iterator& a, const Iterator& b) {
      return a.current_ == b.current_;
    }
    friend bool operator!=(const Iterator& a, const Iterator& b) {
      return !(a == b);
    }
  };

  ChunkedIotaView() = default;

  // Construct the view for the range `[first, end)` split into chunks of
  // `blockSize`. The `first` and the `end` have to be of type `T` exactly (see
  // the class comment), `first <= end` and `blockSize > 0` must hold.
  CPP_template_2(typename U, typename V)(
      requires std::is_same_v<U, T>&& std::is_same_v<V, T>)
      ChunkedIotaView(U first, V end, T blockSize)
      : first_{first}, end_{end}, blockSize_{blockSize} {
    AD_CONTRACT_CHECK(first_ <= end_);
    AD_CONTRACT_CHECK(blockSize_ > 0);
  }

  // _________________________________________________________________________
  Iterator begin() const { return {first_, end_, blockSize_}; }
  Iterator end() const { return {end_, end_, blockSize_}; }

  // The number of chunks.
  size_t size() const {
    auto numElements = static_cast<size_t>(end_ - first_);
    auto blockSize = static_cast<size_t>(blockSize_);
    return numElements / blockSize + (numElements % blockSize != 0);
  }
};

// Deduce `T` from the `first` and the `end`, which have to be of the same type.
template <typename T, typename B>
ChunkedIotaView(T, T, B) -> ChunkedIotaView<T>;

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_VIEWS_CHUNKEDIOTAVIEW_H
