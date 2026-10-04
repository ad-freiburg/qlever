// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COPYONWRITECHUNKEDVECTOR_H
#define QLEVER_SRC_UTIL_COPYONWRITECHUNKEDVECTOR_H

#include <algorithm>
#include <cstddef>
#include <utility>
#include <vector>

#include "backports/span.h"
#include "util/CopyOnWritePtr.h"
#include "util/Exception.h"

namespace ad_utility {

// A vector of `T` that stores its elements in chunks of `ChunkSize` elements,
// each held via a `CopyOnWritePtr`. Copying the vector therefore only copies
// one pointer per chunk, and a mutation clones only the chunk that it touches
// (and only if that chunk is shared with a copy). This is the building block
// for large arrays of which a snapshot is taken after each mutation, but where
// each mutation only touches few elements.
//
// Elements can only be added and removed at the end. All chunks except the
// last one therefore have exactly `ChunkSize` elements, no chunk is empty, and
// the access by index takes constant time.
//
// NOTE: Copies and mutations must be synchronized externally, see the
// IMPORTANT note for `CopyOnWritePtr`. Reading is always safe.
template <typename T, size_t ChunkSize = 512>
class CopyOnWriteChunkedVector {
  static_assert(ChunkSize > 0);

 private:
  using Chunk = std::vector<T>;
  std::vector<CopyOnWritePtr<Chunk>> chunks_;
  size_t size_ = 0;

 public:
  CopyOnWriteChunkedVector() = default;

  // Create a vector with a copy of the given `elements`.
  explicit CopyOnWriteChunkedVector(ql::span<const T> elements)
      : size_{elements.size()} {
    // The number of chunks is `ceil(size_ / ChunkSize)`.
    chunks_.reserve((size_ + ChunkSize - 1) / ChunkSize);
    for (size_t begin = 0; begin < size_; begin += ChunkSize) {
      size_t end = std::min(begin + ChunkSize, size_);
      chunks_.emplace_back(
          Chunk(elements.begin() + begin, elements.begin() + end));
    }
  }

  size_t size() const { return size_; }
  bool empty() const { return size_ == 0; }

  // Read access to the element with the given index.
  const T& operator[](size_t i) const {
    AD_EXPENSIVE_CHECK(i < size_);
    return (*chunks_[i / ChunkSize])[i % ChunkSize];
  }

  // Write access to the element with the given index. Clones the chunk of the
  // element if it is shared with a copy of this vector.
  T& mutableAt(size_t i) {
    AD_CONTRACT_CHECK(i < size_);
    return chunks_[i / ChunkSize].write()[i % ChunkSize];
  }

  // Append an element at the end. Start a new chunk if the last chunk is full.
  void push_back(T value) {
    if (size_ % ChunkSize == 0) {
      chunks_.emplace_back().write().reserve(ChunkSize);
    }
    chunks_.back().write().push_back(std::move(value));
    ++size_;
  }

  // Remove the last element. Remove the last chunk if it becomes empty.
  void pop_back() {
    AD_CONTRACT_CHECK(size_ > 0);
    chunks_.back().write().pop_back();
    --size_;
    if (chunks_.back()->empty()) {
      chunks_.pop_back();
    }
  }

  // Return one `span` per chunk, in order, so that the concatenation of the
  // spans is the sequence of all elements. The spans are non-empty. They stay
  // valid as long as the vector (or a copy that shares the respective chunk)
  // is alive and that chunk is not mutated through the vector.
  std::vector<ql::span<const T>> chunkSpans() const {
    std::vector<ql::span<const T>> result;
    result.reserve(chunks_.size());
    for (const auto& chunk : chunks_) {
      result.emplace_back(chunk->data(), chunk->size());
    }
    return result;
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_COPYONWRITECHUNKEDVECTOR_H
