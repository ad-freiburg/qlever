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
// IMPORTANT note for `CopyOnWritePtr`. Reading via a copy is safe concurrently
// with mutations of another copy. Reading and mutating the same instance
// concurrently is a data race, as for any standard container.
template <typename T, size_t ChunkSize = 512>
class CopyOnWriteChunkedVector {
  static_assert(ChunkSize > 0);

 private:
  using Chunk = std::vector<T>;
  std::vector<CopyOnWritePtr<Chunk>> chunks_;

 public:
  CopyOnWriteChunkedVector() = default;

  // Create a vector with a copy of the given `elements`.
  explicit CopyOnWriteChunkedVector(ql::span<const T> elements) {
    // The number of chunks is `ceil(elements.size() / ChunkSize)`.
    size_t size = elements.size();
    chunks_.reserve((size + ChunkSize - 1) / ChunkSize);
    for (size_t begin = 0; begin < size; begin += ChunkSize) {
      size_t end = std::min(begin + ChunkSize, size);
      chunks_.emplace_back(
          Chunk(elements.begin() + begin, elements.begin() + end));
    }
  }

  // The number of elements. All chunks except the last one are full, so this
  // takes constant time (and there is no need to store the size separately,
  // which would also have to be reset on a move).
  size_t size() const {
    return chunks_.empty()
               ? 0
               : (chunks_.size() - 1) * ChunkSize + chunks_.back()->size();
  }
  bool empty() const { return chunks_.empty(); }

  // Read access to the element with the given index.
  const T& operator[](size_t i) const {
    AD_EXPENSIVE_CHECK(i < size());
    return (*chunks_[i / ChunkSize])[i % ChunkSize];
  }

  // Write access to the element with the given index. Clones the chunk of the
  // element if it is shared with a copy of this vector.
  T& mutableAt(size_t i) {
    AD_CONTRACT_CHECK(i < size());
    return chunks_[i / ChunkSize].write()[i % ChunkSize];
  }

  // Append an element at the end. Start a new chunk if the last chunk is full.
  void push_back(T value) {
    if (chunks_.empty() || chunks_.back()->size() == ChunkSize) {
      chunks_.emplace_back().write().reserve(ChunkSize);
    }
    chunks_.back().write().push_back(std::move(value));
  }

  // Remove the last element. Remove the last chunk if it becomes empty.
  void pop_back() {
    AD_CONTRACT_CHECK(!chunks_.empty());
    chunks_.back().write().pop_back();
    if (chunks_.back()->empty()) {
      chunks_.pop_back();
    }
  }

  // Return one `span` per chunk, in order, so that the concatenation of the
  // spans is the sequence of all elements. The spans are non-empty. They stay
  // valid as long as the vector (or a copy that shares the respective chunk)
  // is alive and that chunk is not mutated through the vector.
  //
  // NOTE: This is the read path for consumers that process the elements as a
  // sequence of contiguous ranges (like the scan machinery does with the block
  // metadata). There is no single span over all elements, and copying them into
  // one vector would cost what the chunking saves.
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
