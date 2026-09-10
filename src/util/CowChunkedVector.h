// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COWCHUNKEDVECTOR_H
#define QLEVER_SRC_UTIL_COWCHUNKEDVECTOR_H

#include <memory>
#include <vector>

#include "backports/span.h"
#include "util/Exception.h"

namespace ad_utility {

// A vector-like container that stores its elements in chunks of a fixed size,
// where each chunk is held via a `shared_ptr`. This makes copying the
// container cheap (only the chunk pointers are copied, the chunks themselves
// are shared between the copies). Mutating access clones a chunk if and only
// if it is shared with another copy, so mutations never affect previously
// made copies. This is useful for large structures of which frequent
// snapshots are taken, but where each mutation between two snapshots only
// touches few elements.
//
// Elements can only be added or removed at the end, so all chunks except the
// last one always have exactly `ChunkSize` elements and access by index is
// constant time.
//
// NOTE: The copy-on-write logic is based on the `use_count` of a chunk.
// Copying the container concurrently with a mutation is therefore undefined
// behavior; the caller must synchronize copies and mutations externally
// (reads from other copies are always safe).
template <typename T, size_t ChunkSize = 512>
class CowChunkedVector {
  static_assert(ChunkSize > 0);
  using Chunk = std::vector<T>;
  std::vector<std::shared_ptr<Chunk>> chunks_;
  size_t size_ = 0;

 public:
  CowChunkedVector() = default;

  // Construct from a vector of elements by copying them into chunks.
  explicit CowChunkedVector(const std::vector<T>& elements)
      : size_{elements.size()} {
    chunks_.reserve((size_ + ChunkSize - 1) / ChunkSize);
    for (size_t begin = 0; begin < size_; begin += ChunkSize) {
      size_t end = std::min(begin + ChunkSize, size_);
      chunks_.push_back(std::make_shared<Chunk>(elements.begin() + begin,
                                                elements.begin() + end));
    }
  }

  size_t size() const { return size_; }
  bool empty() const { return size_ == 0; }

  // Constant-time read access to the element at index `i`.
  const T& operator[](size_t i) const {
    AD_EXPENSIVE_CHECK(i < size_);
    return (*chunks_[i / ChunkSize])[i % ChunkSize];
  }

  // Mutable access to the element at index `i`. If the containing chunk is
  // shared with a copy of this container, it is cloned first (copy-on-write),
  // see the note in the class comment.
  T& mutableAt(size_t i) {
    AD_CONTRACT_CHECK(i < size_);
    return (*mutableChunk(i / ChunkSize))[i % ChunkSize];
  }

  // Append an element at the end.
  void push_back(T value) {
    if (size_ % ChunkSize == 0) {
      chunks_.push_back(std::make_shared<Chunk>());
      chunks_.back()->reserve(ChunkSize);
    }
    mutableChunk(chunks_.size() - 1)->push_back(std::move(value));
    ++size_;
  }

  // Remove the last element.
  void pop_back() {
    AD_CONTRACT_CHECK(size_ > 0);
    mutableChunk(chunks_.size() - 1)->pop_back();
    --size_;
    if (chunks_.back()->empty()) {
      chunks_.pop_back();
    }
  }

  // Return one contiguous `span` per chunk, in order. The concatenation of
  // the spans is the sequence of all elements. The spans remain valid as long
  // as no copy-on-write mutation touches the respective chunk and this
  // container (or a copy sharing the chunk) is alive.
  std::vector<ql::span<const T>> chunkSpans() const {
    std::vector<ql::span<const T>> result;
    result.reserve(chunks_.size());
    for (const auto& chunk : chunks_) {
      result.emplace_back(chunk->data(), chunk->size());
    }
    return result;
  }

 private:
  // Return the chunk with the given index for mutation, cloning it first if
  // it is shared with a copy of this container.
  const std::shared_ptr<Chunk>& mutableChunk(size_t chunkIndex) {
    auto& chunk = chunks_[chunkIndex];
    if (chunk.use_count() > 1) {
      chunk = std::make_shared<Chunk>(*chunk);
    }
    return chunk;
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_COWCHUNKEDVECTOR_H
