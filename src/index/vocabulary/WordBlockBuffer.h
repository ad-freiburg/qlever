// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_WORDBLOCKBUFFER_H
#define QLEVER_SRC_INDEX_VOCABULARY_WORDBLOCKBUFFER_H

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/span.h"
#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"

namespace ad_utility::vocabulary {

// A buffer that stores a block of words (together with one `isExternal` flag
// per word) contiguously, such that adding a word requires no heap allocation
// in the steady state.
//
// The words are copied into a chain of fixed-size chunks (`chunkSize()`). As
// the words never move once they have been stored, a `std::string_view` per
// word can be (and is) created directly when the word is added, and stays valid
// until the next call to `clear()`. `clear()` resets the buffer, but keeps the
// chunks and the vectors (and thus their capacity), so that a buffer that is
// reused for many blocks only allocates during the first of them (see
// `WordBlockBufferPool` below).
class WordBlockBuffer {
 public:
  // The size of a single chunk. Words that are larger than this get a chunk of
  // their own (see `push`).
  static constexpr size_t chunkSize() { return 4UL << 20; }

 private:
  // The chunks that store the actual characters of the words. Only the chunks
  // with an index `< numUsedChunks_` currently hold words, the remaining ones
  // are kept around to be reused after a `clear()`.
  std::vector<std::vector<char>> chunks_;
  size_t numUsedChunks_ = 0;
  // The number of bytes that are already used in the last of the used chunks.
  size_t offsetInLastChunk_ = 0;
  // One view per word, pointing into `chunks_`.
  std::vector<std::string_view> words_;
  // One flag per word. Note: We deliberately don't use a `std::vector<bool>`,
  // because the bit twiddling is more expensive than the (compared to the words
  // themselves) small amount of memory that it saves.
  std::vector<uint8_t> isExternal_;
  // The sum of the sizes of all the words, maintained incrementally to avoid a
  // second pass over the words.
  size_t totalWordBytes_ = 0;

 public:
  WordBlockBuffer() = default;
  WordBlockBuffer(const WordBlockBuffer&) = delete;
  WordBlockBuffer& operator=(const WordBlockBuffer&) = delete;
  WordBlockBuffer(WordBlockBuffer&&) noexcept = default;
  WordBlockBuffer& operator=(WordBlockBuffer&&) noexcept = default;

  // Add a `word` (which is copied into this buffer) together with its
  // `isExternal` flag.
  void push(std::string_view word, bool isExternal) {
    char* target = allocate(word.size());
    // Note: `std::memcpy` has undefined behavior for a `nullptr` argument, even
    // if the size is zero.
    if (!word.empty()) {
      std::memcpy(target, word.data(), word.size());
    }
    words_.emplace_back(target, word.size());
    isExternal_.push_back(static_cast<uint8_t>(isExternal));
    totalWordBytes_ += word.size();
  }

  // The words that have been added since the last `clear()`, in the order in
  // which they were added. The views are valid until the next call to
  // `clear()`.
  ql::span<const std::string_view> words() const { return words_; }

  // The `isExternal` flag of the word at index `i`.
  bool isExternal(size_t i) const { return static_cast<bool>(isExternal_[i]); }

  // The number of words.
  size_t size() const { return words_.size(); }
  bool empty() const { return words_.empty(); }

  // The sum of the sizes of all the words.
  ad_utility::MemorySize totalWordBytes() const {
    return ad_utility::MemorySize::bytes(totalWordBytes_);
  }

  // Remove all the words, but keep the memory of the chunks and the vectors, so
  // that the buffer can be reused without further allocations. All the
  // `string_view`s that were previously obtained via `words()` become invalid.
  void clear() {
    numUsedChunks_ = 0;
    offsetInLastChunk_ = 0;
    words_.clear();
    isExternal_.clear();
    totalWordBytes_ = 0;
  }

 private:
  // Return a pointer to `numBytes` many contiguous bytes inside `chunks_`.
  char* allocate(size_t numBytes) {
    if (numUsedChunks_ == 0 ||
        offsetInLastChunk_ + numBytes > chunks_[numUsedChunks_ - 1].size()) {
      addChunk(numBytes);
    }
    auto& chunk = chunks_[numUsedChunks_ - 1];
    char* result = chunk.data() + offsetInLastChunk_;
    offsetInLastChunk_ += numBytes;
    return result;
  }

  // Make the next chunk the active one. It has room for at least `numBytes`
  // many bytes. A chunk that is already there (from a previous `clear()`) is
  // reused if it is large enough, and replaced otherwise (which can only happen
  // for words that are larger than `chunkSize()`).
  void addChunk(size_t numBytes) {
    size_t requiredSize = std::max(numBytes, chunkSize());
    if (numUsedChunks_ == chunks_.size()) {
      chunks_.emplace_back(requiredSize);
    } else if (chunks_[numUsedChunks_].size() < requiredSize) {
      chunks_[numUsedChunks_] = std::vector<char>(requiredSize);
    }
    ++numUsedChunks_;
    offsetInLastChunk_ = 0;
  }
};

// A pool of `WordBlockBuffer`s that are reused, such that the buffers of blocks
// that have been completely processed can be filled again without allocating
// their memory anew. The pool is threadsafe, as the buffers are typically
// released from a different thread than the one they were acquired from. The
// buffers are handed out as `shared_ptr`s, because they typically travel
// through several `std::function`s (which require their targets to be
// copyable) before they are released again.
class WordBlockBufferPool {
 public:
  using Ptr = std::shared_ptr<WordBlockBuffer>;

 private:
  std::mutex mutex_;
  std::vector<Ptr> buffers_;
  // The maximal number of buffers that are kept for reuse. Releasing more than
  // this many buffers frees their memory instead.
  size_t maxNumBuffers_;

 public:
  explicit WordBlockBufferPool(size_t maxNumBuffers)
      : maxNumBuffers_{maxNumBuffers} {
    AD_CONTRACT_CHECK(maxNumBuffers > 0);
  }

  // Get an empty buffer, either a recycled one or a fresh one.
  Ptr acquire() {
    std::lock_guard lock{mutex_};
    if (buffers_.empty()) {
      return std::make_shared<WordBlockBuffer>();
    }
    Ptr result = std::move(buffers_.back());
    buffers_.pop_back();
    return result;
  }

  // Return a buffer to the pool for reuse. All the `string_view`s that were
  // obtained from that buffer become invalid. Buffers to which another
  // reference is still held (which can happen when the `std::function` that
  // holds the buffer was copied) are not recycled, as they might still be
  // read from.
  void release(Ptr buffer) {
    if (buffer == nullptr || buffer.use_count() != 1) {
      return;
    }
    buffer->clear();
    std::lock_guard lock{mutex_};
    if (buffers_.size() < maxNumBuffers_) {
      buffers_.push_back(std::move(buffer));
    }
  }
};

}  // namespace ad_utility::vocabulary

#endif  // QLEVER_SRC_INDEX_VOCABULARY_WORDBLOCKBUFFER_H
