// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_MERGEBLOCK_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_MERGEBLOCK_H

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <utility>
#include <vector>

#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"

// The words of the partial vocabularies as the parallel block merge (see
// `util/parallelBlockMerge/ParallelBlockMerge.h`) sees them, and the blocks
// that hold them, see `index/vocabulary_merger/PartialVocabularyFile.h`. This
// is not part of the public interface of `index/VocabularyMerger.h`.
//
// NOTE: The `QueueWord` (see `index/vocabulary_merger/QueueWord.h`) is the
// owning counterpart of the `MergeWord` below, which the current merging
// pipeline consumes. A follow-up PR switches the merge to the `MergeBlock`s.
namespace ad_utility::vocabulary_merger::detail {

// A word from a partial vocabulary, as the merge sees it. The word itself is a
// view into the storage of the `MergeBlock` that holds the `MergeWord`, so
// that reading and merging the words allocates nothing per word (a merge
// handles billions of them).
struct MergeWord {
  // The word, a view into the block's storage.
  std::string_view word_;
  // The local index of the word in its partial vocabulary.
  uint64_t index_ = 0;
  // The partial vocabulary that the word came from.
  uint32_t partialFileId_ = 0;
  // Whether the word will be externalized.
  bool isExternal_ = false;

  [[nodiscard]] bool& isExternal() { return isExternal_; }
  [[nodiscard]] const bool& isExternal() const { return isExternal_; }
  [[nodiscard]] std::string_view iriOrLiteral() const { return word_; }
  [[nodiscard]] const uint64_t& id() const { return index_; }
};

// The memory footprint of a `MergeWord` including its word, which the parallel
// merge needs to limit its memory consumption.
inline ad_utility::MemorySize sizeOfMergeWord(const MergeWord& word) {
  return ad_utility::MemorySize::bytes(sizeof(MergeWord) +
                                       word.iriOrLiteral().size());
}

// A block of `MergeWord`s together with the storage that their words point
// into: either the serialized bytes of a block of a partial vocabulary file
// (see `PartialVocabularyRunsInput::getBlock`, which parses the words in place)
// or, for a block of merged words, buffers that the words are copied into. The
// buffers never move once a word points into them, so the block can be moved,
// but not copied. The block is a random-access range of its words.
class MergeBlock {
 public:
  using iterator = std::vector<MergeWord>::iterator;
  using const_iterator = std::vector<MergeWord>::const_iterator;

 private:
  // The size of a buffer of a block of merged words. A word that is longer
  // gets a buffer of its own.
  static constexpr size_t bufferSize_ = 1u << 20;

  std::vector<MergeWord> words_;
  std::vector<std::vector<char>> buffers_;

 public:
  MergeBlock() = default;
  MergeBlock(const MergeBlock&) = delete;
  MergeBlock& operator=(const MergeBlock&) = delete;
  MergeBlock(MergeBlock&&) noexcept = default;
  MergeBlock& operator=(MergeBlock&&) noexcept = default;

  // Construct a block over the serialized `bytes` of `numWords` words of the
  // partial vocabulary `partialFileId`, in the format that
  // `PartialVocabularyWriter` writes: for each word its length (`uint64_t`),
  // its bytes, its external flag (one byte) and its local index (`uint64_t`).
  // The words are parsed in place, nothing is copied.
  MergeBlock(std::vector<char> bytes, size_t numWords, uint32_t partialFileId) {
    buffers_.push_back(std::move(bytes));
    const auto& buffer = buffers_.back();
    words_.reserve(numWords);
    const char* pos = buffer.data();
    const char* end = pos + buffer.size();
    auto readFixed = [&pos, end](auto& target) {
      AD_CORRECTNESS_CHECK(static_cast<size_t>(end - pos) >= sizeof(target),
                           "Truncated block of a partial vocabulary");
      std::memcpy(&target, pos, sizeof(target));
      pos += sizeof(target);
    };
    for (size_t i = 0; i < numWords; ++i) {
      MergeWord word;
      uint64_t length = 0;
      readFixed(length);
      AD_CORRECTNESS_CHECK(static_cast<uint64_t>(end - pos) >= length,
                           "Truncated block of a partial vocabulary");
      word.word_ = std::string_view{pos, length};
      pos += length;
      readFixed(word.isExternal_);
      readFixed(word.index_);
      word.partialFileId_ = partialFileId;
      words_.push_back(word);
    }
    AD_CORRECTNESS_CHECK(pos == end,
                         "Trailing bytes in a block of a partial vocabulary");
  }

  // Append a word, copying it into the storage of this block.
  void push(std::string_view word, bool isExternal, uint32_t partialFileId,
            uint64_t localIndex) {
    MergeWord mergeWord;
    mergeWord.word_ = store(word);
    mergeWord.isExternal_ = isExternal;
    mergeWord.partialFileId_ = partialFileId;
    mergeWord.index_ = localIndex;
    words_.push_back(mergeWord);
  }

  // Append a copy of the `word` of another block.
  void push(const MergeWord& word) {
    push(word.iriOrLiteral(), word.isExternal(), word.partialFileId_,
         word.id());
  }

  // The words of the block, as a range.
  iterator begin() { return words_.begin(); }
  iterator end() { return words_.end(); }
  const_iterator begin() const { return words_.begin(); }
  const_iterator end() const { return words_.end(); }
  size_t size() const { return words_.size(); }
  bool empty() const { return words_.empty(); }
  MergeWord& operator[](size_t i) { return words_[i]; }
  const MergeWord& operator[](size_t i) const { return words_[i]; }
  MergeWord& front() { return words_.front(); }
  const MergeWord& front() const { return words_.front(); }
  MergeWord& back() { return words_.back(); }
  const MergeWord& back() const { return words_.back(); }

  // Release the words and the storage.
  void clear() {
    words_.clear();
    buffers_.clear();
  }

 private:
  // Copy the `word` into the buffers and return the view of the copy. A word
  // is never split across two buffers, and a buffer is never reallocated (it
  // is reserved once and filled up to its capacity), so the views stay valid.
  std::string_view store(std::string_view word) {
    if (buffers_.empty() ||
        buffers_.back().size() + word.size() > buffers_.back().capacity()) {
      buffers_.emplace_back().reserve(std::max(bufferSize_, word.size()));
    }
    auto& buffer = buffers_.back();
    size_t offset = buffer.size();
    buffer.insert(buffer.end(), word.begin(), word.end());
    return {buffer.data() + offset, word.size()};
  }
};
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_MERGEBLOCK_H
