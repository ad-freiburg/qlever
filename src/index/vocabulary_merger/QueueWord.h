// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_QUEUEWORD_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_QUEUEWORD_H

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/span.h"
#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"

// The words that the vocabulary merger (see `index/VocabularyMerger.h`) reads
// from the partial vocabularies and merges, and the blocks that hold them. This
// is not part of the public interface of that header.
namespace ad_utility::vocabulary_merger::detail {

// An occurrence of a word in a partial vocabulary: the index of the partial
// vocabulary and the local index of the word in it.
using Occurrence = std::pair<uint32_t, uint64_t>;

// A word from a partial vocabulary, as the merge sees it. The word itself is a
// view into the storage of the `MergeBlock` that holds the `QueueWord`, so
// that reading and merging the words allocates nothing per word (a merge
// handles billions of them). While the partial vocabularies are merged, the
// further occurrences of the same word in other partial vocabularies are
// collected in the block (see `MergeBlock::addOccurrenceToLastWord`), so that
// the words that leave the merge are distinct within a block, with one entry
// per occurrence.
struct QueueWord {
  // The word, a view into the block's storage.
  std::string_view word_;
  // The local index of the word in its partial vocabulary.
  uint64_t index_ = 0;
  // The range of the further occurrences (see above) in the
  // `moreOccurrences_` of the block.
  uint64_t firstMoreOccurrence_ = 0;
  uint32_t numMoreOccurrences_ = 0;
  // The partial vocabulary that the word came from.
  uint32_t partialFileId_ = 0;
  // Whether the word will be externalized (in any of its occurrences).
  bool isExternal_ = false;

  [[nodiscard]] bool& isExternal() { return isExternal_; }
  [[nodiscard]] const bool& isExternal() const { return isExternal_; }
  [[nodiscard]] std::string_view iriOrLiteral() const { return word_; }
  [[nodiscard]] const uint64_t& id() const { return index_; }
};

// The memory footprint of a `QueueWord` including its word, which the parallel
// merging needs to limit its memory consumption.
inline ad_utility::MemorySize sizeOfQueueWord(const QueueWord& word) {
  return ad_utility::MemorySize::bytes(sizeof(QueueWord) +
                                       word.iriOrLiteral().size());
}

// A block of `QueueWord`s together with the storage that their words point
// into: either the serialized bytes of a block of a partial vocabulary file
// (see `PartialVocabularyRunsInput::getBlock`, which parses the words in place)
// or, for a block of merged words, buffers that the words are copied into. The
// buffers never move once a word points into them, so the block can be moved,
// but not copied. The block is a random-access range of its words.
class MergeBlock {
 public:
  using Occurrence = detail::Occurrence;
  using iterator = std::vector<QueueWord>::iterator;
  using const_iterator = std::vector<QueueWord>::const_iterator;

 private:
  // The size of a buffer of a block of merged words. A word that is longer
  // gets a buffer of its own.
  static constexpr size_t bufferSize_ = 1u << 20;

  std::vector<QueueWord> words_;
  std::vector<Occurrence> moreOccurrences_;
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
      QueueWord word;
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

  // Append a word, copying it into the storage of this block. The occurrence
  // `(partialFileId, localIndex)` is its first one, further ones are added
  // with `addOccurrenceToLastWord`.
  void push(std::string_view word, bool isExternal, uint32_t partialFileId,
            uint64_t localIndex) {
    QueueWord queueWord;
    queueWord.word_ = store(word);
    queueWord.isExternal_ = isExternal;
    queueWord.partialFileId_ = partialFileId;
    queueWord.index_ = localIndex;
    words_.push_back(queueWord);
  }

  // Append a copy of the `word` of another block. It must not have further
  // occurrences (those live in its own block).
  void push(const QueueWord& word) {
    AD_CORRECTNESS_CHECK(word.numMoreOccurrences_ == 0);
    push(word.iriOrLiteral(), word.isExternal(), word.partialFileId_,
         word.id());
  }

  // Record a further occurrence of the last word of the block.
  void addOccurrenceToLastWord(uint32_t partialFileId, uint64_t localIndex) {
    AD_CONTRACT_CHECK(!words_.empty());
    auto& last = words_.back();
    if (last.numMoreOccurrences_ == 0) {
      last.firstMoreOccurrence_ = moreOccurrences_.size();
    }
    // The occurrences of the last word are the last ones in the vector.
    AD_CORRECTNESS_CHECK(last.firstMoreOccurrence_ + last.numMoreOccurrences_ ==
                         moreOccurrences_.size());
    moreOccurrences_.emplace_back(partialFileId, localIndex);
    ++last.numMoreOccurrences_;
  }

  // The further occurrences of a `word` of this block, see above.
  ql::span<const Occurrence> moreOccurrences(const QueueWord& word) const {
    AD_CONTRACT_CHECK(word.firstMoreOccurrence_ + word.numMoreOccurrences_ <=
                      moreOccurrences_.size());
    return {moreOccurrences_.data() + word.firstMoreOccurrence_,
            word.numMoreOccurrences_};
  }

  // The words of the block, as a range.
  iterator begin() { return words_.begin(); }
  iterator end() { return words_.end(); }
  const_iterator begin() const { return words_.begin(); }
  const_iterator end() const { return words_.end(); }
  size_t size() const { return words_.size(); }
  bool empty() const { return words_.empty(); }
  QueueWord& operator[](size_t i) { return words_[i]; }
  const QueueWord& operator[](size_t i) const { return words_[i]; }
  QueueWord& front() { return words_.front(); }
  const QueueWord& front() const { return words_.front(); }
  QueueWord& back() { return words_.back(); }
  const QueueWord& back() const { return words_.back(); }

  // Release the words and the storage.
  void clear() {
    words_.clear();
    moreOccurrences_.clear();
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

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_QUEUEWORD_H
