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
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/memory_resource.h"
#include "backports/span.h"
#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"
#include "util/NoCopyNoMove.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/UninitializedAllocator.h"

// The words of the partial vocabularies as the parallel block merge (see
// `util/parallelBlockMerge/ParallelBlockMerge.h`) sees them, and the blocks
// that hold them, see `index/vocabulary_merger/PartialVocabularyFile.h`. This
// is not part of the public interface of `index/VocabularyMerger.h`.
//
// NOTE: The `QueueWord` (see `index/vocabulary_merger/QueueWord.h`) is the
// owning counterpart of the `MergeWord` below, which the current merging
// pipeline consumes. A follow-up PR switches the merge to the `MergeBlock`s.
namespace ad_utility::vocabulary_merger::detail {

// An occurrence of a word in a partial vocabulary: the index of the partial
// vocabulary and the local index of the word in it.
using Occurrence = std::pair<uint32_t, uint64_t>;

// A word from a partial vocabulary, as the merge sees it. The word itself is a
// view into the storage of the `MergeBlock` that holds the `MergeWord`, so
// that reading and merging the words allocates nothing per word (a merge
// handles billions of them). While the partial vocabularies are merged, the
// further occurrences of the same word in other partial vocabularies are
// collected in the block (see `MergeBlock::addWord`), so that
// the words that leave the merge are distinct within a block, with one entry
// per occurrence.
struct MergeWord {
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

  // The memory footprint of a `MergeWord` including its word, which the
  // parallel merge needs to limit its memory consumption. Every word of a
  // `MergeBlock` owns a distinct slice of the storage of the block (no two
  // words share their bytes), so the sum over the words of a block is the
  // footprint of the block.
  static ad_utility::MemorySize memorySize(const MergeWord& word) {
    return ad_utility::MemorySize::bytes(sizeof(MergeWord) +
                                         word.iriOrLiteral().size());
  }

  // Read a word of the partial vocabulary `partialFileId` from the
  // `serializer`, in the format that `PartialVocabularyWriter` (see
  // `index/vocabulary_merger/PartialVocabularyFile.h`) writes: the word as a
  // string, its external flag and its local index. The word is not copied, but
  // points into the buffer of the `serializer`, which therefore has to outlive
  // the returned `MergeWord` (and must not move its buffer).
  CPP_template(typename S)(
      requires serialization::SpanReadSerializer<S>) static MergeWord
      fromZeroCopyDeserializer(S& serializer, uint32_t partialFileId) {
    MergeWord word;
    word.word_ = serialization::zeroCopyDeserializeToStringView(serializer);
    serializer >> word.isExternal_;
    serializer >> word.index_;
    word.partialFileId_ = partialFileId;
    return word;
  }
};

// A block of `MergeWord`s together with the storage that their words point
// into: either the serialized bytes of a block of a partial vocabulary file
// (see `PartialVocabularyRunsInput::getBlock`, which parses the words in place)
// or, for a block of merged words, a monotonic buffer that the words are copied
// into. The storage never moves once a word points into it, so the block can
// be moved, but not copied. The block is a random-access range of its words.
class MergeBlock : public ad_utility::NoCopy {
 public:
  using Occurrence = detail::Occurrence;
  using iterator = std::vector<MergeWord>::iterator;
  using const_iterator = std::vector<MergeWord>::const_iterator;
  // The buffer of the bytes of a block from a file. The bytes are not zeroed
  // when the buffer is created, because the read from the file overwrites
  // them anyway.
  using Buffer = ad_utility::UninitializedVector<char>;

 private:
  // The size of the first chunk of the `mergedWords_` buffer below.
  static constexpr ad_utility::MemorySize initialBufferSize_ =
      ad_utility::MemorySize::megabytes(1);

  std::vector<MergeWord> words_;
  std::vector<Occurrence> moreOccurrences_;
  // The bytes of a block that was read from a file, see the constructor below.
  Buffer bytesFromFile_;
  // The storage of the words of a block of merged words, see `addWord`. A
  // monotonic buffer allocates in chunks and never moves or frees what it has
  // allocated (until `release`), so the views into it stay valid. It lives on
  // the heap, so that the block can be moved. Constructing it allocates no
  // chunk yet, the first chunk is only allocated by the first `store`.
  std::unique_ptr<ql::pmr::monotonic_buffer_resource> mergedWords_ =
      std::make_unique<ql::pmr::monotonic_buffer_resource>(
          initialBufferSize_.getBytes());

 public:
  MergeBlock() = default;

  // Construct a block over the serialized `bytes` of `numWords` words of the
  // partial vocabulary `partialFileId`, in the format that
  // `PartialVocabularyWriter` writes (see `MergeWord::fromZeroCopyDeserializer`
  // above). The words are parsed in place, nothing is copied.
  MergeBlock(Buffer bytes, size_t numWords, uint32_t partialFileId)
      : bytesFromFile_{std::move(bytes)} {
    const auto& buffer = bytesFromFile_;
    words_.reserve(numWords);
    serialization::ByteBufferReadSerializerT serializer{
        ql::span<const char>{buffer.data(), buffer.size()}};
    for (size_t i = 0; i < numWords; ++i) {
      words_.push_back(
          MergeWord::fromZeroCopyDeserializer(serializer, partialFileId));
    }
    AD_CORRECTNESS_CHECK(serializer.getCurrentPosition() == buffer.size(),
                         "Trailing bytes in a block of a partial vocabulary");
  }

  // Add a merged `word` that the merge yields (a copy of it, the `word`
  // itself lives in an input block, so it must not have further occurrences).
  // A word that is equal to the last word of this block (the same word from
  // another partial vocabulary; the merge yields equal words consecutively) is
  // not appended, but recorded as a further occurrence of that last word, and
  // the external flag of the last word becomes the OR of the two. This removes
  // most of the duplicates on the threads of the merge, so that the consumer
  // of the merged blocks only sees the duplicates at the block boundaries.
  void addWord(const MergeWord& word) {
    AD_CORRECTNESS_CHECK(word.numMoreOccurrences_ == 0);
    if (!words_.empty() &&
        words_.back().iriOrLiteral() == word.iriOrLiteral()) {
      words_.back().isExternal_ = words_.back().isExternal_ || word.isExternal_;
      addOccurrenceToLastWord(word.partialFileId_, word.id());
      return;
    }
    MergeWord copy;
    copy.word_ = store(word.iriOrLiteral());
    copy.isExternal_ = word.isExternal();
    copy.partialFileId_ = word.partialFileId_;
    copy.index_ = word.id();
    words_.push_back(copy);
  }

  // The further occurrences of a `word` of this block, see above.
  ql::span<const Occurrence> moreOccurrences(const MergeWord& word) const {
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
  MergeWord& operator[](size_t i) { return words_[i]; }
  const MergeWord& operator[](size_t i) const { return words_[i]; }
  MergeWord& front() { return words_.front(); }
  const MergeWord& front() const { return words_.front(); }
  MergeWord& back() { return words_.back(); }
  const MergeWord& back() const { return words_.back(); }

  // Release the words and the storage.
  void clear() {
    words_.clear();
    moreOccurrences_.clear();
    bytesFromFile_.clear();
    bytesFromFile_.shrink_to_fit();
    mergedWords_->release();
  }

 private:
  // Record a further occurrence of the last word of the block, see `addWord`.
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

  // Copy the `word` into `mergedWords_` and return the view of the copy.
  std::string_view store(std::string_view word) {
    auto* copy =
        static_cast<char*>(mergedWords_->allocate(word.size(), alignof(char)));
    std::copy(word.begin(), word.end(), copy);
    return {copy, word.size()};
  }
};
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_MERGEBLOCK_H
