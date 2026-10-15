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
// that hold them, see `index/vocabulary_merger/PartialVocabularyFile.h`. The
// input and the output of the merge have different words and blocks:
//
// - The input consists of `InputBlock`s of `MergeWord`s, which the merge reads
//   from the partial vocabulary files and compares. A `MergeWord` is a single
//   occurrence of a word in a single partial vocabulary.
// - The output consists of `MergedBlock`s of `MergedWord`s. A `MergedWord`
//   holds the list of all the occurrences (from all the partial vocabularies)
//   that the merge folded into it, see `MergedBlock::addWord`.
//
// This is not part of the public interface of `index/VocabularyMerger.h`.
//
// NOTE: The `QueueWord` (see `index/vocabulary_merger/QueueWord.h`) is the
// owning counterpart of the `MergeWord` below; the current merging pipeline
// still consumes `QueueWord`s. A follow-up PR switches the merge to the blocks
// of this file.
namespace ad_utility::vocabulary_merger::detail {

// An occurrence of a word in a partial vocabulary: the index of the partial
// vocabulary and the local index of the word in it.
using Occurrence = std::pair<uint32_t, uint64_t>;

// A word of a partial vocabulary, as the merge reads and compares it: the
// occurrence of the word in the partial vocabulary `partialFileId_` at the
// local index `index_`, with its external flag. The word itself is a view into
// the storage of the `InputBlock` that holds the `MergeWord`, so that reading
// and merging the words allocates nothing per word (a merge handles billions
// of them).
struct MergeWord {
  // The word, a view into the block's storage.
  std::string_view word_;
  // The local index of the word in its partial vocabulary.
  uint64_t index_ = 0;
  // The partial vocabulary that the word came from.
  uint32_t partialFileId_ = 0;
  // Whether the word is to be externalized.
  bool isExternal_ = false;

  [[nodiscard]] bool isExternal() const { return isExternal_; }
  [[nodiscard]] std::string_view iriOrLiteral() const { return word_; }
  [[nodiscard]] const uint64_t& id() const { return index_; }

  // The occurrence that this word is.
  [[nodiscard]] Occurrence occurrence() const {
    return {partialFileId_, index_};
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

// A word of the merged vocabulary, as the merge yields it: the word (a view
// into the storage of the `MergedBlock` that holds the `MergedWord`), whether
// it is to be externalized (in any of its occurrences), and the range of its
// occurrences in the `occurrences()` of its block. The merge folds the equal
// `MergeWord`s of all the partial vocabularies into one `MergedWord` (see
// `MergedBlock::addWord`), so a `MergedWord` has at least one occurrence, and
// all of its occurrences are stored alike.
struct MergedWord {
  // The word, a view into the block's storage.
  std::string_view word_;
  // The range of the occurrences in the `occurrences_` of the block.
  uint64_t occurrencesOffset_ = 0;
  uint32_t numOccurrences_ = 0;
  // Whether the word is to be externalized (in any of its occurrences).
  bool isExternal_ = false;

  // Construct from the first occurrence `word` of the merged word, whose text
  // was already copied into the storage of the block as `storedWord`, and the
  // position `occurrencesOffset` at which that occurrence was stored in the
  // `occurrences_` of the block.
  MergedWord(const MergeWord& word, std::string_view storedWord,
             uint64_t occurrencesOffset)
      : word_{storedWord},
        occurrencesOffset_{occurrencesOffset},
        numOccurrences_{1},
        isExternal_{word.isExternal()} {}

  [[nodiscard]] bool& isExternal() { return isExternal_; }
  [[nodiscard]] const bool& isExternal() const { return isExternal_; }
  [[nodiscard]] std::string_view iriOrLiteral() const { return word_; }
};

// The part that the two kinds of blocks below share: a block is a
// random-access range of its words.
template <typename Word>
class BlockOfWords {
 public:
  using iterator = typename std::vector<Word>::iterator;
  using const_iterator = typename std::vector<Word>::const_iterator;

 protected:
  std::vector<Word> words_;

 public:
  iterator begin() { return words_.begin(); }
  iterator end() { return words_.end(); }
  const_iterator begin() const { return words_.begin(); }
  const_iterator end() const { return words_.end(); }
  size_t size() const { return words_.size(); }
  bool empty() const { return words_.empty(); }
  Word& operator[](size_t i) { return words_[i]; }
  const Word& operator[](size_t i) const { return words_[i]; }
  Word& front() { return words_.front(); }
  const Word& front() const { return words_.front(); }
  Word& back() { return words_.back(); }
  const Word& back() const { return words_.back(); }
};

// A block of a partial vocabulary file, as the merge reads it: the serialized
// bytes of the block (see `PartialVocabularyRunsInput::getBlock`) and the
// `MergeWord`s that were parsed in place and point into these bytes. The bytes
// never move once a word points into them, so the block can be moved, but not
// copied.
class InputBlock : public BlockOfWords<MergeWord>, public ad_utility::NoCopy {
 public:
  // The buffer of the bytes of a block from a file. The bytes are not zeroed
  // when the buffer is created, because the read from the file overwrites
  // them anyway.
  using Buffer = ad_utility::UninitializedVector<char>;

 private:
  Buffer bytes_;

 public:
  // Construct a block over the serialized `bytes` of `numWords` words of the
  // partial vocabulary `partialFileId`, in the format that
  // `PartialVocabularyWriter` writes (see `MergeWord::fromZeroCopyDeserializer`
  // above). The words are parsed in place, nothing is copied.
  InputBlock(Buffer bytes, size_t numWords, uint32_t partialFileId)
      : bytes_{std::move(bytes)} {
    words_.reserve(numWords);
    serialization::ByteBufferReadSerializerT serializer{
        ql::span<const char>{bytes_.data(), bytes_.size()}};
    for (size_t i = 0; i < numWords; ++i) {
      words_.push_back(
          MergeWord::fromZeroCopyDeserializer(serializer, partialFileId));
    }
    AD_CORRECTNESS_CHECK(serializer.getCurrentPosition() == bytes_.size(),
                         "Trailing bytes in a block of a partial vocabulary");
  }
};

// A block of merged words, as the merge yields it: the `MergedWord`s, the
// storage that their words were copied into, and their occurrences. The
// storage never moves once a word points into it, so the block can be moved,
// but not copied.
class MergedBlock : public BlockOfWords<MergedWord>, public ad_utility::NoCopy {
 public:
  using Occurrence = detail::Occurrence;

 private:
  // The size of the first chunk of the `storage_` below.
  static constexpr ad_utility::MemorySize initialStorageSize_ =
      ad_utility::MemorySize::megabytes(1);

  // The occurrences of all the words, see `MergedWord::occurrencesOffset_`.
  std::vector<Occurrence> occurrences_;
  // The storage of the words, see `store`. A monotonic buffer allocates in
  // chunks and never moves or frees what it has allocated (until `release`),
  // so the views into it stay valid. It lives on the heap, so that the block
  // can be moved. Constructing it allocates no chunk yet, the first chunk is
  // only allocated by the first `store`.
  std::unique_ptr<ql::pmr::monotonic_buffer_resource> storage_ =
      std::make_unique<ql::pmr::monotonic_buffer_resource>(
          initialStorageSize_.getBytes());

 public:
  MergedBlock() = default;

  // The memory that appending the `word` to a block occupies at most (see
  // `addWord`), which the parallel merge needs to limit the memory of its
  // output blocks. Every word of a block owns a distinct slice of the storage
  // of the block (no two words share their bytes) and a distinct range of the
  // occurrences, so the sum over the words that were appended to a block is a
  // bound for the footprint of the block.
  //
  // NOTE: A `word` that `addWord` folds into the last word of the block only
  // costs an `Occurrence`, so for it this size is an overestimate, which is on
  // the safe side.
  static ad_utility::MemorySize memorySizeOfAppended(const MergeWord& word) {
    return ad_utility::MemorySize::bytes(
        sizeof(MergedWord) + word.iriOrLiteral().size() + sizeof(Occurrence));
  }

  // Add a `word` that the merge yields (the `word` itself lives in an
  // `InputBlock`). A `word` that is equal to the last word of this block (the
  // same word from another partial vocabulary; the merge yields equal words
  // consecutively) is not appended, but its occurrence is added to that last
  // word, whose external flag becomes the OR of the two. Otherwise, the `word`
  // is copied into the storage of this block and becomes a new `MergedWord`
  // with this single occurrence. This removes most of the duplicates on the
  // threads of the merge, so that the consumer of the merged blocks only sees
  // the duplicates at the block boundaries.
  void addWord(const MergeWord& word) {
    if (!words_.empty() &&
        words_.back().iriOrLiteral() == word.iriOrLiteral()) {
      MergedWord& last = words_.back();
      // The occurrences of the last word are the last ones in the vector.
      AD_CORRECTNESS_CHECK(last.occurrencesOffset_ + last.numOccurrences_ ==
                           occurrences_.size());
      last.isExternal_ = last.isExternal_ || word.isExternal();
      occurrences_.push_back(word.occurrence());
      ++last.numOccurrences_;
      return;
    }
    occurrences_.push_back(word.occurrence());
    words_.emplace_back(word, store(word.iriOrLiteral()),
                        occurrences_.size() - 1);
  }

  // The occurrences of a `word` of this block.
  ql::span<const Occurrence> occurrences(const MergedWord& word) const {
    AD_CONTRACT_CHECK(word.occurrencesOffset_ + word.numOccurrences_ <=
                      occurrences_.size());
    return {occurrences_.data() + word.occurrencesOffset_,
            word.numOccurrences_};
  }

  // Release the words, their occurrences and the storage.
  void clear() {
    words_.clear();
    occurrences_.clear();
    storage_->release();
  }

 private:
  // Copy the `word` into the `storage_` and return the view of the copy.
  std::string_view store(std::string_view word) {
    auto* copy =
        static_cast<char*>(storage_->allocate(word.size(), alignof(char)));
    std::copy(word.begin(), word.end(), copy);
    return {copy, word.size()};
  }
};
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_MERGEBLOCK_H
