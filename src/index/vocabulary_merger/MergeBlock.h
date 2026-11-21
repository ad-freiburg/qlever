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

#include "backports/span.h"
#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"
#include "util/NoCopyNoMove.h"
#include "util/RecyclingPool.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/UninitializedAllocator.h"
#include "util/UniqueCleanup.h"

// The words of the partial vocabularies as the parallel block merge (see
// `util/parallelBlockMerge/ParallelBlockMerge.h`) sees them, and the blocks
// that hold them, see `index/vocabulary_merger/PartialVocabularyFile.h`. The
// input and the output of the merge have different words and blocks:
//
// - The input consists of `InputBlock`s of `InputWord`s, which the merge reads
//   from the partial vocabulary files and compares. A `InputWord` is a single
//   occurrence of a word in a single partial vocabulary.
// - The output consists of `MergedBlock`s of `MergedWord`s. A `MergedWord`
//   holds the list of all the occurrences (from all the partial vocabularies)
//   that the merge folded into it, see `MergedBlock::addWord`.
//
// The buffers of both kinds of blocks come from and go back to the
// `BufferPools` below, which every block has. This is not part of the public
// interface of `index/VocabularyMerger.h`.
namespace ad_utility::vocabulary_merger::detail {

// An occurrence of a word in a partial vocabulary: the index of the partial
// vocabulary and the local index of the word in it.
using Occurrence = std::pair<uint32_t, uint64_t>;

// A word of a partial vocabulary, as the merge reads and compares it: the
// occurrence of the word in the partial vocabulary `partialFileId_` at the
// local index `index_`, with its external flag. The word itself is a view into
// the storage of the `InputBlock` that holds the `InputWord`, so that reading
// and merging the words allocates nothing per word (a merge handles billions
// of them).
struct InputWord {
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
  // the returned `InputWord` (and must not move its buffer).
  CPP_template(typename S)(requires serialization::SpanReadSerializer<S>)
  static InputWord
      fromZeroCopyDeserializer(S& serializer, uint32_t partialFileId) {
    InputWord word;
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
// `InputWord`s of all the partial vocabularies into one `MergedWord` (see
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

  // Construct a merged word with a single occurrence so far: the `word` (which
  // has to point into the storage of the block), its external flag, and the
  // position `occurrencesOffset` of that occurrence in the `occurrences_` of
  // the block.
  MergedWord(std::string_view word, bool isExternal, uint64_t occurrencesOffset)
      : word_{word},
        occurrencesOffset_{occurrencesOffset},
        numOccurrences_{1},
        isExternal_{isExternal} {}

  [[nodiscard]] bool& isExternal() { return isExternal_; }
  [[nodiscard]] const bool& isExternal() const { return isExternal_; }
  [[nodiscard]] std::string_view iriOrLiteral() const { return word_; }
};

// The words are stored in huge vectors (one entry per occurrence of a word in
// a partial vocabulary), so an accidental growth of these structs would cost
// real memory. Both consist of a `string_view`, a `uint64_t`, a `uint32_t` and
// a `bool`, which pack into 32 bytes.
static_assert(sizeof(InputWord) == 32);
static_assert(sizeof(MergedWord) == 32);

// A thread-safe pool of the buffers that the blocks below store their bytes
// or words in, so that the merge reuses them instead of allocating and freeing
// one per block. All the buffers of a pool have the same `capacity`. A request
// for a larger buffer (a block with a very long word) is served by a larger
// one, which is replaced by a fresh buffer of the pool's capacity when it is
// given back. How many buffers the pool keeps is up to the
// `ad_utility::RecyclingPool`: never more than were taken at the same time.
//
// NOTE: With an allocator that returns freed memory to the operating system
// (immediately or after a short decay, depending on the allocator and on the
// size of the allocation), the merge of billions of words otherwise spends a
// quarter of the machine in the kernel on allocating and freeing the buffers.
class BufferPool {
 public:
  // The bytes are not zeroed when a buffer is created or resized, because
  // they are overwritten anyway (by the read from the file or by the words).
  using Buffer = ad_utility::UninitializedVector<char>;

 private:
  size_t capacity_;
  ad_utility::RecyclingPool<Buffer> pool_;

 public:
  explicit BufferPool(size_t capacity) : capacity_{capacity} {}

  // Get an empty buffer with a capacity of at least `minCapacity` (the
  // capacity of the pool, unless more is needed).
  Buffer take(size_t minCapacity = 0) {
    const size_t capacity = std::max(capacity_, minCapacity);
    Buffer buffer = pool_.take([capacity]() {
      Buffer fresh;
      fresh.reserve(capacity);
      return fresh;
    });
    buffer.clear();
    buffer.reserve(capacity);
    return buffer;
  }

  // Give a buffer back. A buffer that is larger than the capacity of the pool
  // (see `take`) is replaced by a fresh one of that capacity, so that the
  // memory of the pool stays bounded by its number of buffers times the
  // capacity, and not times the longest word that was ever seen (thousands of
  // buffers may be in the pool, and a single literal can be hundreds of
  // kilobytes long). A smaller buffer is grown by the next `take`.
  void giveBack(Buffer buffer) {
    buffer.clear();
    if (buffer.capacity() > capacity_) {
      buffer.shrink_to_fit();
      buffer.reserve(capacity_);
    }
    pool_.giveBack(std::move(buffer));
  }
};

// The two pools of the buffers of the blocks below: for the bytes of the
// `InputBlock`s of the partial vocabulary files, and for the storage of the
// words of the `MergedBlock`s. They are separate because their buffers differ
// in size by more than an order of magnitude (see
// `VOCAB_MERGER_FILE_BLOCK_BUFFER_SIZE` and
// `VOCAB_MERGER_MERGED_WORDS_BUFFER_SIZE` in `index/ConstantsIndexBuilding.h`)
// and the pools keep their buffers' memory touched: with a single pool of the
// larger size, each of the many blocks of files in flight (one per partial
// vocabulary and chunk) would hold the resident memory of a block of merged
// words.
struct BufferPools {
  BufferPool fileBlocks_;
  BufferPool mergedWords_;

  BufferPools(size_t fileBlockCapacity, size_t mergedWordsCapacity)
      : fileBlocks_{fileBlockCapacity}, mergedWords_{mergedWordsCapacity} {}
};

// Return the `pools`, which must not be `nullptr` (every block has pools).
inline std::shared_ptr<BufferPools> checkedBufferPools(
    std::shared_ptr<BufferPools> pools) {
  AD_CONTRACT_CHECK(pools != nullptr);
  return pools;
}

// The part that the two kinds of blocks below share: a block is a
// random-access range of its words.
template <typename Word>
class BlockOfWords {
 public:
  using iterator = typename std::vector<Word>::iterator;
  using const_iterator = typename std::vector<Word>::const_iterator;

 private:
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

 protected:
  // The words, for the derived blocks to fill.
  std::vector<Word>& words() { return words_; }
  const std::vector<Word>& words() const { return words_; }
};

// A block of a partial vocabulary file, as the merge reads it: the serialized
// bytes of the block (see `PartialVocabularyRunsInput::getBlock`) and the
// `InputWord`s that were parsed in place and point into these bytes. The bytes
// never move once a word points into them, so the block can be moved, but not
// copied. The bytes go back to the pool of the file blocks when the block is
// destroyed.
class InputBlock : public BlockOfWords<InputWord>, public ad_utility::NoCopy {
 public:
  using Buffer = BufferPool::Buffer;

 private:
  // The cleanup of the `bytes_` below: give them back to the pool. It owns
  // the pools (and not a pointer into the block), so that it stays valid when
  // the block is moved.
  struct GiveBackToPool {
    std::shared_ptr<BufferPools> pools_;
    void operator()(Buffer&& bytes) const {
      pools_->fileBlocks_.giveBack(std::move(bytes));
    }
  };
  // NOTE: The `UniqueCleanup` gives the bytes back when the block is
  // destroyed or overwritten by a move assignment, and does nothing for a
  // moved-from block, so the implicit move operations are correct.
  ad_utility::unique_cleanup::UniqueCleanup<Buffer, GiveBackToPool> bytes_;

 public:
  // Construct a block over the serialized `bytes` of `numWords` words of the
  // partial vocabulary `partialFileId`, in the format that
  // `PartialVocabularyWriter` writes (see `InputWord::fromZeroCopyDeserializer`
  // above). The words are parsed in place, nothing is copied. The `bytes` go
  // back to the `pools` (which must not be `nullptr`) when the block is
  // destroyed.
  InputBlock(Buffer bytes, size_t numWords, uint32_t partialFileId,
             std::shared_ptr<BufferPools> pools)
      : bytes_{std::move(bytes),
               GiveBackToPool{checkedBufferPools(std::move(pools))}} {
    words().reserve(numWords);
    serialization::ByteBufferReadSerializerT serializer{
        ql::span<const char>{bytes_->data(), bytes_->size()}};
    for (size_t i = 0; i < numWords; ++i) {
      words().push_back(
          InputWord::fromZeroCopyDeserializer(serializer, partialFileId));
    }
    AD_CORRECTNESS_CHECK(serializer.getCurrentPosition() == bytes_->size(),
                         "Trailing bytes in a block of a partial vocabulary");
  }
};

// A block of merged words, as the merge yields it: the `MergedWord`s, the
// buffers that their words were copied into, and their occurrences. The
// buffers never move once a word points into them, so the block can be moved,
// but not copied. The buffers come from and go back to the pool of the merged
// words.
class MergedBlock : public BlockOfWords<MergedWord>, public ad_utility::NoCopy {
 public:
  using Occurrence = detail::Occurrence;
  using Buffer = BufferPool::Buffer;

 private:
  using Buffers = std::vector<Buffer>;
  // The cleanup of the `buffers_` below: give them back to the pool. It owns
  // the pools (and not a pointer into the block), so that it stays valid when
  // the block is moved.
  struct GiveBackToPool {
    std::shared_ptr<BufferPools> pools_;
    void operator()(Buffers&& buffers) const {
      for (auto& buffer : buffers) {
        pools_->mergedWords_.giveBack(std::move(buffer));
      }
    }
  };
  using OwnedBuffers =
      ad_utility::unique_cleanup::UniqueCleanup<Buffers, GiveBackToPool>;

  // The occurrences of all the words, see `MergedWord::occurrencesOffset_`.
  std::vector<Occurrence> occurrences_;
  // The pools that the `buffers_` are taken from, see `store`.
  std::shared_ptr<BufferPools> pools_;
  // The buffers that the words are copied into, see `store`. A word is never
  // split across two buffers, and a buffer is never reallocated (it is
  // reserved once and filled up to its capacity), so the views into it stay
  // valid.
  //
  // NOTE: The `UniqueCleanup` gives the buffers back to the pool when the
  // block is destroyed, overwritten by a move assignment, or cleared (see
  // `clear`), and does nothing for a moved-from block, so the implicit move
  // operations are correct.
  OwnedBuffers buffers_;

 public:
  // Construct an empty block whose buffers come from the `pools` (which must
  // not be `nullptr`).
  explicit MergedBlock(std::shared_ptr<BufferPools> pools)
      : pools_{checkedBufferPools(std::move(pools))},
        buffers_{Buffers{}, GiveBackToPool{pools_}} {}

  // The memory that appending the `word` to a block occupies at most (see
  // `addWord`), which the parallel merge needs to limit the memory of its
  // output blocks. Every word of a block owns a distinct slice of the buffers
  // of the block (no two words share their bytes) and a distinct range of the
  // occurrences, so the sum over the words that were appended to a block is a
  // bound for the footprint of the block.
  //
  // NOTE: A `word` that `addWord` folds into the last word of the block only
  // costs an `Occurrence`, so for it this size is an overestimate, which is on
  // the safe side.
  static ad_utility::MemorySize memorySizeOfAppended(const InputWord& word) {
    return ad_utility::MemorySize::bytes(
        sizeof(MergedWord) + word.iriOrLiteral().size() + sizeof(Occurrence));
  }

  // Add a `word` that the merge yields (the `word` itself lives in an
  // `InputBlock`). A `word` that is equal to the last word of this block is
  // not appended, but added as a further occurrence of that word, whose
  // external flag becomes the OR of the two. Otherwise, the `word` is copied
  // into the buffers of this block and becomes a new `MergedWord` with this
  // single occurrence.
  //
  // NOTE: An equal word is the same word from another partial vocabulary, and
  // the merge yields equal words consecutively. The folding thus removes most
  // of the duplicates on the threads of the merge, and the consumer of the
  // merged blocks only sees the duplicates at the block boundaries.
  void addWord(const InputWord& word) {
    if (!empty() && back().iriOrLiteral() == word.iriOrLiteral()) {
      MergedWord& last = back();
      // The occurrences of the last word are the last ones in the vector.
      AD_CORRECTNESS_CHECK(last.occurrencesOffset_ + last.numOccurrences_ ==
                           occurrences_.size());
      last.isExternal_ = last.isExternal_ || word.isExternal();
      occurrences_.push_back(word.occurrence());
      ++last.numOccurrences_;
      return;
    }
    occurrences_.push_back(word.occurrence());
    words().emplace_back(store(word.iriOrLiteral()), word.isExternal(),
                         occurrences_.size() - 1);
  }

  // The occurrences of a `word` of this block.
  ql::span<const Occurrence> occurrences(const MergedWord& word) const {
    AD_CONTRACT_CHECK(word.occurrencesOffset_ + word.numOccurrences_ <=
                      occurrences_.size());
    return {occurrences_.data() + word.occurrencesOffset_,
            word.numOccurrences_};
  }

  // The number of occurrences of all the words of this block.
  size_t numOccurrences() const { return occurrences_.size(); }

  // Release the words, their occurrences and the buffers (which go back to
  // the pool). The block stays usable.
  void clear() {
    words().clear();
    occurrences_.clear();
    // The move assignment runs the cleanup of the old buffers.
    buffers_ = OwnedBuffers{Buffers{}, GiveBackToPool{pools_}};
  }

 private:
  // Copy the `word` into the buffers and return the view of the copy.
  std::string_view store(std::string_view word) {
    Buffers& buffers = *buffers_;
    if (buffers.empty() ||
        buffers.back().size() + word.size() > buffers.back().capacity()) {
      buffers.push_back(pools_->mergedWords_.take(word.size()));
    }
    auto& buffer = buffers.back();
    size_t offset = buffer.size();
    buffer.insert(buffer.end(), word.begin(), word.end());
    return {buffer.data() + offset, word.size()};
  }
};
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_MERGEBLOCK_H
