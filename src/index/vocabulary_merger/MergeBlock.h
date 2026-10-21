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
#include "index/ConstantsIndexBuilding.h"
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
// that hold them, see `index/vocabulary_merger/PartialVocabularyFile.h`. This
// is not part of the public interface of `index/VocabularyMerger.h`.
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

// A thread-safe pool of the buffers that the `MergeBlock`s below store their
// words in, so that the merge reuses them instead of allocating and freeing
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

// The two pools of the buffers of the `MergeBlock`s below: for the bytes of
// the blocks of the partial vocabulary files, and for the storage of the
// blocks of merged words. They are separate because their buffers differ in
// size by more than an order of magnitude (see
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

// A block of `MergeWord`s together with the storage that their words point
// into: either the serialized bytes of a block of a partial vocabulary file
// (see `PartialVocabularyRunsInput::getBlock`, which parses the words in place)
// or, for a block of merged words, buffers that the words are copied into. The
// buffers never move once a word points into them, so the block can be moved,
// but not copied. The block is a random-access range of its words. The
// buffers come from and go back to the `BufferPools` if the block has them.
class MergeBlock : public ad_utility::NoCopy {
 public:
  using iterator = std::vector<MergeWord>::iterator;
  using const_iterator = std::vector<MergeWord>::const_iterator;
  using Buffer = BufferPool::Buffer;

 private:
  using Buffers = std::vector<Buffer>;
  // The cleanup of the `buffers_` below: give them back to the pools, or do
  // nothing if the block has no pools (the buffers are then simply freed). It
  // owns a copy of everything it needs (and not pointers into the block), so
  // that it stays valid when the block is moved.
  struct GiveBackToPools {
    std::shared_ptr<BufferPools> pools_;
    // Whether the first buffer holds the bytes of a block of a file (and hence
    // goes back to the pool of those). All other buffers hold merged words.
    bool firstBufferIsFromFile_ = false;

    void operator()(Buffers&& buffers) const {
      if (pools_ == nullptr) {
        return;
      }
      for (size_t i = 0; i < buffers.size(); ++i) {
        auto& pool = (i == 0 && firstBufferIsFromFile_) ? pools_->fileBlocks_
                                                        : pools_->mergedWords_;
        pool.giveBack(std::move(buffers[i]));
      }
    }
  };
  using OwnedBuffers =
      ad_utility::unique_cleanup::UniqueCleanup<Buffers, GiveBackToPools>;

  std::vector<MergeWord> words_;
  // The pools that the buffers of a block of merged words are taken from (see
  // `store`), `nullptr` for a block without pools.
  std::shared_ptr<BufferPools> pools_;
  // The bytes of a block that was read from a file (see the constructor
  // below), or the buffers that the words of a block of merged words are
  // copied into (see `push`). A word is never split across two buffers, and a
  // buffer is never reallocated (it is reserved once and filled up to its
  // capacity), so the views into it stay valid.
  //
  // NOTE: The `UniqueCleanup` gives the buffers back to the pools when the
  // block is destroyed, overwritten by a move assignment, or cleared (see
  // `clear`), and does nothing for a moved-from block, so the move operations
  // below can be defaulted.
  OwnedBuffers buffers_{Buffers{}, GiveBackToPools{}};

 public:
  MergeBlock() = default;
  explicit MergeBlock(std::shared_ptr<BufferPools> pools)
      : pools_{std::move(pools)},
        buffers_{Buffers{}, GiveBackToPools{pools_, false}} {}
  MergeBlock(MergeBlock&&) noexcept = default;
  MergeBlock& operator=(MergeBlock&&) noexcept = default;

  // Construct a block over the serialized `bytes` of `numWords` words of the
  // partial vocabulary `partialFileId`, in the format that
  // `PartialVocabularyWriter` writes (see `MergeWord::fromZeroCopyDeserializer`
  // above). The words are parsed in place, nothing is copied. The `bytes` go
  // back to the `pools` when the block is cleared or destroyed. The `pools`
  // may be `nullptr` (for example in tests): the block then works just the
  // same, only its buffers are freed instead of given back.
  MergeBlock(Buffer bytes, size_t numWords, uint32_t partialFileId,
             std::shared_ptr<BufferPools> pools = nullptr)
      : pools_{std::move(pools)},
        buffers_{Buffers{}, GiveBackToPools{pools_, true}} {
    buffers_->push_back(std::move(bytes));
    const auto& buffer = buffers_->back();
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

  // Release the words and the storage (the buffers go back to the pools). The
  // block stays usable, for merged words.
  void clear() {
    words_.clear();
    // The move assignment runs the cleanup of the old buffers.
    buffers_ = OwnedBuffers{Buffers{}, GiveBackToPools{pools_, false}};
  }

 private:
  // Copy the `word` into the buffers and return the view of the copy.
  std::string_view store(std::string_view word) {
    Buffers& buffers = *buffers_;
    if (buffers.empty() ||
        buffers.back().size() + word.size() > buffers.back().capacity()) {
      if (pools_ != nullptr) {
        buffers.push_back(pools_->mergedWords_.take(word.size()));
      } else {
        // Without pools, the buffers have the same size as with them. A word
        // that is longer gets a buffer of its own.
        buffers.emplace_back().reserve(std::max(
            VOCAB_MERGER_MERGED_WORDS_BUFFER_SIZE.getBytes(), word.size()));
      }
    }
    auto& buffer = buffers.back();
    size_t offset = buffer.size();
    buffer.insert(buffer.end(), word.begin(), word.end());
    return {buffer.data() + offset, word.size()};
  }
};
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_MERGEBLOCK_H
