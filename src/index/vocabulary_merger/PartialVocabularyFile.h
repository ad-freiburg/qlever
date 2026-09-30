// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_PARTIALVOCABULARYFILE_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_PARTIALVOCABULARYFILE_H

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "index/ConstantsIndexBuilding.h"
#include "index/IndexBuilderTypes.h"
#include "index/PartialVocabularyFilenames.h"
#include "index/vocabulary_merger/QueueWord.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/File.h"
#include "util/MemorySize/MemorySize.h"
#include "util/NoCopyNoMove.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"

// The file format of a partial vocabulary (see `partialVocabularyWordsFilename`
// in `index/PartialVocabularyFilenames.h`), with its writer and the input
// policy that lets the parallel block merge (see
// `util/parallelBlockMerge/ParallelBlockMerge.h`) merge the partial
// vocabularies. The format is:
//
//   uint64_t numWords
//   the words, each as a `TripleComponentWithIndex` (word, isExternal, local
//     index), in the order of the vocabulary, grouped into blocks of about
//     `PARTIAL_VOCAB_BLOCK_SIZE` bytes
//   the block index: uint64_t numBlocks, then per block its `BlockMetadata`
//   uint64_t offsetOfBlockIndex
//
// The words are stored exactly as before the block index existed, so a reader
// that only wants the words (`numWords`, then that many words) still works. The
// block index is what the merge needs: the first and the last word of every
// block, without reading the block, so that it can split the runs into
// independent chunks by their words alone and then read only the blocks that a
// chunk needs, see `PartialVocabularyRunsInput`.
namespace ad_utility::vocabulary_merger {

// The metadata of a single block of words, see above. The first and the last
// word are stored in full, because the merge compares them with the words of
// other partial vocabularies.
struct PartialVocabularyBlockMetadata {
  uint64_t offset_ = 0;
  uint64_t numBytes_ = 0;
  uint64_t numWords_ = 0;
  TripleComponentWithIndex first_;
  TripleComponentWithIndex last_;

  AD_SERIALIZE_FRIEND_FUNCTION(PartialVocabularyBlockMetadata) {
    serializer | arg.offset_;
    serializer | arg.numBytes_;
    serializer | arg.numWords_;
    serializer | arg.first_;
    serializer | arg.last_;
  }
};

// Write a partial vocabulary file. The words have to be pushed in the order of
// the vocabulary, each with its local index and its external flag, and
// `finish()` (which the destructor calls if necessary) writes the block index.
class PartialVocabularyWriter : public ad_utility::NoCopyNoMove {
 private:
  serialization::FileWriteSerializer file_;
  // The block that is currently being filled, serialized.
  serialization::ByteBufferWriteSerializer currentBlock_;
  std::vector<PartialVocabularyBlockMetadata> blocks_;
  // The first and the last word of the current block.
  TripleComponentWithIndex firstWordOfBlock_;
  TripleComponentWithIndex lastWordOfBlock_;
  size_t numWordsInBlock_ = 0;
  uint64_t numWords_ = 0;
  // The offset in the file at which the current block starts.
  uint64_t offset_ = 0;
  bool finishWasCalled_ = false;

 public:
  explicit PartialVocabularyWriter(const std::string& filename)
      : file_{filename} {
    // The number of words is only known at the end and is patched in by
    // `finish()`, see there.
    file_ << numWords_;
    offset_ = sizeof(numWords_);
  }

  ~PartialVocabularyWriter() {
    if (!finishWasCalled_) {
      ad_utility::terminateIfThrows(
          [this]() { finish(); },
          "Calling `finish` from the destructor of `PartialVocabularyWriter`");
    }
  }

  // Add the next word.
  void operator()(std::string_view word, bool isExternal, uint64_t localIndex) {
    AD_CONTRACT_CHECK(!finishWasCalled_);
    currentBlock_ << word;
    currentBlock_ << isExternal;
    currentBlock_ << localIndex;
    if (numWordsInBlock_ == 0) {
      firstWordOfBlock_ = {std::string{word}, isExternal, localIndex};
    }
    lastWordOfBlock_.iriOrLiteral_ = word;
    lastWordOfBlock_.isExternal_ = isExternal;
    lastWordOfBlock_.index_ = localIndex;
    ++numWordsInBlock_;
    ++numWords_;
    if (currentBlock_.getCurrentPosition() >=
        PARTIAL_VOCAB_BLOCK_SIZE.getBytes()) {
      finishBlock();
    }
  }

  // Write the last block, the block index and its offset, then close the file.
  void finish() {
    if (finishWasCalled_) {
      return;
    }
    finishWasCalled_ = true;
    finishBlock();
    uint64_t offsetOfBlockIndex = offset_;
    file_ << blocks_;
    file_ << offsetOfBlockIndex;
    // Patch in the number of words at the very beginning of the file.
    file_.setSerializationPosition(0);
    file_ << numWords_;
    file_.close();
  }

 private:
  // Write the current block (if it has any words) and store its metadata.
  void finishBlock() {
    if (numWordsInBlock_ == 0) {
      return;
    }
    const auto& bytes = currentBlock_.data();
    file_.serializeBytes(bytes.data(), bytes.size());
    blocks_.push_back({offset_, bytes.size(), numWordsInBlock_,
                       std::move(firstWordOfBlock_), lastWordOfBlock_});
    offset_ += bytes.size();
    currentBlock_.clear();
    numWordsInBlock_ = 0;
  }
};

// The input policy of the parallel block merge (see
// `util/parallelBlockMerge/RunsInputPolicy.h`) over the partial vocabularies
// `partialVocabularyWordsFilename(basename, i)` for `i` in
// `[0, numPartialVocabularies)`: each partial vocabulary is a run, and its
// blocks are the blocks of the file. The block index of every file is read
// once, when this object is constructed; the blocks themselves are read only
// on demand by `getBlock`, which is thread-safe (a `pread` on a file that is
// never written to).
class PartialVocabularyRunsInput : public ad_utility::NoCopy {
 public:
  using Element = detail::QueueWord;
  using Block = detail::MergeBlock;

 private:
  // The metadata of a block, with the first and the last word already in the
  // form that the merge compares. Their words point into the
  // `boundaryWords_` of the run.
  struct BlockInfo {
    uint64_t offset_;
    uint64_t numBytes_;
    uint64_t numWords_;
    Element first_;
    Element last_;
  };
  struct Run {
    ad_utility::File file_;
    std::vector<BlockInfo> blocks_;
    // The first and the last words of all blocks, back to back. This is a
    // `std::vector` and not a `std::string`, because it must not move its
    // bytes when the `Run` is moved (a short string would).
    std::vector<char> boundaryWords_;
  };
  std::vector<Run> runs_;
  // The buffers of the blocks, see `detail::BufferPool`: a block of a file is
  // a bit larger than `PARTIAL_VOCAB_BLOCK_SIZE` (its last word crosses that
  // size), a block of merged words holds at most
  // `VOCAB_MERGER_WORD_BATCH_MEMORY_SIZE`.
  std::shared_ptr<detail::BufferPool> pool_ =
      std::make_shared<detail::BufferPool>(
          PARTIAL_VOCAB_BLOCK_SIZE.getBytes() * 5 / 4,
          VOCAB_MERGER_WORD_BATCH_MEMORY_SIZE.getBytes() / 8,
          /*maxNumBuffers=*/1u << 16);

 public:
  PartialVocabularyRunsInput(const std::string& basename,
                             size_t numPartialVocabularies) {
    runs_.reserve(numPartialVocabularies);
    for (size_t i = 0; i < numPartialVocabularies; ++i) {
      runs_.push_back(openRun(partialVocabularyWordsFilename(basename, i), i));
    }
  }

  PartialVocabularyRunsInput(PartialVocabularyRunsInput&&) noexcept = default;
  PartialVocabularyRunsInput& operator=(PartialVocabularyRunsInput&&) noexcept =
      default;

  size_t numRuns() const { return runs_.size(); }

  size_t numBlocks(size_t run) const { return runs_.at(run).blocks_.size(); }

  size_t numElementsInBlock(size_t run, size_t block) const {
    return runs_.at(run).blocks_.at(block).numWords_;
  }

  // The size of a block in the file. This is also the weight of the block for
  // the chunk boundaries of the merge (see `blockWeight` in
  // `util/parallelBlockMerge/MergeHelpersImpl.h`): the cost of merging a word
  // grows with its length, so chunks of equal size in bytes cost about the
  // same, while chunks of equal numbers of words do not (a chunk of long
  // literals costs many times a chunk of short IRIs, and the consumer, which
  // takes the chunks in order, then waits for it while the merge runs ahead).
  size_t numBytesInBlock(size_t run, size_t block) const {
    return runs_.at(run).blocks_.at(block).numBytes_;
  }
  size_t blockWeight(size_t run, size_t block) const {
    return numBytesInBlock(run, block);
  }

  const Element& firstElement(size_t run, size_t block) const {
    return runs_.at(run).blocks_.at(block).first_;
  }

  const Element& lastElement(size_t run, size_t block) const {
    return runs_.at(run).blocks_.at(block).last_;
  }

  // Read a block. The words are parsed in place, see `MergeBlock`.
  Block getBlock(size_t run, size_t block) const {
    const auto& info = runs_.at(run).blocks_.at(block);
    std::vector<char> bytes = pool_->get(info.numBytes_);
    bytes.resize(info.numBytes_);
    auto numBytesRead = runs_.at(run).file_.read(
        bytes.data(), info.numBytes_, static_cast<off_t>(info.offset_));
    AD_CORRECTNESS_CHECK(numBytesRead >= 0 &&
                         static_cast<uint64_t>(numBytesRead) == info.numBytes_);
    return Block{std::move(bytes), info.numWords_, static_cast<uint32_t>(run),
                 pool_};
  }

  Block makeEmptyBlock() const { return Block{pool_}; }

  // Append a merged `word` to an output `block` of the merge (a copy of the
  // word, the `word` itself lives in an input block). A word that is equal to
  // the last word of the block (the same word from another partial
  // vocabulary; the merge yields equal words consecutively) is not appended,
  // but recorded as a further occurrence of that last word, see
  // `MergeBlock::addOccurrenceToLastWord`. This removes most of the
  // duplicates on the threads of the merge, so that the single thread behind
  // the merge only sees the duplicates at the block boundaries.
  void appendToBlock(Block& block, const Element& word) const {
    if (!block.empty() && block.back().iriOrLiteral() == word.iriOrLiteral()) {
      AD_CORRECTNESS_CHECK(word.numMoreOccurrences_ == 0);
      block.back().isExternal() =
          block.back().isExternal() || word.isExternal();
      block.addOccurrenceToLastWord(word.partialFileId_, word.id());
      return;
    }
    block.push(word);
  }

  MemorySize memorySizeOfElement(const Element& word) const {
    return detail::sizeOfQueueWord(word);
  }

 private:
  // Open a partial vocabulary file and read its block index.
  static Run openRun(const std::string& filename, size_t runIdx) {
    Run run{ad_utility::File{filename, "r"}, {}, {}};
    uint64_t sizeOfFile = run.file_.sizeOfFile();
    AD_CONTRACT_CHECK(sizeOfFile >= 2 * sizeof(uint64_t),
                      "The partial vocabulary file ", filename,
                      " is too small to hold a block index");
    uint64_t offsetOfBlockIndex = 0;
    auto readExactly = [&run, &filename](void* target, uint64_t numBytes,
                                         uint64_t offset) {
      auto numBytesRead =
          run.file_.read(target, numBytes, static_cast<off_t>(offset));
      AD_CORRECTNESS_CHECK(
          numBytesRead >= 0 && static_cast<uint64_t>(numBytesRead) == numBytes,
          "Could not read the block index of the partial "
          "vocabulary file ",
          filename);
    };
    readExactly(&offsetOfBlockIndex, sizeof(offsetOfBlockIndex),
                sizeOfFile - sizeof(offsetOfBlockIndex));
    AD_CONTRACT_CHECK(offsetOfBlockIndex >= sizeof(uint64_t) &&
                          offsetOfBlockIndex + sizeof(uint64_t) <= sizeOfFile,
                      "The partial vocabulary file ", filename,
                      " has no valid block index");
    std::vector<char> bytes(sizeOfFile - sizeof(offsetOfBlockIndex) -
                            offsetOfBlockIndex);
    readExactly(bytes.data(), bytes.size(), offsetOfBlockIndex);
    serialization::ByteBufferReadSerializer reader{std::move(bytes)};
    std::vector<PartialVocabularyBlockMetadata> blocks;
    reader >> blocks;
    // The first and the last words of the blocks are stored back to back in
    // `boundaryWords_`, which is allocated once and never resized afterwards,
    // so that the `Element`s can point into it.
    size_t numBoundaryBytes = 0;
    for (const auto& block : blocks) {
      numBoundaryBytes += block.first_.iriOrLiteral().size() +
                          block.last_.iriOrLiteral().size();
    }
    run.boundaryWords_.reserve(numBoundaryBytes);
    auto makeElement = [&run, runIdx](const TripleComponentWithIndex& word) {
      auto& storage = run.boundaryWords_;
      size_t offset = storage.size();
      storage.insert(storage.end(), word.iriOrLiteral().begin(),
                     word.iriOrLiteral().end());
      Element element;
      element.word_ = {storage.data() + offset, word.iriOrLiteral().size()};
      element.isExternal_ = word.isExternal();
      element.index_ = word.index_;
      element.partialFileId_ = static_cast<uint32_t>(runIdx);
      return element;
    };
    run.blocks_.reserve(blocks.size());
    for (const auto& block : blocks) {
      AD_CORRECTNESS_CHECK(block.numWords_ > 0);
      run.blocks_.push_back({block.offset_, block.numBytes_, block.numWords_,
                             makeElement(block.first_),
                             makeElement(block.last_)});
    }
    AD_CORRECTNESS_CHECK(run.boundaryWords_.size() == numBoundaryBytes);
    return run;
  }
};

}  // namespace ad_utility::vocabulary_merger

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_PARTIALVOCABULARYFILE_H
