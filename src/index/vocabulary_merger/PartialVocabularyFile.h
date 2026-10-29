// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_PARTIALVOCABULARYFILE_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_PARTIALVOCABULARYFILE_H

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/memory.h"
#include "backports/span.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/IndexBuilderTypes.h"
#include "index/PartialVocabularyFilenames.h"
#include "index/vocabulary_merger/MergeBlock.h"
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
// vocabularies. A file consists of the following four parts, in this order:
//
// 1. The number of words, as a `uint64_t`.
// 2. The words, in the order of the vocabulary. Each word is serialized as a
//    `TripleComponentWithIndex`, that is, the word itself, its external flag
//    and its local index. The words are grouped into blocks: a block ends
//    after the word that makes it at least `PARTIAL_VOCAB_BLOCK_SIZE` bytes
//    long. The blocks are only visible through the block index (part 3), the
//    words themselves are stored back to back.
// 3. The block index: the number of blocks as a `uint64_t`, then the
//    `PartialVocabularyBlockMetadata` of every block.
// 4. The offset of the block index (part 3) in the file, as a `uint64_t`.
//
// The words are stored exactly as before the block index existed, so a reader
// that only wants the words (part 1, then that many words) still works. The
// block index is what the merge needs: the first and the last word of every
// block, without reading the block, so that it can split the runs into
// independent chunks by their words alone and then read only the blocks that a
// chunk needs, see `PartialVocabularyRunsInput`. This is how `mergeVocabulary`
// (see `index/VocabularyMerger.h`) merges the partial vocabularies.
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
// the vocabulary, each with its local index and its external flag (bundled in
// a `PartialVocabIndexWithExternalFlag`), and `finish()` (which the destructor
// calls if necessary) writes the block index.
class PartialVocabularyWriter : public ad_utility::NoCopyNoMove {
 private:
  serialization::FileWriteSerializer file_;
  // The block that is currently being filled, serialized.
  serialization::ByteBufferWriteSerializer currentBlock_;
  std::vector<PartialVocabularyBlockMetadata> blocks_;
  // The position in `currentBlock_` at which its last word starts. The first
  // word starts at position 0. Both are read back from the block when it is
  // finished, see `finishBlock`, so that nothing is copied per word.
  size_t offsetOfLastWord_ = 0;
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
  void operator()(std::string_view word,
                  PartialVocabIndexWithExternalFlag indexAndExternalFlag) {
    AD_CONTRACT_CHECK(!finishWasCalled_);
    offsetOfLastWord_ = currentBlock_.getCurrentPosition();
    currentBlock_ << word;
    currentBlock_ << indexAndExternalFlag.isExternal();
    currentBlock_ << indexAndExternalFlag.index();
    ++numWordsInBlock_;
    ++numWords_;
    if (currentBlock_.getCurrentPosition() >=
        PARTIAL_VOCAB_BLOCK_SIZE.getBytes()) {
      finishBlock();
    }
  }

  // Write the last block, the block index and its offset, then close the file.
  void finish() {
    if (std::exchange(finishWasCalled_, true)) {
      return;
    }
    finishBlock();
    uint64_t offsetOfBlockIndex = offset_;
    file_ << blocks_;
    file_ << offsetOfBlockIndex;
    // Patch in the number of words at the very beginning of the file.
    serialization::serializeAtPosition(file_, 0, numWords_);
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
                       wordInCurrentBlock(0),
                       wordInCurrentBlock(offsetOfLastWord_)});
    offset_ += bytes.size();
    currentBlock_.clear();
    numWordsInBlock_ = 0;
  }

  // Read the word that starts at the `offset` in the current block back out of
  // the serialized bytes of the block.
  TripleComponentWithIndex wordInCurrentBlock(size_t offset) const {
    const auto& bytes = currentBlock_.data();
    AD_CORRECTNESS_CHECK(offset < bytes.size());
    serialization::ByteBufferReadSerializerT reader{
        ql::span<const char>{bytes.data() + offset, bytes.size() - offset}};
    TripleComponentWithIndex word;
    reader >> word;
    return word;
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
  using Element = detail::InputWord;
  using Block = detail::InputBlock;
  // The merge folds the equal words of the partial vocabularies into one
  // `MergedWord` with all their occurrences, so its output blocks are of a
  // different type than its input blocks, see `MergeBlock.h`.
  using OutputBlock = detail::MergedBlock;

  // The totals over all blocks of all partial vocabularies, see `statistics`.
  struct Statistics {
    size_t numBlocks_ = 0;
    size_t numWords_ = 0;
    // The sizes of the blocks in the files.
    size_t numBytes_ = 0;
  };

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
    // The first and the last words of all blocks, back to back. This is
    // allocated once with its final size and never resized or moved, so that
    // the `Element`s can point into it.
    std::unique_ptr<char[]> boundaryWords_;
  };
  std::vector<Run> runs_;
  // The pools of the buffers of the blocks (see `detail::BufferPools`), with
  // the buffer sizes from `index/ConstantsIndexBuilding.h`.
  std::shared_ptr<detail::BufferPools> pools_ =
      std::make_shared<detail::BufferPools>(
          VOCAB_MERGER_FILE_BLOCK_BUFFER_SIZE.getBytes(),
          VOCAB_MERGER_MERGED_WORDS_BUFFER_SIZE.getBytes());

 public:
  PartialVocabularyRunsInput(const std::string& basename,
                             size_t numPartialVocabularies) {
    runs_.reserve(numPartialVocabularies);
    for (size_t i = 0; i < numPartialVocabularies; ++i) {
      runs_.push_back(openRun(partialVocabularyWordsFilename(basename, i), i));
    }
  }

  size_t numRuns() const { return runs_.size(); }

  size_t numBlocks(size_t run) const { return runs_.at(run).blocks_.size(); }

  size_t numElementsInBlock(size_t run, size_t block) const {
    return runs_.at(run).blocks_.at(block).numWords_;
  }

  // The size of a block in the file.
  size_t numBytesInBlock(size_t run, size_t block) const {
    return runs_.at(run).blocks_.at(block).numBytes_;
  }

  // Return the number of blocks, of words, and of bytes in the files over all
  // partial vocabularies.
  Statistics statistics() const {
    Statistics result;
    for (const Run& run : runs_) {
      result.numBlocks_ += run.blocks_.size();
      for (const BlockInfo& block : run.blocks_) {
        result.numWords_ += block.numWords_;
        result.numBytes_ += block.numBytes_;
      }
    }
    return result;
  }

  // The weight of a block for the chunk boundaries of the merge (see
  // `blockWeight` in `util/parallelBlockMerge/MergeHelpersImpl.h`), which is
  // its size in bytes. The cost of merging a word grows with its length, so
  // chunks of equal size in bytes cost about the same, while chunks of equal
  // numbers of words do not (a chunk of long literals costs many times a chunk
  // of short IRIs, and the consumer, which takes the chunks in order, then
  // waits for it while the merge runs ahead).
  size_t blockWeight(size_t run, size_t block) const {
    return numBytesInBlock(run, block);
  }

  const Element& firstElement(size_t run, size_t block) const {
    return runs_.at(run).blocks_.at(block).first_;
  }

  const Element& lastElement(size_t run, size_t block) const {
    return runs_.at(run).blocks_.at(block).last_;
  }

  // Read a block. The words are parsed in place, see `InputBlock`. The bytes
  // of the block come from the `pools_` and go back to them with the block.
  Block getBlock(size_t run, size_t block) const {
    const auto& info = runs_.at(run).blocks_.at(block);
    Block::Buffer bytes = pools_->fileBlocks_.take(info.numBytes_);
    bytes.resize(info.numBytes_);
    auto numBytesRead = runs_.at(run).file_.read(
        bytes.data(), info.numBytes_, static_cast<off_t>(info.offset_));
    AD_CORRECTNESS_CHECK(static_cast<uint64_t>(numBytesRead) == info.numBytes_);
    return Block{std::move(bytes), info.numWords_, static_cast<uint32_t>(run),
                 pools_};
  }

  // Create an empty block of merged words, whose buffers come from the
  // `pools_`.
  OutputBlock makeEmptyBlock() const { return OutputBlock{pools_}; }

  // Append a merged `word` to an output `block` of the merge. A word that is
  // equal to the last word of the block is folded into that word, see
  // `MergedBlock::addWord`.
  void appendToBlock(OutputBlock& block, const Element& word) const {
    block.addWord(word);
  }

  // The memory that the `word` occupies in an output block, see
  // `MergedBlock::memorySizeOfAppended`.
  MemorySize memorySizeOfElement(const Element& word) const {
    return OutputBlock::memorySizeOfAppended(word);
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
      AD_CORRECTNESS_CHECK(static_cast<uint64_t>(numBytesRead) == numBytes,
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
    Block::Buffer bytes(sizeOfFile - sizeof(offsetOfBlockIndex) -
                        offsetOfBlockIndex);
    readExactly(bytes.data(), bytes.size(), offsetOfBlockIndex);
    serialization::ByteBufferReadSerializerT reader{std::move(bytes)};
    std::vector<PartialVocabularyBlockMetadata> blocks;
    reader >> blocks;
    // The first and the last words of the blocks are stored back to back in
    // `boundaryWords_`, see there.
    size_t numBoundaryBytes = ::ranges::accumulate(
        blocks, size_t{0}, [](size_t sum, const auto& block) {
          return sum + block.first_.iriOrLiteral().size() +
                 block.last_.iriOrLiteral().size();
        });
    run.boundaryWords_ =
        ql::make_unique_for_overwrite<char[]>(numBoundaryBytes);
    auto makeElement = [&run, runIdx, numBoundaryBytes,
                        numBytesStored = size_t{0}](
                           const TripleComponentWithIndex& word) mutable {
      const auto& bytes = word.iriOrLiteral();
      AD_CORRECTNESS_CHECK(numBytesStored + bytes.size() <= numBoundaryBytes);
      char* storage = run.boundaryWords_.get() + numBytesStored;
      std::copy(bytes.begin(), bytes.end(), storage);
      numBytesStored += bytes.size();
      Element element;
      element.word_ = {storage, bytes.size()};
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
    return run;
  }
};

}  // namespace ad_utility::vocabulary_merger

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_PARTIALVOCABULARYFILE_H
