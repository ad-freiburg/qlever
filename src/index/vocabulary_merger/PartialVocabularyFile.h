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
#include <functional>
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
// 2. The words, in the order of the vocabulary (by geo sort key first, see
//    `ItemVecEntry`). Each word is serialized as a `TripleComponentWithIndex`,
//    that is, the word itself, its external flag and its local index. The
//    words are grouped into blocks: a block ends after the shortest of the
//    `PARTIAL_VOCAB_BLOCK_CUT_WINDOW` words from the one that makes it at
//    least `PARTIAL_VOCAB_BLOCK_SIZE` bytes long on, see
//    `PartialVocabularyWriter`. The blocks are only visible through the block
//    index (part 3), the words themselves are stored back to back.
// 3. The block index: the number of blocks as a `uint64_t`, then the
//    `PartialVocabularyBlockMetadata` of every block (its position, its number
//    of words and its last word).
// 4. The offset of the block index (part 3) in the file, as a `uint64_t`.
//
// The words are stored exactly as before the block index existed, so a reader
// that only wants the words (part 1, then that many words) still works. The
// block index is what the merge needs: the last word of every block, without
// reading the block, so that it can split the runs into independent chunks by
// their words alone and then read only the blocks that a chunk needs, see
// `PartialVocabularyRunsInput`. This is how `mergeVocabulary` (see
// `index/VocabularyMerger.h`) merges the partial vocabularies.
namespace ad_utility::vocabulary_merger {

// The metadata of a single block of words, see above. The last word is stored
// in full, because the merge compares it with the words of other partial
// vocabularies. The first word is not stored: the merge only needs a lower
// bound for it, and the last word of the block before is one (see
// `PartialVocabularyRunsInput::firstElement`).
struct PartialVocabularyBlockMetadata {
  uint64_t offset_ = 0;
  uint64_t numBytes_ = 0;
  uint64_t numWords_ = 0;
  TripleComponentWithIndex last_;

  AD_SERIALIZE_FRIEND_FUNCTION(PartialVocabularyBlockMetadata) {
    serializer | arg.offset_;
    serializer | arg.numBytes_;
    serializer | arg.numWords_;
    serializer | arg.last_;
  }
};

// Write a partial vocabulary file. The words have to be pushed in the order of
// the vocabulary, each with its local index and its external flag (bundled in
// a `PartialVocabIndexWithExternalFlag`), and `finish()` (which the destructor
// calls if necessary) writes the block index.
//
// A block ends after the shortest of the `PARTIAL_VOCAB_BLOCK_CUT_WINDOW`
// words from the one that makes the block at least `PARTIAL_VOCAB_BLOCK_SIZE`
// bytes long on, and the words after it start the next block. The merge keeps
// the last word of every block in memory (see `PartialVocabularyRunsInput`),
// and ending a block right at the word that makes it reach the size would
// favor long words there (a long word is more likely to be that word than a
// short one), see `PARTIAL_VOCAB_BLOCK_CUT_WINDOW`.
class PartialVocabularyWriter : public ad_utility::NoCopyNoMove {
 private:
  // A word of the current block: where it starts in `currentBlock_` and its
  // serialized size.
  struct WordInBlock {
    size_t offset_;
    size_t numBytes_;
  };
  serialization::FileWriteSerializer file_;
  // The block that is currently being filled, serialized.
  serialization::ByteBufferWriteSerializer currentBlock_;
  std::vector<PartialVocabularyBlockMetadata> blocks_;
  // The words of the current block from the one that made it reach
  // `PARTIAL_VOCAB_BLOCK_SIZE` on, the candidates for its last word (see the
  // class comment).
  std::vector<WordInBlock> candidates_;
  // The position in `currentBlock_` at which its last word starts. It is read
  // back from the block when the block is finished, see `finishBlock`, so that
  // nothing is copied per word.
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
    const size_t endOfWord = currentBlock_.getCurrentPosition();
    if (endOfWord >= PARTIAL_VOCAB_BLOCK_SIZE.getBytes()) {
      candidates_.push_back({offsetOfLastWord_, endOfWord - offsetOfLastWord_});
      if (candidates_.size() == PARTIAL_VOCAB_BLOCK_CUT_WINDOW) {
        cutBlockAtShortestCandidate();
      }
    }
  }

  // Write the last block, the block index and its offset, then close the file.
  void finish() {
    if (std::exchange(finishWasCalled_, true)) {
      return;
    }
    finishBlock(currentBlock_.getCurrentPosition(), numWordsInBlock_,
                offsetOfLastWord_);
    uint64_t offsetOfBlockIndex = offset_;
    file_ << blocks_;
    file_ << offsetOfBlockIndex;
    // Patch in the number of words at the very beginning of the file.
    serialization::serializeAtPosition(file_, 0, numWords_);
    file_.close();
  }

 private:
  // End the current block after the shortest of the `candidates_` (see the
  // class comment). The words after it start the next block.
  void cutBlockAtShortestCandidate() {
    AD_CORRECTNESS_CHECK(!candidates_.empty());
    size_t shortest = 0;
    for (size_t i = 1; i < candidates_.size(); ++i) {
      if (candidates_[i].numBytes_ < candidates_[shortest].numBytes_) {
        shortest = i;
      }
    }
    const size_t cutOffset =
        candidates_[shortest].offset_ + candidates_[shortest].numBytes_;

    // The words after the cut, with their offsets relative to the next block,
    // and their bytes.
    std::vector<WordInBlock> movedWords(candidates_.begin() + shortest + 1,
                                        candidates_.end());
    for (WordInBlock& word : movedWords) {
      word.offset_ -= cutOffset;
    }
    const auto& bytes = currentBlock_.data();
    std::vector<char> movedBytes(bytes.begin() + cutOffset, bytes.end());
    finishBlock(cutOffset, numWordsInBlock_ - movedWords.size(),
                candidates_[shortest].offset_);

    // The next block starts with the moved words. They can already be
    // candidates again, if they alone make the block reach the size (a very
    // long word), but they are fewer than a window, so no further cut is due.
    currentBlock_.serializeBytes(movedBytes.data(), movedBytes.size());
    numWordsInBlock_ = movedWords.size();
    for (const WordInBlock& word : movedWords) {
      offsetOfLastWord_ = word.offset_;
      if (word.offset_ + word.numBytes_ >=
          PARTIAL_VOCAB_BLOCK_SIZE.getBytes()) {
        candidates_.push_back(word);
      }
    }
  }

  // Write the first `numBytes` of the current block, which hold `numWords`
  // words the last of which starts at `offsetOfLastWord`, and store its
  // metadata. Nothing is written for an empty block.
  void finishBlock(size_t numBytes, size_t numWords, size_t offsetOfLastWord) {
    if (numWords == 0) {
      AD_CORRECTNESS_CHECK(numBytes == 0);
      return;
    }
    const auto& bytes = currentBlock_.data();
    AD_CORRECTNESS_CHECK(numBytes <= bytes.size() &&
                         offsetOfLastWord < numBytes);
    file_.serializeBytes(bytes.data(), numBytes);
    blocks_.push_back(
        {offset_, numBytes, numWords, wordInCurrentBlock(offsetOfLastWord)});
    offset_ += numBytes;
    currentBlock_.clear();
    candidates_.clear();
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
//
// The `geoSortKeyFn` computes the geo sort key of a word (see
// `TripleComponentComparator::geoSortKey`), which the merge orders by before
// the words themselves. It is called once per word when the word is read, so
// that the merge never parses a WKT literal per comparison. Without a geo cell
// grid it is empty, and all keys are 0.
class PartialVocabularyRunsInput : public ad_utility::NoCopy {
 public:
  using Element = detail::InputWord;
  using Block = detail::InputBlock;
  // The merge folds the equal words of the partial vocabularies into one
  // `MergedWord` with all their occurrences, so its output blocks are of a
  // different type than its input blocks, see `MergeBlock.h`.
  using OutputBlock = detail::MergedBlock;
  using GeoSortKeyFn = std::function<uint64_t(std::string_view)>;

  // The totals over all blocks of all partial vocabularies, see `statistics`.
  struct Statistics {
    size_t numBlocks_ = 0;
    size_t numWords_ = 0;
    // The sizes of the blocks in the files.
    size_t numBytes_ = 0;
    // The memory that the block index takes: the metadata of the blocks with
    // their boundary words, see `openRun`.
    size_t numBytesOfBlockIndex_ = 0;
  };

 private:
  // The metadata of a block, with the last word already in the form that the
  // merge compares. Its word points into the `boundaryWords_` of the run.
  struct BlockInfo {
    uint64_t offset_;
    uint64_t numBytes_;
    uint64_t numWords_;
    Element last_;
  };
  struct Run {
    ad_utility::File file_;
    std::vector<BlockInfo> blocks_;
    // The first word of the file (if it has any) and the last words of all
    // blocks, back to back. This is allocated once with its final size and
    // never resized or moved, so that the `Element`s can point into it.
    std::unique_ptr<char[]> boundaryWords_;
    size_t numBoundaryBytes_ = 0;
    // The first word of the file, see `firstElement`.
    Element firstWord_;
  };
  std::vector<Run> runs_;
  // See the class comment.
  GeoSortKeyFn geoSortKeyFn_;
  // The pools of the buffers of the blocks (see `detail::BufferPools`), with
  // the buffer sizes from `index/ConstantsIndexBuilding.h`.
  std::shared_ptr<detail::BufferPools> pools_ =
      std::make_shared<detail::BufferPools>(
          VOCAB_MERGER_FILE_BLOCK_BUFFER_SIZE.getBytes(),
          VOCAB_MERGER_MERGED_WORDS_BUFFER_SIZE.getBytes());

 public:
  PartialVocabularyRunsInput(const std::string& basename,
                             size_t numPartialVocabularies,
                             GeoSortKeyFn geoSortKeyFn = nullptr)
      : geoSortKeyFn_{std::move(geoSortKeyFn)} {
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
      result.numBytesOfBlockIndex_ +=
          run.blocks_.size() * sizeof(BlockInfo) + run.numBoundaryBytes_;
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

  // A lower bound of the first word of a block: the last word of the block
  // before it, or the first word of the file for the first block. The merge
  // only uses it to leave out the blocks that lie entirely beyond the end of a
  // chunk (see `blockRangeForRun` in `util/parallelBlockMerge/MergeHelpers.h`),
  // for which a lower bound suffices, and this way the block index stores a
  // single word per block (see `PartialVocabularyBlockMetadata`).
  const Element& firstElement(size_t run, size_t block) const {
    const Run& theRun = runs_.at(run);
    AD_CONTRACT_CHECK(block < theRun.blocks_.size());
    return block == 0 ? theRun.firstWord_ : theRun.blocks_[block - 1].last_;
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
                 pools_, geoSortKeyFn_};
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
  Run openRun(const std::string& filename, size_t runIdx) const {
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
                           "Could not read the partial vocabulary file ",
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
    // The first word of the file (see `firstElement`) is not part of the block
    // index. It starts right after the number of words (part 1), with the
    // length of its text.
    TripleComponentWithIndex firstWord;
    if (!blocks.empty()) {
      uint64_t numBytesOfText = 0;
      readExactly(&numBytesOfText, sizeof(numBytesOfText), sizeof(uint64_t));
      Block::Buffer bytesOfWord(sizeof(numBytesOfText) + numBytesOfText +
                                sizeof(bool) + sizeof(uint64_t));
      AD_CONTRACT_CHECK(
          sizeof(uint64_t) + bytesOfWord.size() <= offsetOfBlockIndex,
          "The partial vocabulary file ", filename, " has no valid first word");
      readExactly(bytesOfWord.data(), bytesOfWord.size(), sizeof(uint64_t));
      serialization::ByteBufferReadSerializerT wordReader{
          std::move(bytesOfWord)};
      wordReader >> firstWord;
    }
    // The first word and the last words of the blocks are stored back to back
    // in `boundaryWords_`, see there.
    size_t numBoundaryBytes =
        ::ranges::accumulate(blocks, firstWord.iriOrLiteral().size(),
                             [](size_t sum, const auto& block) {
                               return sum + block.last_.iriOrLiteral().size();
                             });
    run.boundaryWords_ =
        ql::make_unique_for_overwrite<char[]>(numBoundaryBytes);
    run.numBoundaryBytes_ = numBoundaryBytes;
    auto makeElement = [this, &run, runIdx, numBoundaryBytes,
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
      if (geoSortKeyFn_) {
        element.geoSortKey_ = geoSortKeyFn_(element.word_);
      }
      return element;
    };
    if (!blocks.empty()) {
      run.firstWord_ = makeElement(firstWord);
    }
    run.blocks_.reserve(blocks.size());
    for (const auto& block : blocks) {
      AD_CORRECTNESS_CHECK(block.numWords_ > 0);
      run.blocks_.push_back({block.offset_, block.numBytes_, block.numWords_,
                             makeElement(block.last_)});
    }
    return run;
  }
};

}  // namespace ad_utility::vocabulary_merger

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_PARTIALVOCABULARYFILE_H
