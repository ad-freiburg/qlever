// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_SEGMENT_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_SEGMENT_H

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "backports/three_way_comparison.h"
#include "global/Id.h"
#include "index/IndexBuilderTypes.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "index/vocabulary_merger/Concepts.h"
#include "index/vocabulary_merger/MergeBlock.h"
#include "index/vocabulary_merger/SegmentLocalId.h"
#include "index/vocabulary_merger/VocabularyMetaData.h"
#include "util/Exception.h"
#include "util/RegexSet.h"

// The segments of the vocabulary merger: a segment is a range of consecutive
// merged words (the blocks that the parallel block merge of the partial
// vocabularies yields, see `index/vocabulary_merger/PartialVocabularyFile.h`),
// processed by one task on the thread pool. A follow-up PR replaces the
// single-threaded pipeline behind `mergeVocabulary` (see
// `index/VocabularyMerger.h`) by these segments. This is not part of the
// public interface of that header.
//
// Terminology: the vocabulary that is being built may consist of several
// *sub-vocabularies* (for example a main vocabulary and a separate vocabulary
// for the geometry literals, see `SplitVocabulary`), each with its own
// contiguous index space. The `ParallelWordWriterBase` (see
// `index/vocabulary/VocabularyTypes.h`) decides for every word to which
// sub-vocabulary it belongs (`subVocabularyOf`) and which index the word gets
// at a given position in that sub-vocabulary (`indexOf`, which may encode
// additional information in the index). The blank nodes are not written to
// any vocabulary, but are numbered separately; for the bookkeeping in this
// file they are treated as one more (pseudo) sub-vocabulary, see
// `SegmentLocalId::blankNodeSubVocabulary`.
namespace ad_utility::vocabulary_merger::detail {

// An entry of the ID map of a partial vocabulary as a segment produces it: the
// index of the partial vocabulary, the index of the word in that partial
// vocabulary, and the segment-local ID of the merged word.
//
// NOTE: The index in the partial vocabulary is stored in 32 bits (a partial
// vocabulary has some million words), so that the entry is 16 bytes.
struct SegmentIdMapEntry {
  uint32_t partialVocabularyIndex_;
  uint32_t localIndex_;
  SegmentLocalId id_;

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(SegmentIdMapEntry,
                                              partialVocabularyIndex_,
                                              localIndex_, id_)

  // Make the output of failed tests readable.
  friend std::ostream& operator<<(std::ostream& os,
                                  const SegmentIdMapEntry& entry) {
    return os << '{' << entry.partialVocabularyIndex_ << ", "
              << entry.localIndex_ << ", " << entry.id_ << '}';
  }
};
static_assert(sizeof(SegmentIdMapEntry) == 16);

// The number of words of every sub-vocabulary (and of blank nodes, see
// `SegmentLocalId::blankNodeSubVocabulary`) in all the segments before a given
// segment, which turn the segment-local IDs of that segment into global ones.
struct SegmentBases {
  std::array<uint64_t, SegmentLocalId::blankNodeSubVocabulary + 1> bases_{};

  // The global index (for a word) or blank node number (for a blank node).
  uint64_t globalIndexOf(SegmentLocalId id) const {
    return id.index() + bases_[id.subVocabulary()];
  }

  // The global ID.
  Id globalIdOf(SegmentLocalId id) const {
    if (id.isBlankNode()) {
      return Id::makeFromBlankNodeIndex(
          BlankNodeIndex::make(globalIndexOf(id)));
    }
    return Id::makeFromVocabIndex(VocabIndex::make(globalIndexOf(id)));
  }
};

// The output of a segment task.
struct Segment {
  // The distinct words of the segment, one block per sub-vocabulary, in
  // order. The `firstPosition_` of these blocks is not set (the segment does
  // not know its position).
  std::vector<WordBlock> wordBlocks_;
  // The number of distinct blank nodes of the segment.
  size_t numBlankNodes_ = 0;
  // The metadata of the segment, with segment-local IDs.
  SegmentMetaData metaData_;
  // The ID map entries, grouped by partial vocabulary: the entries of the
  // partial vocabulary `p` are `idMapEntries_[idMapRunStarts_[p] ..
  // idMapRunStarts_[p + 1])`.
  std::vector<SegmentIdMapEntry> idMapEntries_;
  std::vector<size_t> idMapRunStarts_;
  // The number of merged words (including the duplicates) that the segment
  // was built from.
  size_t numMergedWords_ = 0;

  // The number of distinct words including the blank nodes. This is exact:
  // `buildSegment` removes all the duplicates, also those at the boundaries of
  // the merged blocks (see there), so each distinct word is in exactly one of
  // the `wordBlocks_` exactly once. The counts have to be exact, because
  // `basesOfNextSegment` derives the global IDs of the next segment from them.
  size_t numDistinctWords() const {
    return ::ranges::accumulate(wordBlocks_, numBlankNodes_,
                                [](size_t sum, const WordBlock& block) {
                                  return sum + block.numWords();
                                });
  }

  // The bases of the segment that follows this one, given the bases of this
  // one.
  SegmentBases basesOfNextSegment(SegmentBases bases) const {
    for (size_t sub = 0; sub < wordBlocks_.size(); ++sub) {
      bases.bases_[sub] += wordBlocks_[sub].numWords();
    }
    bases.bases_[SegmentLocalId::blankNodeSubVocabulary] += numBlankNodes_;
    return bases;
  }
};

// Build the segment of the consecutive `blocks` of merged words (see
// `PartialVocabularyRunsInput` for how the merge folds the occurrences of a
// word within a block): eliminate the remaining duplicates (the same word at
// the boundary of two blocks), combine their external flags, detect the blank
// nodes (see `isBlankNode`), assign every distinct word to its sub-vocabulary
// and its segment-local ID, and collect the ID map entries of all the
// occurrences. The words of the `blocks` have to be in the order of the
// `comparator` (a `WordComparator`), which is only checked if the expensive
// checks are enabled. The `blocks` are destroyed by this function.
//
// NOTE: The constraint on `W` is a `static_assert` in the body and not a
// `requires` clause, because the function is a friend of the `SegmentBuilder`
// below, and the C++17 emulation of a `requires` clause (a default template
// argument) is not allowed in a friend declaration.
template <typename W>
Segment buildSegment(std::vector<MergeBlock> blocks,
                     const ParallelWordWriterBase& writer,
                     const ad_utility::RegexSet& blankNodeIriRegexes,
                     size_t numPartialVocabularies, const W& comparator);

// The state of `buildSegment` (see above) while it builds a segment. The
// distinct word that was seen last is held back (the "pending" word) until a
// different word arrives, because its external flag is the OR over all its
// occurrences, and its occurrences are collected from all the merged words
// that are equal to it (at most two, the last of one block and the first of
// the next).
class SegmentBuilder {
 private:
  using Occurrence = MergeBlock::Occurrence;

  const ParallelWordWriterBase& writer_;
  const ad_utility::RegexSet& blankNodeIriRegexes_;
  size_t numPartialVocabularies_;
  uint8_t numSubVocabularies_;
  // The size of the per-word payload that the block writer of each
  // sub-vocabulary precomputes, see `BlockWriterBase::precomputePayload`.
  std::vector<size_t> payloadSizes_;
  // A buffer for that payload, reused for every word.
  std::string payload_;
  // The pending word, see above. It points into one of the input blocks.
  std::string_view pendingWord_;
  bool hasPendingWord_ = false;
  bool pendingIsExternal_ = false;
  std::vector<Occurrence> pendingOccurrences_;
  // The segment under construction.
  Segment segment_;

  // The only way to build a segment is the free function `buildSegment`.
  template <typename W>
  friend Segment buildSegment(std::vector<MergeBlock> blocks,
                              const ParallelWordWriterBase& writer,
                              const ad_utility::RegexSet& blankNodeIriRegexes,
                              size_t numPartialVocabularies,
                              const W& comparator);

  // Set up the builder for a segment of words from `numPartialVocabularies`
  // partial vocabularies that the `writer` writes.
  SegmentBuilder(const ParallelWordWriterBase& writer,
                 const ad_utility::RegexSet& blankNodeIriRegexes,
                 size_t numPartialVocabularies)
      : writer_{writer},
        blankNodeIriRegexes_{blankNodeIriRegexes},
        numPartialVocabularies_{numPartialVocabularies},
        numSubVocabularies_{writer.numSubVocabularies()} {
    AD_CONTRACT_CHECK(numSubVocabularies_ > 0 &&
                      numSubVocabularies_ <
                          SegmentLocalId::blankNodeSubVocabulary);
    segment_.wordBlocks_.resize(numSubVocabularies_);
    payloadSizes_.resize(numSubVocabularies_);
    for (uint8_t sub = 0; sub < numSubVocabularies_; ++sub) {
      payloadSizes_[sub] = writer.blockWriter(sub).precomputedPayloadSize();
      segment_.wordBlocks_[sub].payloadSize_ = payloadSizes_[sub];
    }
  }

  // Reserve the space for the ID map entries of the `blocks` (every merged
  // word has at least one occurrence) and remember their total number.
  void reserveForBlocks(const std::vector<MergeBlock>& blocks) {
    size_t numMergedWords = ::ranges::accumulate(
        blocks, size_t{0},
        [](size_t sum, const MergeBlock& block) { return sum + block.size(); });
    segment_.numMergedWords_ = numMergedWords;
    segment_.idMapEntries_.reserve(numMergedWords);
  }

  // Add the words of the next `block`. The words have to be in the order of
  // the `comparator` (see `buildSegment`), and after the words of the
  // previous blocks. The `block` has to stay alive until the next call to
  // `commitPendingWord`, because the pending word may point into it.
  CPP_template_2(typename W)(requires WordComparator<W>)
  void addBlock(const MergeBlock& block, [[maybe_unused]] const W& comparator) {
    bool isFirstWordOfBlock = true;
    for (const auto& word : block) {
      if (isFirstWordOfBlock && hasPendingWord_ &&
          word.iriOrLiteral() == pendingWord_) {
        // The same word ended the previous block.
        pendingIsExternal_ = pendingIsExternal_ || word.isExternal();
        addOccurrences(block, word);
      } else {
        AD_EXPENSIVE_CHECK(
            !hasPendingWord_ || comparator(pendingWord_, word.iriOrLiteral()),
            "Total vocabulary order violated for ", pendingWord_, " and ",
            word.iriOrLiteral());
        commitPendingWord();
        startPendingWord(block, word);
      }
      isFirstWordOfBlock = false;
    }
  }

  // Make the `word` of the `block` the pending word.
  void startPendingWord(const MergeBlock& block, const MergeWord& word) {
    pendingWord_ = word.iriOrLiteral();
    pendingIsExternal_ = word.isExternal();
    hasPendingWord_ = true;
    addOccurrences(block, word);
  }

  // Add the occurrences of the `word` of the `block` (its own and the further
  // ones that the merge folded into it, see `MergeBlock::addWord`) to those
  // of the pending word.
  void addOccurrences(const MergeBlock& block, const MergeWord& word) {
    pendingOccurrences_.emplace_back(word.partialFileId_, word.id());
    for (const auto& occurrence : block.moreOccurrences(word)) {
      pendingOccurrences_.push_back(occurrence);
    }
  }

  // Finish the pending word (if there is one): assign its segment-local ID,
  // add it to its word block (or count it as a blank node), and turn its
  // occurrences into ID map entries.
  void commitPendingWord() {
    if (!hasPendingWord_) {
      return;
    }
    SegmentLocalId id =
        isBlankNode(pendingWord_, blankNodeIriRegexes_)
            ? SegmentLocalId::makeBlankNode(segment_.numBlankNodes_++)
            : addPendingWordToWordBlock();
    for (const auto& [partialFileId, localIndex] : pendingOccurrences_) {
      AD_CORRECTNESS_CHECK(partialFileId < numPartialVocabularies_ &&
                           localIndex <= std::numeric_limits<uint32_t>::max());
      segment_.idMapEntries_.push_back(SegmentIdMapEntry{
          partialFileId, static_cast<uint32_t>(localIndex), id});
    }
    pendingOccurrences_.clear();
    hasPendingWord_ = false;
  }

  // Add the pending word (which is not a blank node) to the word block of its
  // sub-vocabulary and to the metadata, and return its segment-local ID.
  SegmentLocalId addPendingWordToWordBlock() {
    uint8_t sub = writer_.subVocabularyOf(pendingWord_);
    AD_CORRECTNESS_CHECK(sub < numSubVocabularies_);
    auto& words = segment_.wordBlocks_[sub];
    auto id = SegmentLocalId::make(
        sub, writer_.indexOf(sub, words.numWords(), pendingWord_));
    segment_.metaData_.addWord(pendingWord_, id);
    payload_.resize(payloadSizes_[sub]);
    if (!payload_.empty()) {
      writer_.blockWriter(sub).precomputePayload(
          pendingWord_, ql::span<char>{payload_.data(), payload_.size()});
    }
    words.push(pendingWord_, pendingIsExternal_, payload_);
    return id;
  }

  // Group the ID map entries by partial vocabulary (a counting sort), so that
  // the writers of the ID maps find the entries of each partial vocabulary in
  // one contiguous run, see `Segment::idMapRunStarts_`.
  void groupIdMapEntriesByPartialVocabulary() {
    std::vector<size_t>& runStarts = segment_.idMapRunStarts_;
    runStarts.assign(numPartialVocabularies_ + 1, 0);
    for (const auto& entry : segment_.idMapEntries_) {
      ++runStarts[entry.partialVocabularyIndex_ + 1];
    }
    for (size_t p = 0; p < numPartialVocabularies_; ++p) {
      runStarts[p + 1] += runStarts[p];
    }
    std::vector<SegmentIdMapEntry> sorted(segment_.idMapEntries_.size());
    std::vector<size_t> next(runStarts.begin(), runStarts.end() - 1);
    for (const auto& entry : segment_.idMapEntries_) {
      sorted[next[entry.partialVocabularyIndex_]++] = entry;
    }
    segment_.idMapEntries_ = std::move(sorted);
  }
};

// _____________________________________________________________________________
template <typename W>
Segment buildSegment(std::vector<MergeBlock> blocks,
                     const ParallelWordWriterBase& writer,
                     const ad_utility::RegexSet& blankNodeIriRegexes,
                     size_t numPartialVocabularies, const W& comparator) {
  static_assert(WordComparator<W>);
  SegmentBuilder builder{writer, blankNodeIriRegexes, numPartialVocabularies};
  builder.reserveForBlocks(blocks);
  for (const auto& block : blocks) {
    builder.addBlock(block, comparator);
  }
  builder.commitPendingWord();
  // NOTE: The blocks (which the pending word points into) are only destroyed
  // here, after the last word was committed.
  blocks.clear();
  builder.groupIdMapEntriesByPartialVocabulary();
  return std::move(builder.segment_);
}
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_SEGMENT_H
