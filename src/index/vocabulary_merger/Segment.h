// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
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
#include <functional>
#include <limits>
#include <optional>
#include <ostream>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/three_way_comparison.h"
#include "global/Id.h"
#include "index/IndexBuilderTypes.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "index/vocabulary_merger/Concepts.h"
#include "index/vocabulary_merger/MergeBlock.h"
#include "index/vocabulary_merger/SegmentLocalId.h"
#include "index/vocabulary_merger/VocabularyMetaData.h"
#include "util/CompactStringVector.h"
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
// `SegmentLocalId::blankNodeSubVocabularyIndex`.
namespace ad_utility::vocabulary_merger::detail {

// An entry of the ID map of a partial vocabulary as a segment produces it: the
// index of the partial vocabulary, the index of the word in that partial
// vocabulary, and the segment-local ID of the merged word.
//
// NOTE: The index in the partial vocabulary is stored in 32 bits, so that the
// entry is 16 bytes. This is safe, because a partial vocabulary holds the
// words of a single batch of the parser (`NUM_TRIPLES_PER_PARTIAL_VOCAB`
// triples by default, see `index/ConstantsIndexBuilding.h`; at most three
// words per triple), which has to fit into memory and is thus far below four
// billion words. The limit is also checked when an entry is created, see
// `SegmentBuilder::commitPendingWord`.
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

// This class is associated with a single segment and converts the
// `SegmentLocalId`s of this segment to global IDs. To do so it stores for each
// sub-vocabulary the number of words of that sub-vocabulary in all the
// segments before this one (and the same for the blank nodes, see
// `SegmentLocalId::blankNodeSubVocabularyIndex`), which is what has to be added
// to the segment-local index. The converter for the first segment is the
// default-constructed one (all zeros), the one for each following segment is
// obtained from the previous segment via `Segment::idConverterForNextSegment`.
struct IdConverterForSegment {
  std::array<uint64_t, SegmentLocalId::blankNodeSubVocabularyIndex + 1>
      numWordsInPreviousSegments_{};

  // The global index (for a word) or blank node number (for a blank node).
  //
  // NOTE: The index of a word may carry marker bits above its position (see
  // `ParallelWordWriterBase::indexOf`), which the addition must not carry
  // into. This requires `indexOf` to be additive in the position, which holds
  // as long as the positions stay far below the marker bits (the current
  // writers put them in the topmost bits of the index). The check below only
  // catches an overflow of the index as a whole.
  uint64_t globalIndexOf(SegmentLocalId id) const {
    uint64_t result =
        id.index() + numWordsInPreviousSegments_[id.subVocabIndex()];
    AD_CORRECTNESS_CHECK(result <= SegmentLocalId::maxIndex);
    return result;
  }

  // The global ID.
  Id globalIdOf(SegmentLocalId id) const {
    if (id.isBlankNode()) {
      return Id::makeFromBlankNodeIndex(
          BlankNodeIndex::make(globalIndexOf(id)));
    }
    return vocabIdFromIndex(globalIndexOf(id));
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
  // The ID map entries, grouped by partial vocabulary: `idMapEntries_[p]` are
  // the entries of the partial vocabulary `p` (in the order of the words), so
  // there is one element per partial vocabulary, which may be empty.
  CompactVectorOfStrings<SegmentIdMapEntry> idMapEntries_;

  // The number of distinct words including the blank nodes. This is exact:
  // `buildSegment` removes all the duplicates, also those at the boundaries of
  // the merged blocks (see there), so each distinct word is in exactly one of
  // the `wordBlocks_` exactly once. The counts have to be exact, because
  // `idConverterForNextSegment` derives the global IDs of the next segment
  // from them.
  size_t numDistinctWords() const {
    return ::ranges::accumulate(wordBlocks_, numBlankNodes_, std::plus<>{},
                                &WordBlock::numWords);
  }

  // The ID converter of the segment that follows this one, given the
  // `converter` of this one.
  IdConverterForSegment idConverterForNextSegment(
      IdConverterForSegment converter) const {
    auto& counts = converter.numWordsInPreviousSegments_;
    for (size_t sub = 0; sub < wordBlocks_.size(); ++sub) {
      counts[sub] += wordBlocks_[sub].numWords();
    }
    counts[SegmentLocalId::blankNodeSubVocabularyIndex] += numBlankNodes_;
    return converter;
  }
};

// Build the segment of the consecutive `blocks` of merged words (see
// `MergedBlock::addWord` for how the merge folds the occurrences of a word
// within a block): eliminate the remaining duplicates (the same word at
// the boundary of two blocks), combine their external flags, detect the blank
// nodes (see `isBlankNode`), assign every distinct word to its sub-vocabulary
// and its segment-local ID, and collect the ID map entries of all the
// occurrences. The words of the `blocks` have to be in the order of the merged
// vocabulary (by geo sort key, then by the `comparator`, a `WordComparator`),
// which is only checked if the expensive checks are enabled. The `blocks` are
// destroyed by this function.
//
// NOTE: The constraint on `W` is a `static_assert` in the body and not a
// `requires` clause, because the function is a friend of the `SegmentBuilder`
// below, and the C++17 emulation of a `requires` clause (a default template
// argument) is not allowed in a friend declaration.
template <typename W>
Segment buildSegment(std::vector<MergedBlock> blocks,
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
  using Occurrence = MergedBlock::Occurrence;

  const ParallelWordWriterBase& writer_;
  const ad_utility::RegexSet& blankNodeIriRegexes_;
  size_t numPartialVocabularies_;
  uint8_t numSubVocabularies_;
  // Whether the block writer of each sub-vocabulary precomputes a per-word
  // payload, see `BlockWriterBase::precomputePayload`.
  std::vector<bool> hasPayload_;
  // The pending word, see above (its word points into one of the blocks),
  // with the OR of the external flags of all its merged words so far. Its
  // occurrence range is not used, because the occurrences of a pending word
  // come from up to two blocks: they are collected in `pendingOccurrences_`.
  std::optional<MergedWord> pendingWord_;
  std::vector<Occurrence> pendingOccurrences_;
  // The ID map entries of the committed words, in the order of the words.
  // They are grouped by partial vocabulary at the very end, see
  // `groupIdMapEntriesByPartialVocabulary`.
  std::vector<SegmentIdMapEntry> idMapEntries_;
  // The segment under construction.
  Segment segment_;

  // The only way to build a segment is the free function `buildSegment`.
  template <typename W>
  friend Segment buildSegment(std::vector<MergedBlock> blocks,
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
                          SegmentLocalId::blankNodeSubVocabularyIndex);
    segment_.wordBlocks_.resize(numSubVocabularies_);
    hasPayload_.resize(numSubVocabularies_);
    for (uint8_t sub = 0; sub < numSubVocabularies_; ++sub) {
      size_t payloadSize = writer.blockWriter(sub).precomputedPayloadSize();
      segment_.wordBlocks_[sub].payloadSize_ = payloadSize;
      hasPayload_[sub] = payloadSize > 0;
    }
  }

  // Reserve the space for the ID map entries of the `blocks` (one per
  // occurrence of a word).
  void reserveForBlocks(const std::vector<MergedBlock>& blocks) {
    idMapEntries_.reserve(::ranges::accumulate(blocks, size_t{0}, std::plus<>{},
                                               &MergedBlock::numOccurrences));
  }

  // Add the words of the next `block`. The words have to be in the order of
  // the `comparator` (see `buildSegment`), and after the words of the
  // previous blocks. The `block` has to stay alive until the next call to
  // `commitPendingWord`, because the pending word may point into it.
  CPP_template_2(typename W)(requires WordComparator<W>)
  void addBlock(const MergedBlock& block,
                [[maybe_unused]] const W& comparator) {
    bool isFirstWordOfBlock = true;
    for (const auto& word : block) {
      if (isFirstWordOfBlock && pendingWord_.has_value() &&
          word.iriOrLiteral() == pendingWord_->iriOrLiteral()) {
        // The same word ended the previous block.
        pendingWord_->isExternal() |= word.isExternal();
      } else {
        // NOTE: The comparator is strict, so this check also fails for a word
        // that is equal to the pending one, which means that it also checks
        // that the words within a block are distinct (see
        // `MergedBlock::addWord`, which the logic above relies on).
        if (pendingWord_.has_value()) {
          checkVocabularyOrder(comparator, pendingWord_->geoSortKey_,
                               pendingWord_->iriOrLiteral(), word.geoSortKey_,
                               word.iriOrLiteral());
        }
        commitPendingWord();
        pendingWord_ = word;
      }
      addOccurrences(block, word);
      isFirstWordOfBlock = false;
    }
  }

  // Add the occurrences of the `word` of the `block` to those of the pending
  // word.
  void addOccurrences(const MergedBlock& block, const MergedWord& word) {
    auto occurrences = block.occurrences(word);
    pendingOccurrences_.insert(pendingOccurrences_.end(), occurrences.begin(),
                               occurrences.end());
  }

  // Finish the pending word (if there is one): assign its segment-local ID,
  // add it to its word block (or count it as a blank node), and turn its
  // occurrences into ID map entries.
  void commitPendingWord() {
    if (!pendingWord_.has_value()) {
      return;
    }
    SegmentLocalId id =
        isBlankNode(pendingWord_->iriOrLiteral(), blankNodeIriRegexes_)
            ? SegmentLocalId::makeBlankNode(segment_.numBlankNodes_++)
            : addPendingWordToWordBlock();
    for (const auto& [partialFileId, localIndex] : pendingOccurrences_) {
      AD_CORRECTNESS_CHECK(partialFileId < numPartialVocabularies_ &&
                           localIndex <= std::numeric_limits<uint32_t>::max());
      idMapEntries_.push_back(SegmentIdMapEntry{
          partialFileId, static_cast<uint32_t>(localIndex), id});
    }
    pendingOccurrences_.clear();
    pendingWord_.reset();
  }

  // Add the pending word (which is not a blank node) to the word block of its
  // sub-vocabulary and to the metadata, and return its segment-local ID.
  SegmentLocalId addPendingWordToWordBlock() {
    std::string_view word = pendingWord_->iriOrLiteral();
    uint8_t sub = writer_.subVocabularyOf(word);
    AD_CORRECTNESS_CHECK(sub < numSubVocabularies_);
    auto& words = segment_.wordBlocks_[sub];
    auto id = SegmentLocalId::makeFromWordIndex(
        sub, writer_.indexOf(sub, words.numWords(), word));
    segment_.metaData_.addWord(word, id);
    // The payload is computed directly into the word block.
    auto payload = words.pushWithPayloadSlot(word, pendingWord_->isExternal());
    if (hasPayload_[sub]) {
      writer_.blockWriter(sub).precomputePayload(word, payload);
    }
    return id;
  }

  // Move the `idMapEntries_` into the segment, grouped by partial vocabulary
  // (see `Segment::idMapEntries_`), so that the writers of the ID maps find
  // the entries of each partial vocabulary in one contiguous run. The entries
  // of a partial vocabulary stay in the order of the words.
  //
  // This is a counting sort by the partial vocabulary index, which directly
  // produces the layout of the `CompactVectorOfStrings` (the sorted entries
  // and the offset of each group) in two passes over the entries. A
  // `std::stable_sort` would be simpler to write, but the counting sort is
  // linear in the number of entries, needs no comparisons, and works because
  // the keys are the few small numbers `[0, numPartialVocabularies_)`.
  void groupIdMapEntriesByPartialVocabulary() {
    // First pass: `offsets[p + 1]` becomes the number of entries of the partial
    // vocabulary `p`, then the prefix sums turn `offsets[p]` into the start of
    // its group, with `offsets.back()` the total number of entries (one more
    // offset than groups, as the `CompactVectorOfStrings` wants it).
    std::vector<uint64_t> offsets(numPartialVocabularies_ + 1, 0);
    for (const auto& entry : idMapEntries_) {
      ++offsets[entry.partialVocabularyIndex_ + 1];
    }
    ::ranges::partial_sum(offsets, offsets.begin());
    // Second pass: place each entry at the next free position of its group.
    // The `next` positions start at the group starts and advance per entry.
    std::vector<uint64_t> next(offsets.begin(), offsets.end() - 1);
    std::vector<SegmentIdMapEntry> sorted(idMapEntries_.size());
    for (const auto& entry : idMapEntries_) {
      sorted[next[entry.partialVocabularyIndex_]++] = entry;
    }
    segment_.idMapEntries_ =
        CompactVectorOfStrings<SegmentIdMapEntry>::fromDataAndOffsets(
            std::move(sorted), std::move(offsets));
  }
};

// _____________________________________________________________________________
template <typename W>
Segment buildSegment(std::vector<MergedBlock> blocks,
                     const ParallelWordWriterBase& writer,
                     const ad_utility::RegexSet& blankNodeIriRegexes,
                     size_t numPartialVocabularies, const W& comparator) {
  // See the NOTE at the declaration for why this is not a `requires` clause.
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
