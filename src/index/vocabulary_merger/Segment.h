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
#include <string_view>
#include <utility>
#include <vector>

#include "backports/three_way_comparison.h"
#include "global/Id.h"
#include "global/ValueId.h"
#include "index/IndexBuilderTypes.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "index/vocabulary_merger/Concepts.h"
#include "index/vocabulary_merger/QueueWord.h"
#include "index/vocabulary_merger/VocabularyMetaData.h"
#include "util/Exception.h"
#include "util/RegexSet.h"

// The segments of the vocabulary merger (see the comment above
// `mergeVocabulary` in `index/VocabularyMerger.h`): a segment is a range of
// consecutive merged words, processed by one task on the thread pool. This is
// not part of the public interface of that header.
namespace ad_utility::vocabulary_merger::detail {

// The ID of a merged word within its segment: the sub-vocabulary of the word
// (see `ParallelWordWriterBase::subVocabularyOf`) in the upper bits, and in
// the lower bits the index that the word has in that sub-vocabulary if the
// segment started at position `0` (see `ParallelWordWriterBase::indexOf`). A
// blank node has the `blankNodeSubVocabulary` and the number of blank nodes
// before it in the segment. The global ID of the word is obtained by adding
// the number of words of its sub-vocabulary (or of blank nodes) in all the
// previous segments, see `SegmentBases`.
using SegmentLocalId = uint64_t;
constexpr size_t subVocabularyShift = ValueId::numDataBits;
constexpr uint8_t blankNodeSubVocabulary =
    (1u << (64 - subVocabularyShift)) - 1;
constexpr uint64_t localIndexMask = (uint64_t{1} << subVocabularyShift) - 1;
static_assert(64 - subVocabularyShift >= 4,
              "At least four bits are needed for the sub-vocabulary");

// ___________________________________________________________________________
inline SegmentLocalId makeSegmentLocalId(uint8_t sub, uint64_t index) {
  AD_CORRECTNESS_CHECK(sub <= blankNodeSubVocabulary &&
                       index <= localIndexMask);
  return (static_cast<uint64_t>(sub) << subVocabularyShift) | index;
}
inline uint8_t subVocabularyOf(SegmentLocalId id) {
  return static_cast<uint8_t>(id >> subVocabularyShift);
}
inline uint64_t localIndexOf(SegmentLocalId id) { return id & localIndexMask; }

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
// `blankNodeSubVocabulary`) in all the segments before a given segment, which
// turn the segment-local IDs of that segment into global ones.
struct SegmentBases {
  std::array<uint64_t, blankNodeSubVocabulary + 1> bases_{};

  // The global index (for a word) or blank node number (for a blank node).
  uint64_t globalIndexOf(SegmentLocalId id) const {
    return localIndexOf(id) + bases_[subVocabularyOf(id)];
  }

  // The global ID.
  Id globalIdOf(SegmentLocalId id) const {
    if (subVocabularyOf(id) == blankNodeSubVocabulary) {
      return Id::makeFromBlankNodeIndex(
          BlankNodeIndex::make(globalIndexOf(id)));
    }
    return Id::makeFromVocabIndex(VocabIndex::make(globalIndexOf(id)));
  }
};

// The output of a segment task.
struct Segment {
  // The distinct words of the segment, per sub-vocabulary, in order. The
  // `firstPosition_` of these blocks is not set (the segment does not know
  // its position).
  std::vector<WordBlock> words_;
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

  // The number of distinct words including the blank nodes.
  size_t numDistinctWords() const {
    size_t result = numBlankNodes_;
    for (const auto& block : words_) {
      result += block.numWords();
    }
    return result;
  }

  // The bases of the segment that follows this one, given the bases of this
  // one.
  SegmentBases basesOfNextSegment(SegmentBases bases) const {
    for (size_t sub = 0; sub < words_.size(); ++sub) {
      bases.bases_[sub] += words_[sub].numWords();
    }
    bases.bases_[blankNodeSubVocabulary] += numBlankNodes_;
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
// `comparator`, which is only checked if the expensive checks are enabled.
// The `blocks` are destroyed by this function.
CPP_template(typename W)(requires WordComparator<W>) Segment
    buildSegment(std::vector<std::vector<QueueWord>> blocks,
                 const ParallelWordWriterBase& writer,
                 const ad_utility::RegexSet& blankNodeIriRegexes,
                 size_t numPartialVocabularies,
                 [[maybe_unused]] const W& comparator) {
  using Occurrence = QueueWord::Occurrence;
  Segment segment;
  const uint8_t numSubs = writer.numSubVocabularies();
  AD_CONTRACT_CHECK(numSubs > 0 && numSubs < blankNodeSubVocabulary);
  segment.words_.resize(numSubs);
  // Every merged word has at least one occurrence.
  size_t numMergedWords = 0;
  for (const auto& block : blocks) {
    numMergedWords += block.size();
  }
  segment.numMergedWords_ = numMergedWords;
  segment.idMapEntries_.reserve(numMergedWords);

  // The distinct word that was seen last is held back until a different word
  // arrives, because its external flag is the OR over all its occurrences, and
  // its occurrences are collected from all the merged words that are equal to
  // it (at most two, the last of one block and the first of the next).
  std::string_view pendingWord;
  bool hasPendingWord = false;
  bool pendingIsExternal = false;
  std::vector<Occurrence> pendingOccurrences;
  auto addOccurrences = [&pendingOccurrences](const QueueWord& word) {
    pendingOccurrences.emplace_back(static_cast<uint32_t>(word.partialFileId_),
                                    word.id());
    for (const auto& occurrence : word.moreOccurrences_) {
      pendingOccurrences.push_back(occurrence);
    }
  };
  auto commitPendingWord = [&]() {
    if (!hasPendingWord) {
      return;
    }
    SegmentLocalId id;
    if (isBlankNode(pendingWord, blankNodeIriRegexes)) {
      id = makeSegmentLocalId(blankNodeSubVocabulary, segment.numBlankNodes_++);
    } else {
      uint8_t sub = writer.subVocabularyOf(pendingWord);
      AD_CORRECTNESS_CHECK(sub < numSubs);
      auto& words = segment.words_[sub];
      id = makeSegmentLocalId(
          sub, writer.indexOf(sub, words.numWords(), pendingWord));
      segment.metaData_.addWord(pendingWord, id);
      words.push(pendingWord, pendingIsExternal);
    }
    for (const auto& [partialFileId, localIndex] : pendingOccurrences) {
      AD_CORRECTNESS_CHECK(partialFileId < numPartialVocabularies &&
                           localIndex <= std::numeric_limits<uint32_t>::max());
      segment.idMapEntries_.push_back(SegmentIdMapEntry{
          partialFileId, static_cast<uint32_t>(localIndex), id});
    }
    pendingOccurrences.clear();
    hasPendingWord = false;
  };

  for (auto& block : blocks) {
    bool isFirstWordOfBlock = true;
    for (const auto& word : block) {
      if (isFirstWordOfBlock && hasPendingWord &&
          word.iriOrLiteral() == pendingWord) {
        pendingIsExternal = pendingIsExternal || word.isExternal();
        addOccurrences(word);
      } else {
        AD_EXPENSIVE_CHECK(
            !hasPendingWord || comparator(pendingWord, word.iriOrLiteral()),
            "Total vocabulary order violated for ", pendingWord, " and ",
            word.iriOrLiteral());
        commitPendingWord();
        pendingWord = word.iriOrLiteral();
        pendingIsExternal = word.isExternal();
        addOccurrences(word);
        hasPendingWord = true;
      }
      isFirstWordOfBlock = false;
    }
  }
  commitPendingWord();
  // NOTE: The blocks (which the pending word points into) are only destroyed
  // here, after the last word was committed.
  blocks.clear();

  // Group the ID map entries by partial vocabulary (a counting sort), so
  // that the writers of the ID maps find the entries of each partial
  // vocabulary in one contiguous run.
  std::vector<size_t>& runStarts = segment.idMapRunStarts_;
  runStarts.assign(numPartialVocabularies + 1, 0);
  for (const auto& entry : segment.idMapEntries_) {
    ++runStarts[entry.partialVocabularyIndex_ + 1];
  }
  for (size_t p = 0; p < numPartialVocabularies; ++p) {
    runStarts[p + 1] += runStarts[p];
  }
  std::vector<SegmentIdMapEntry> sorted(segment.idMapEntries_.size());
  std::vector<size_t> next(runStarts.begin(), runStarts.end() - 1);
  for (const auto& entry : segment.idMapEntries_) {
    sorted[next[entry.partialVocabularyIndex_]++] = entry;
  }
  segment.idMapEntries_ = std::move(sorted);
  return segment;
}
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_SEGMENT_H
