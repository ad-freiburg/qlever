// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/SecondaryVocabulary.h"

#include <functional>
#include <utility>

#include "backports/algorithm.h"
#include "util/Exception.h"

// _____________________________________________________________________________
SecondaryVocabulary::SecondaryVocabulary(std::vector<std::string> words) {
  CompactVectorOfStrings<char> segment;
  segment.build(words);
  appendSegment(std::move(segment));
}

// _____________________________________________________________________________
void SecondaryVocabulary::appendSegment(CompactVectorOfStrings<char> segment) {
  // Check that the words of `segment` are sorted and pairwise distinct. This
  // is a precondition that the caller has to establish (see the declaration),
  // because `segment` may be a zero-copy view that must not be reordered here.
  AD_CONTRACT_CHECK(
      ql::ranges::adjacent_find(segment, std::greater_equal<>{}) ==
          segment.end(),
      "The words of a segment of a secondary vocabulary have to be sorted and "
      "pairwise distinct");

  // Determine where in `sortedIndices_` the new words have to go, which also
  // checks that none of them is already contained. NOTE: Both of these have to
  // happen before `segment` is appended below, because `wordAt` must only see
  // the words that were already contained, and because a rejected segment has
  // to leave this vocabulary unchanged.
  std::vector<size_t> insertPositions = insertPositionsInSortedIndices(segment);

  uint64_t firstGlobalIndex = numWords();
  segmentOffsets_.push_back(firstGlobalIndex);
  segments_.push_back(std::move(segment));
  mergeIntoSortedIndices(insertPositions, firstGlobalIndex);
}

// _____________________________________________________________________________
size_t SecondaryVocabulary::numWords() const {
  if (segments_.empty()) {
    return 0;
  }
  return segmentOffsets_.back() + segments_.back().size();
}

// _____________________________________________________________________________
size_t SecondaryVocabulary::numSegments() const { return segments_.size(); }

// _____________________________________________________________________________
std::string_view SecondaryVocabulary::operator[](
    SecondaryVocabIndex index) const {
  uint64_t globalIndex = index.get();
  AD_CONTRACT_CHECK(globalIndex < numWords());
  auto it = ql::ranges::upper_bound(segmentOffsets_, globalIndex);
  size_t segmentIdx = static_cast<size_t>(it - segmentOffsets_.begin()) - 1;
  return segments_.at(segmentIdx)[globalIndex - segmentOffsets_[segmentIdx]];
}

// _____________________________________________________________________________
std::optional<SecondaryVocabIndex> SecondaryVocabulary::getId(
    std::string_view word) const {
  auto project = [this](uint64_t globalIndex) { return wordAt(globalIndex); };
  auto it = ql::ranges::lower_bound(sortedIndices_, word, {}, project);
  if (it == sortedIndices_.end() || project(*it) != word) {
    return std::nullopt;
  }
  return SecondaryVocabIndex::make(*it);
}

// _____________________________________________________________________________
std::vector<size_t> SecondaryVocabulary::insertPositionsInSortedIndices(
    const CompactVectorOfStrings<char>& segment) const {
  auto project = [this](uint64_t globalIndex) { return wordAt(globalIndex); };
  std::vector<size_t> insertPositions;
  insertPositions.reserve(segment.size());
  // The words of `segment` are sorted, so their insert positions are
  // non-decreasing and the search range can be shrunk from the left as we go.
  auto begin = sortedIndices_.begin();
  for (std::string_view word : segment) {
    auto it =
        ql::ranges::lower_bound(begin, sortedIndices_.end(), word, {}, project);
    AD_CONTRACT_CHECK(it == sortedIndices_.end() || project(*it) != word,
                      "The words of a secondary vocabulary have to be "
                      "distinct, but the word ",
                      word, " is already contained in a previous segment");
    insertPositions.push_back(static_cast<size_t>(it - sortedIndices_.begin()));
    begin = it;
  }
  return insertPositions;
}

// _____________________________________________________________________________
void SecondaryVocabulary::mergeIntoSortedIndices(
    const std::vector<size_t>& insertPositions, uint64_t firstGlobalIndex) {
  size_t numOldWords = sortedIndices_.size();
  size_t numNewWords = insertPositions.size();
  sortedIndices_.resize(numOldWords + numNewWords);

  // Fill `sortedIndices_` from the back. `writeIdx` is one past the position
  // that is written next, and `readIdx` one past the previously contained
  // global index that is moved next. Going backwards through the new words,
  // first move all the previously contained global indices that have to end up
  // behind the current new word, then write that new word's global index. The
  // global indices at the positions in front of `insertPositions.front()` stay
  // where they are, so each of them is moved at most once.
  size_t writeIdx = numOldWords + numNewWords;
  size_t readIdx = numOldWords;
  for (size_t i = numNewWords; i > 0; --i) {
    while (readIdx > insertPositions[i - 1]) {
      sortedIndices_[--writeIdx] = sortedIndices_[--readIdx];
    }
    sortedIndices_[--writeIdx] = firstGlobalIndex + (i - 1);
  }
  AD_CORRECTNESS_CHECK(writeIdx == readIdx);
}

// _____________________________________________________________________________
std::string_view SecondaryVocabulary::wordAt(uint64_t globalIndex) const {
  return (*this)[SecondaryVocabIndex::make(globalIndex)];
}
