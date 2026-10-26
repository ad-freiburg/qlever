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
#include "backports/shift.h"
#include "util/Exception.h"
#include "util/Views.h"

// _____________________________________________________________________________
SecondaryVocabulary::SecondaryVocabulary(ql::span<const std::string> words) {
  CompactVectorOfStrings<char> segment;
  segment.build(words);
  appendSegment(std::move(segment));
}

// _____________________________________________________________________________
void SecondaryVocabulary::appendSegment(CompactVectorOfStrings<char> segment) {
  // Empty segments would only bloat `segments_` and `segmentOffsets_`.
  if (segment.size() == 0) {
    return;
  }

  // A vocabulary that was read via zero-copy deserialization is read-only.
  AD_CONTRACT_CHECK(
      segmentOffsets_.isOwned() && sortedIndices_.isOwned(),
      "A secondary vocabulary that was read via zero-copy deserialization "
      "cannot be extended, call `clone()` first");

  // Check that the words of `segment` are sorted and pairwise distinct. This
  // is a precondition that the caller has to establish (see the declaration),
  // because `segment` may be a zero-copy view that must not be reordered here.
  AD_CONTRACT_CHECK(
      ql::ranges::adjacent_find(segment, std::greater_equal<>{}) ==
          segment.end(),
      "The words of a segment of a secondary vocabulary have to be sorted and "
      "pairwise distinct");

  // Determine where in `sortedIndices_` the new words have to go, which also
  // checks that none of them is already contained.
  //
  // NOTE: Both of these have to happen before `segment` is appended below,
  // because `wordAt` must only see the words that were already contained, and
  // because a rejected segment has to leave this vocabulary unchanged.
  std::vector<size_t> insertPositions = insertPositionsInSortedIndices(segment);

  uint64_t firstGlobalIndex = numWords();
  segmentOffsets_.modify([firstGlobalIndex](auto& segmentOffsets) {
    segmentOffsets.push_back(firstGlobalIndex);
  });
  segments_.push_back(std::move(segment));
  sortedIndices_.modify([&insertPositions, firstGlobalIndex](auto& sorted) {
    mergeIntoSortedIndices(sorted, insertPositions, firstGlobalIndex);
  });
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
  // `segmentOffsets_[0] == 0 <= globalIndex`, so `it` is never `begin()`, and
  // the segment before `it` is the last one that starts at or before
  // `globalIndex`.
  auto segmentOffsets = segmentOffsets_.view();
  auto it = ql::ranges::upper_bound(segmentOffsets, globalIndex);
  size_t segmentIdx = static_cast<size_t>(it - segmentOffsets.begin()) - 1;
  return segments_[segmentIdx][globalIndex - segmentOffsets[segmentIdx]];
}

// _____________________________________________________________________________
std::optional<SecondaryVocabIndex> SecondaryVocabulary::getId(
    std::string_view word) const {
  auto [position, found] = lowerBoundInSortedIndices(word, 0);
  if (!found) {
    return std::nullopt;
  }
  return SecondaryVocabIndex::make(sortedIndices_[position]);
}

// _____________________________________________________________________________
SecondaryVocabulary SecondaryVocabulary::clone() const {
  SecondaryVocabulary result;
  result.segments_.reserve(segments_.size());
  for (const auto& segment : segments_) {
    result.segments_.push_back(segment.clone());
  }
  result.segmentOffsets_ = segmentOffsets_.clone();
  result.sortedIndices_ = sortedIndices_.clone();
  return result;
}

// _____________________________________________________________________________
std::vector<size_t> SecondaryVocabulary::insertPositionsInSortedIndices(
    const CompactVectorOfStrings<char>& segment) const {
  std::vector<size_t> insertPositions;
  insertPositions.reserve(segment.size());
  // The words of `segment` are sorted, so their insert positions are
  // non-decreasing and the search range can be shrunk from the left as we go.
  size_t begin = 0;
  for (std::string_view word : segment) {
    auto [position, found] = lowerBoundInSortedIndices(word, begin);
    AD_CONTRACT_CHECK(!found,
                      "The words of a secondary vocabulary have to be "
                      "distinct, but the word ",
                      word, " is already contained in a previous segment");
    insertPositions.push_back(position);
    begin = position;
  }
  return insertPositions;
}

// _____________________________________________________________________________
void SecondaryVocabulary::mergeIntoSortedIndices(
    std::vector<uint64_t>& sortedIndices,
    const std::vector<size_t>& insertPositions, uint64_t firstGlobalIndex) {
  size_t numOldWords = sortedIndices.size();
  size_t numNewWords = insertPositions.size();
  sortedIndices.resize(numOldWords + numNewWords);

  // Fill `sortedIndices` from the back. `previousInsertPos` is the position
  // at which the global index of the previously handled new word was written
  // (initially the end). Going backwards through the new words, for the new
  // word `i`, first shift the previously contained global indices that have to
  // end up behind it to the right by `i + 1` positions (which makes room for
  // the global indices of the new words `0, ..., i`), then write the global
  // index of word `i` directly in front of the shifted ones. The global
  // indices at the positions in front of `insertPositions.front()` stay where
  // they are, so each of them is moved at most once.
  auto previousInsertPos = sortedIndices.end();
  for (size_t i : ad_utility::integerRange(numNewWords) | ql::views::reverse) {
    auto insertPos =
        sortedIndices.begin() + static_cast<std::ptrdiff_t>(insertPositions[i]);
    ql::shift_right(insertPos, previousInsertPos,
                    static_cast<std::ptrdiff_t>(i + 1));
    previousInsertPos = insertPos + static_cast<std::ptrdiff_t>(i);
    *previousInsertPos = firstGlobalIndex + i;
  }
}

// _____________________________________________________________________________
std::pair<size_t, bool> SecondaryVocabulary::lowerBoundInSortedIndices(
    std::string_view word, size_t first) const {
  auto sortedIndices = sortedIndices_.view();
  auto project = [this](uint64_t globalIndex) { return wordAt(globalIndex); };
  auto it = ql::ranges::lower_bound(sortedIndices.begin() + first,
                                    sortedIndices.end(), word, {}, project);
  bool found = it != sortedIndices.end() && project(*it) == word;
  return {static_cast<size_t>(it - sortedIndices.begin()), found};
}

// _____________________________________________________________________________
std::string_view SecondaryVocabulary::wordAt(uint64_t globalIndex) const {
  return (*this)[SecondaryVocabIndex::make(globalIndex)];
}
