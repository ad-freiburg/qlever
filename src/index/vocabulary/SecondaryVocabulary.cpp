// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/SecondaryVocabulary.h"

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
  // Check that the words within `segment` are pairwise distinct, via a sorted
  // copy of the words (the segment itself may be in arbitrary order).
  std::vector<std::string_view> wordsOfSegment(segment.begin(), segment.end());
  ql::ranges::sort(wordsOfSegment);
  AD_CONTRACT_CHECK(
      ql::ranges::adjacent_find(wordsOfSegment) == wordsOfSegment.end(),
      "The words of a secondary vocabulary have to be distinct");

  // Check that none of the words of `segment` is already contained in this
  // vocabulary. NOTE: This has to happen before `segment` is appended below,
  // because `getId` must only find the words that were already contained.
  for (std::string_view word : wordsOfSegment) {
    AD_CONTRACT_CHECK(
        !getId(word).has_value(),
        "The words of a secondary vocabulary have to be distinct");
  }

  segmentOffsets_.push_back(numWords());
  segments_.push_back(std::move(segment));
  rebuildSortedIndices();
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
void SecondaryVocabulary::rebuildSortedIndices() {
  sortedIndices_.clear();
  sortedIndices_.reserve(numWords());
  for (uint64_t i = 0; i < numWords(); ++i) {
    sortedIndices_.push_back(i);
  }
  auto project = [this](uint64_t globalIndex) { return wordAt(globalIndex); };
  ql::ranges::sort(sortedIndices_, {}, project);
}

// _____________________________________________________________________________
std::string_view SecondaryVocabulary::wordAt(uint64_t globalIndex) const {
  return (*this)[SecondaryVocabIndex::make(globalIndex)];
}
