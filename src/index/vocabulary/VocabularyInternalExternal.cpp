// Copyright 2024 - 2026, The QLever Authors, in particular:
//
// 2024 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/VocabularyInternalExternal.h"

#include <optional>
#include <range/v3/view/enumerate.hpp>
#include <string>
#include <string_view>
#include <utility>

// _____________________________________________________________________________
std::string VocabularyInternalExternal::operator[](uint64_t i) const {
  auto fromInternal = internalVocab_[i];
  if (fromInternal.has_value()) {
    return std::string{fromInternal.value()};
  }
  return externalVocab_[i];
}

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyInternalExternal::lookupBatch(
    ql::span<const size_t> indices) const {
  AD_CONTRACT_CHECK(!indices.empty());

  // One pass over `indices`: a word of the internal vocabulary is placed as a
  // view into that vocabulary (no copy); all other indices are collected, with
  // their positions in `indices`, for one batched lookup in the external
  // vocabulary. The internal vocabulary has "holes", so each index needs one
  // membership probe (an allocation-free binary search); indices at or past
  // `internalVocab_.endIndex()` are known misses and skip the search.
  MultiSourceVocabBatchAssembler assembler(indices.size());
  MarkerIndicesAndPositions externalSlots;
  const uint64_t internalEnd = internalVocab_.endIndex();
  for (const auto& [position, index] : ::ranges::views::enumerate(indices)) {
    auto internalWord = index < internalEnd ? internalVocab_[index]
                                            : std::optional<std::string_view>{};
    if (internalWord.has_value()) {
      assembler.assignUnownedViewAtPosition(position, internalWord.value());
    } else {
      externalSlots.addPair(index, position);
    }
  }

  if (externalSlots.empty()) {
    return std::move(assembler).finalizeVocabBatchLookupResult();
  }
  auto external =
      externalVocab_.lookupBatch(externalSlots.getUnderlyingIndices());
  if (externalSlots.size() == indices.size()) {
    // No internal hit: the positions are `0, 1, ...`, so the external batch
    // already is the result.
    return external;
  }
  assembler.scatterSubBatchResultAtPositions(
      std::move(external), externalSlots.getResultPositions());
  return std::move(assembler).finalizeVocabBatchLookupResult();
}

// _____________________________________________________________________________
VocabularyInternalExternal::WordWriter::WordWriter(const std::string& filename,
                                                   size_t milestoneDistance)
    : internalWriter_{filename + ".internal"},
      externalWriter_{filename + ".external"},
      milestoneDistance_{milestoneDistance} {}

// _____________________________________________________________________________
uint64_t VocabularyInternalExternal::WordWriter::operator()(
    std::string_view str, bool isExternal) {
  externalWriter_(str, true);
  if (!isExternal || sinceMilestone_ >= milestoneDistance_ || idx_ == 0) {
    internalWriter_(str, idx_);
    sinceMilestone_ = 0;
  }
  ++sinceMilestone_;
  return idx_++;
}

// _____________________________________________________________________________
void VocabularyInternalExternal::WordWriter::finishImpl() {
  internalWriter_.finish();
  externalWriter_.finish();
}

// _____________________________________________________________________________
VocabularyInternalExternal::WordWriter::~WordWriter() {
  if (!finishWasCalled()) {
    ad_utility::terminateIfThrows([this]() { this->finish(); },
                                  "Calling `finish` from the destructor of "
                                  "`VocabularyInternalExternal::WordWriter`");
  }
}

// _____________________________________________________________________________
void VocabularyInternalExternal::open(const std::string& filename) {
  AD_LOG_INFO << "Reading vocabulary from file " << filename << " ..."
              << std::endl;
  internalVocab_.open(filename + ".internal");
  externalVocab_.open(filename + ".external");
  AD_LOG_INFO << "Done, number of words: " << size() << std::endl;
  AD_LOG_INFO << "Number of words in internal vocabulary (these are also part "
                 "of the external vocabulary): "
              << internalVocab_.size() << std::endl;
}
