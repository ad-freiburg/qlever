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

#include <absl/strings/str_cat.h>

#include <memory>
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
  return finishLookup(beginLookup(indices));
}

// _____________________________________________________________________________
std::unique_ptr<VocabLookupHandleBase> VocabularyInternalExternal::beginLookup(
    ql::span<const size_t> indices) const {
  AD_CONTRACT_CHECK(!indices.empty());

  // One pass over `indices`: a word of the internal vocabulary is placed as a
  // view into that vocabulary (no copy); all other indices are collected, with
  // their positions in `indices`, for one batched lookup in the external
  // vocabulary. The internal vocabulary has "holes", so each index needs one
  // membership probe (an allocation-free binary search); indices at or past
  // `internalVocab_.endIndex()` are known misses and skip the search.
  auto handle = std::unique_ptr<MixedLookupHandle>(
      new MixedLookupHandle(*this, indices.size()));
  const uint64_t internalEnd = internalVocab_.endIndex();
  for (const auto& [position, index] : ::ranges::views::enumerate(indices)) {
    auto internalWord = index < internalEnd ? internalVocab_[index]
                                            : std::optional<std::string_view>{};
    if (internalWord.has_value()) {
      handle->assembler_.assignUnownedViewAtPosition(position,
                                                     internalWord.value());
    } else {
      handle->externalSlots_.addPair(index, position);
    }
  }

  // Submit the reads for all external words in one non-blocking
  // `beginLookup`; `finish` waits for them.
  if (!handle->externalSlots_.empty()) {
    handle->externalHandle_ = externalVocab_.beginLookup(
        handle->externalSlots_.getUnderlyingIndices());
  }
  return handle;
}

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyInternalExternal::finishLookup(
    std::unique_ptr<VocabLookupHandleBase> handleBase) const {
  AD_CONTRACT_CHECK(handleBase != nullptr);
  return handleBase->finish();
}

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyInternalExternal::MixedLookupHandle::finish() {
  if (externalSlots_.empty()) {
    return std::move(assembler_).finalizeVocabBatchLookupResult();
  }
  AD_CORRECTNESS_CHECK(externalHandle_ != nullptr);
  auto external =
      vocab_->externalVocab_.finishLookup(std::move(externalHandle_));
  if (externalSlots_.size() == numIndices_) {
    // No internal hit: the positions are `0, 1, ...`, so the external batch
    // already is the result.
    return external;
  }
  assembler_.scatterSubBatchResultAtPositions(
      std::move(external), externalSlots_.getResultPositions());
  return std::move(assembler_).finalizeVocabBatchLookupResult();
}

// _____________________________________________________________________________
VocabularyInternalExternal::WordWriter::WordWriter(const std::string& filename,
                                                   size_t milestoneDistance)
    : internalWriter_{absl::StrCat(filename, internalSuffix)},
      externalWriter_{absl::StrCat(filename, externalSuffix)},
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
  internalVocab_.open(absl::StrCat(filename, internalSuffix));
  externalVocab_.open(absl::StrCat(filename, externalSuffix));
  AD_LOG_INFO << "Done, number of words: " << size() << std::endl;
  AD_LOG_INFO << "Number of words in internal vocabulary (these are also part "
                 "of the external vocabulary): "
              << internalVocab_.size() << std::endl;
}
