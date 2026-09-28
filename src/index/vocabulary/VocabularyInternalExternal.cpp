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

// Helpers for `VocabularyInternalExternal::lookupBatch` (see below).
namespace {

// The requested indices split by the vocabulary that resolves them. Each pair
// records the underlying vocabulary index together with its position in the
// original input, which restores the request order when the two sub-results
// are assembled.
//
// Classification requires one membership probe per index: the internal
// vocabulary has "holes", and its batch lookup reports missing entries as
// placeholders rather than failures, so a single optimistic batch cannot
// distinguish hits from misses. The probe is a binary search without any
// allocation; indices at or past `internalVocab.endIndex()` are known misses in
// O(1) and skip the search.
struct IndexPartition {
  MarkerIndicesAndPositions internalSlots;
  MarkerIndicesAndPositions diskSlots;
};

// _____________________________________________________________________________
IndexPartition partitionIndicesBySource(
    ql::span<const size_t> indices,
    const VocabularyInMemoryBinSearch& internalVocab) {
  IndexPartition result;
  result.internalSlots.reserve(indices.size());
  result.diskSlots.reserve(indices.size());

  const uint64_t internalEnd = internalVocab.endIndex();
  for (const auto& [i, idx] : ::ranges::views::enumerate(indices)) {
    const uint64_t vocabIndex = static_cast<uint64_t>(idx);
    if (vocabIndex < internalEnd &&
        internalVocab.positionOfIndex(vocabIndex).has_value()) {
      result.internalSlots.addPair(idx, i);
    } else {
      result.diskSlots.addPair(idx, i);
    }
  }
  return result;
}
}  // namespace

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyInternalExternal::lookupBatch(
    ql::span<const size_t> indices) const {
  AD_CONTRACT_CHECK(!indices.empty());

  auto partition = partitionIndicesBySource(indices, internalVocab_);

  // Take the fast path when all indices are resolved through the external
  // (disk) vocabulary.
  if (partition.internalSlots.empty()) {
    return externalVocab_.lookupBatch(
        partition.diskSlots.getUnderlyingIndices());
  }

  if (partition.diskSlots.empty()) {
    return internalVocab_.lookupBatch(
        partition.internalSlots.getUnderlyingIndices());
  }

  // Handle mixed internal and external indices by assembling results from both
  // sources. This path provides the basic exception guarantee: if either
  // sub-lookup throws, the partially assembled state is discarded with the
  // local `assembler`, so no partial result is observable by the caller.
  MultiSourceVocabBatchAssembler assembler(indices.size());

  // 1. Pass the internal sub-result to the assembler, which takes ownership of
  // the result data so its string views remain valid, and place the values at
  // their original request positions.
  auto internal = internalVocab_.lookupBatch(
      partition.internalSlots.getUnderlyingIndices());
  assembler.scatterSubBatchResultAtPositions(
      internal, partition.internalSlots.getResultPositions());

  // 2. Pass the external sub-result to the assembler and retain its result data
  // so the returned string views remain valid, placing the values at their
  // original request positions.
  auto disk =
      externalVocab_.lookupBatch(partition.diskSlots.getUnderlyingIndices());
  assembler.scatterSubBatchResultAtPositions(
      disk, partition.diskSlots.getResultPositions());

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
