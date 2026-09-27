// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/VocabularyInternalExternal.h"

#include <absl/strings/str_cat.h>

#include "util/Algorithm.h"

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
  // Collect the indices that miss the internal vocabulary, so that the
  // external vocabulary serves all of them in one batch (from its `io_uring`
  // ring pool).
  auto data = std::make_shared<StringVectorVocabBatchLookupData>();
  data->buffer().resize(indices.size());
  std::vector<size_t> missPositions;
  std::vector<size_t> missIndices;
  missPositions.reserve(indices.size());
  missIndices.reserve(indices.size());
  for (size_t i = 0; i < indices.size(); ++i) {
    if (auto hit = internalVocab_[indices[i]]; hit.has_value()) {
      data->buffer()[i] = std::string{hit.value()};
    } else {
      missPositions.push_back(i);
      missIndices.push_back(indices[i]);
    }
  }
  if (!missIndices.empty()) {
    auto external = externalVocab_.lookupBatch(missIndices);
    // Internal invariant (not violable via the public interface): the external
    // lookup yields one word per index, in order.
    AD_CORRECTNESS_CHECK(external->size() == missIndices.size());
    for (const auto& [position, word] :
         ::ranges::views::zip(missPositions, *external)) {
      data->buffer()[position] = std::string{word};
    }
  }
  // Build the views only after the buffer is complete, so that no reallocation
  // can move the bytes the views point into.
  data->views().reserve(data->buffer().size());
  for (const auto& word : data->buffer()) {
    data->views().emplace_back(word);
  }
  return StringVectorVocabBatchLookupData::asResult(std::move(data));
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
