// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (kalmbach@cs.uni-freiburg.de)

#include "index/vocabulary/VocabularyInternalExternal.h"

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
  // Step 1: Look up all indices in the internal (RAM) vocabulary.
  auto internalResult = internalVocab_.lookupBatch(indices);

  // Step 2: Identify which indices were not found in the internal vocabulary.
  std::vector<size_t> missingIndices;
  for (size_t i = 0; i < indices.size(); ++i) {
    if (!(*internalResult)[i].has_value()) {
      missingIndices.push_back(indices[i]);
    }
  }

  // Step 3: Look up the missing indices in the external (disk) vocabulary.
  VocabBatchLookupResult externalResult;
  if (!missingIndices.empty()) {
    externalResult = externalVocab_.lookupBatch(missingIndices);
  }

  // Step 4: Combine results. We need a struct that keeps both the internal and
  // external results alive, since our string_views point into their memory.
  struct CombinedData {
    decltype(internalResult) internal;
    VocabBatchLookupResult external;
    std::vector<std::string_view> views;
    ql::span<std::string_view> span;
  };

  auto combined = std::make_shared<CombinedData>();
  combined->internal = std::move(internalResult);
  combined->external = std::move(externalResult);
  combined->views.resize(indices.size());

  size_t externalIdx = 0;
  for (size_t i = 0; i < indices.size(); ++i) {
    if ((*combined->internal)[i].has_value()) {
      combined->views[i] = (*combined->internal)[i].value();
    } else {
      combined->views[i] = (*combined->external)[externalIdx++];
    }
  }

  combined->span = ql::span<std::string_view>{combined->views};
  auto* spanPtr = &combined->span;
  return VocabBatchLookupResult(std::move(combined), spanPtr);
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
