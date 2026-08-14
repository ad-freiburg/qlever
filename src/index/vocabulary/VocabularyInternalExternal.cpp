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

#include <string>
#include <vector>

#include "backports/algorithm.h"
#include "util/TransparentFunctors.h"

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
  auto handle = std::make_unique<MixedLookupHandle>();
  handle->vocab_ = this;
  handle->numIndices_ = indices.size();

  std::vector<size_t> externalIndices;
  externalIndices.reserve(indices.size());
  handle->internalWords_.reserve(indices.size());
  handle->internalPositions_.reserve(indices.size());
  handle->externalPositions_.reserve(indices.size());

  // Classify each index: RAM hits are resolved immediately, disk misses are
  // collected into `externalIndices` for one batched on-disk lookup.
  for (auto [pos, idx] : ::ranges::views::enumerate(indices)) {
    auto fromInternal = internalVocab_[idx];
    if (fromInternal.has_value()) {
      handle->internalWords_.emplace_back(*fromInternal);
      handle->internalPositions_.push_back(pos);
    } else {
      externalIndices.push_back(idx);
      handle->externalPositions_.push_back(pos);
    }
  }

  // Submit the reads for all disk misses in one non-blocking `beginLookup`.
  if (!externalIndices.empty()) {
    handle->externalHandle_ = externalVocab_.beginLookup(externalIndices);
  }

  return handle;
}

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyInternalExternal::finishLookup(
    std::unique_ptr<VocabLookupHandleBase> handleBase) const {
  auto* handle = static_cast<MixedLookupHandle*>(handleBase.get());
  AD_CONTRACT_CHECK(handle != nullptr && handle->vocab_ == this);
  return handle->finish();
}

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyInternalExternal::MixedLookupHandle::finish() {
  auto data = std::make_shared<MixedVocabBatchLookupData>();
  data->internalWords_ = std::move(internalWords_);
  data->views_.resize(numIndices_);
  if (externalHandle_) {
    data->diskResult_ =
        vocab_->externalVocab_.finishLookup(std::move(externalHandle_));
    for (auto [position, word] :
         ::ranges::views::zip(externalPositions_, *data->diskResult_)) {
      data->views_[position] = word;
    }
  }
  for (auto [position, word] :
       ::ranges::views::zip(internalPositions_, data->internalWords_)) {
    data->views_[position] = word;
  }
  return MixedVocabBatchLookupData::asResult(std::move(data));
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
