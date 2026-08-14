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
  AD_CONTRACT_CHECK(!indices.empty());

  std::vector<std::string> words(indices.size());
  std::vector<size_t> diskIndices;
  std::vector<size_t> diskSlots;
  diskIndices.reserve(indices.size());
  diskSlots.reserve(indices.size());

  for (auto [i, idx] : ::ranges::views::enumerate(indices)) {
    auto fromInternal = internalVocab_[idx];
    if (fromInternal.has_value()) {
      words[i] = std::string{fromInternal.value()};
    } else {
      diskSlots.push_back(i);
      diskIndices.push_back(idx);
    }
  }

  if (!diskIndices.empty()) {
    auto disk = externalVocab_.lookupBatch(diskIndices);
    AD_CORRECTNESS_CHECK(disk->size() == diskIndices.size());
    for (auto [slot, word] : ::ranges::views::zip(diskSlots, *disk)) {
      words[slot] = std::string{word};
    }
  }

  auto data = std::make_shared<StringVectorVocabBatchLookupData>();
  data->buffer() = std::move(words);
  data->views() = ::ranges::to_vector(
      data->buffer() |
      ql::views::transform(ad_utility::staticCast<std::string_view>));
  return StringVectorVocabBatchLookupData::asResult(std::move(data));
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
