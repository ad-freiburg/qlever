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
#include <string_view>
#include <vector>

#include "backports/algorithm.h"

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

  // Classify without mixed-result buffers. The pure-disk path returns the
  // external batch directly and must not allocate `diskSlots` or `assembled`.
  std::vector<size_t> diskIndices;
  std::vector<std::pair<size_t, std::string_view>> internalSlots;
  diskIndices.reserve(indices.size());

  for (auto [i, idx] : ::ranges::views::enumerate(indices)) {
    auto fromInternal = internalVocab_[idx];
    if (fromInternal.has_value()) {
      internalSlots.emplace_back(static_cast<size_t>(i), fromInternal.value());
    } else {
      diskIndices.push_back(idx);
    }
  }

  // Hand the disk batch through so we do not copy the already-owned compressed
  // bytes.
  if (diskIndices.size() == indices.size()) {
    return externalVocab_.lookupBatch(diskIndices);
  }

  std::vector<std::string_view> assembled(indices.size());
  for (auto [position, word] : internalSlots) {
    assembled[position] = word;
  }
  std::vector<VocabBatchOwner> owners;
  if (!diskIndices.empty()) {
    // Mixed path only: input positions of disk misses, same order as
    // `diskIndices`.
    std::vector<char> isInternal(indices.size(), 0);
    for (const auto& [position, word] : internalSlots) {
      isInternal[position] = 1;
    }
    std::vector<size_t> diskSlots;
    diskSlots.reserve(diskIndices.size());
    for (size_t i = 0; i < indices.size(); ++i) {
      if (isInternal[i] == 0) {
        diskSlots.push_back(i);
      }
    }
    AD_CORRECTNESS_CHECK(diskSlots.size() == diskIndices.size());

    auto disk = externalVocab_.lookupBatch(diskIndices);
    owners.reserve(1 + static_cast<size_t>(!internalSlots.empty()));
    scatterVocabBatchLookupResult(std::move(disk), diskSlots, assembled,
                                  owners);
  }
  if (!internalSlots.empty()) {
    owners.push_back(internalVocab_.wordStorage());
  }
  return keepAliveVocabBatch(std::move(owners), std::move(assembled));
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
