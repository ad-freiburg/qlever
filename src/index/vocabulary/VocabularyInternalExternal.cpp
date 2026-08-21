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
// Partition input indices into internal matches and external misses, keeping
// track of their positions in the original input.
struct IndexPartition {
  std::vector<std::pair<size_t, std::string_view>> internalSlots;
  std::vector<std::pair<size_t, size_t>> diskSlots;  // (inputPosition, idx)
};

static IndexPartition partitionIndicesBySource(
    ql::span<const size_t> indices,
    const VocabularyInMemoryBinSearch& internalVocab) {
  IndexPartition result;
  result.internalSlots.reserve(indices.size());
  result.diskSlots.reserve(indices.size());

  for (auto [i, idx] : ::ranges::views::enumerate(indices)) {
    auto fromInternal = internalVocab[idx];
    if (fromInternal.has_value()) {
      result.internalSlots.emplace_back(static_cast<size_t>(i),
                                        fromInternal.value());
    } else {
      result.diskSlots.emplace_back(static_cast<size_t>(i), idx);
    }
  }
  return result;
}

VocabBatchLookupResult VocabularyInternalExternal::lookupBatch(
    ql::span<const size_t> indices) const {
  AD_CONTRACT_CHECK(!indices.empty());

  // Partition indices by source (internal vs external). The helper computes
  // both the vocabulary indices and their positions in the original input,
  // avoiding duplicate classification logic.
  auto partition = partitionIndicesBySource(indices, internalVocab_);

  // Fast path: all indices are in the external (disk) vocabulary.
  if (partition.internalSlots.empty()) {
    std::vector<size_t> diskIndices;
    diskIndices.reserve(partition.diskSlots.size());
    for (const auto& [position, idx] : partition.diskSlots) {
      diskIndices.push_back(idx);
    }
    return externalVocab_.lookupBatch(diskIndices);
  }

  // Slow path: mixed internal + external. Assemble results from both sources.
  std::vector<std::string_view> assembled(indices.size());

  // Fill in internal results first.
  for (const auto& [position, word] : partition.internalSlots) {
    assembled[position] = word;
  }

  // Gather disk results and scatter them into the assembled vector.
  std::vector<VocabBatchOwner> owners;
  if (!partition.diskSlots.empty()) {
    // Extract the disk indices (second element of each pair).
    std::vector<size_t> diskIndices;
    diskIndices.reserve(partition.diskSlots.size());
    for (const auto& [position, idx] : partition.diskSlots) {
      diskIndices.push_back(idx);
    }

    // Extract the positions in the original input (first element of each pair).
    std::vector<size_t> diskSlots;
    diskSlots.reserve(partition.diskSlots.size());
    for (const auto& [position, idx] : partition.diskSlots) {
      diskSlots.push_back(position);
    }

    auto disk = externalVocab_.lookupBatch(diskIndices);
    owners.reserve(1 + static_cast<size_t>(!partition.internalSlots.empty()));
    scatterVocabBatchLookupResult(std::move(disk), diskSlots, assembled,
                                  owners);
  }

  // Add ownership of internal data.
  if (!partition.internalSlots.empty()) {
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
