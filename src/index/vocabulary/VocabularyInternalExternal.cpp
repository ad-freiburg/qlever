// Copyright 2024, 2026, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//         Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR

#include "index/vocabulary/VocabularyInternalExternal.h"

#include <string>
#include <string_view>
#include <utility>
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
// Paired lookup data for disk/external vocabulary indices: keeps underlying
// indices to look up and output positions in sync.
class DiskLookupData {
 private:
  std::vector<size_t> indices_;
  std::vector<size_t> positions_;

 public:
  void reserve(size_t capacity) {
    indices_.reserve(capacity);
    positions_.reserve(capacity);
  }

  void add(size_t idx, size_t position) {
    indices_.push_back(idx);
    positions_.push_back(position);
  }

  [[nodiscard]] ql::span<const size_t> indices() const { return indices_; }
  [[nodiscard]] ql::span<const size_t> positions() const { return positions_; }
  [[nodiscard]] bool empty() const { return indices_.empty(); }
  [[nodiscard]] size_t size() const { return indices_.size(); }
};

// Partition input indices into internal-vocabulary hits and indices that must
// be resolved by the external vocabulary, while keeping their positions in the
// original input.
struct IndexPartition {
  // (inputPosition, word)
  std::vector<std::pair<size_t, std::string_view>> internalSlots_;
  DiskLookupData diskSlots_;
};

static IndexPartition partitionIndicesBySource(
    ql::span<const size_t> indices,
    const VocabularyInMemoryBinSearch& internalVocab) {
  IndexPartition result;
  result.internalSlots_.reserve(indices.size());
  result.diskSlots_.reserve(indices.size());

  for (auto [i, idx] : ::ranges::views::enumerate(indices)) {
    auto fromInternal = internalVocab[idx];
    if (fromInternal.has_value()) {
      result.internalSlots_.emplace_back(static_cast<size_t>(i),
                                         fromInternal.value());
    } else {
      result.diskSlots_.add(idx, static_cast<size_t>(i));
    }
  }
  return result;
}

VocabBatchLookupResult VocabularyInternalExternal::lookupBatch(
    ql::span<const size_t> indices) const {
  AD_CONTRACT_CHECK(!indices.empty());

  auto partition = partitionIndicesBySource(indices, internalVocab_);

  // Take the fast path when all indices are resolved through the external
  // (disk) vocabulary.
  if (partition.internalSlots_.empty()) {
    return externalVocab_.lookupBatch(partition.diskSlots_.indices());
  }

  // Handle mixed internal and external indices by assembling results from both
  // sources.
  std::vector<std::string_view> assembled(indices.size());

  // Fill in internal results first.
  for (const auto& [position, word] : partition.internalSlots_) {
    assembled[position] = word;
  }

  // Gather disk results and scatter them into the assembled vector.
  std::vector<VocabBatchOwner> owners;
  if (!partition.diskSlots_.empty()) {
    auto disk = externalVocab_.lookupBatch(partition.diskSlots_.indices());
    owners.reserve(2);
    scatterVocabBatchLookupResult(
        std::move(disk), partition.diskSlots_.positions(), assembled, owners);
  }

  // Add ownership of internal data.
  owners.push_back(internalVocab_.wordStorage());

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
