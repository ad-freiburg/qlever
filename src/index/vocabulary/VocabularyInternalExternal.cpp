// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (kalmbach@cs.uni-freiburg.de)

#include "index/vocabulary/VocabularyInternalExternal.h"

#include <absl/strings/str_cat.h>

// _____________________________________________________________________________
std::string VocabularyInternalExternal::operator[](uint64_t i) const {
  auto fromInternal = internalVocab_[i];
  if (fromInternal.has_value()) {
    return std::string{fromInternal.value()};
  }
  return externalVocab_[i];
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
void VocabularyInternalExternal::BlockWriter::append(
    std::unique_ptr<PreparedBlockBase> prepared) {
  const WordBlock& block = static_cast<Prepared&>(*prepared).block_;
  AD_CONTRACT_CHECK(block.firstPosition_ == writer_.idx_);
  writer_.externalWriter_.writeBlock(block);
  // The same rule as in `WordWriter::operator()`, only the external writer
  // has already been fed.
  for (size_t i = 0; i < block.numWords(); ++i) {
    if (!block.isExternal_[i] ||
        writer_.sinceMilestone_ >= writer_.milestoneDistance_ ||
        writer_.idx_ == 0) {
      writer_.internalWriter_(block.word(i), writer_.idx_);
      writer_.sinceMilestone_ = 0;
    }
    ++writer_.sinceMilestone_;
    ++writer_.idx_;
  }
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
