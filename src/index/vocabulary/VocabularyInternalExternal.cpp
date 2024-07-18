// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (kalmbach@cs.uni-freiburg.de)

#include "./VocabularyInternalExternal.h"

// _____________________________________________________________________________
std::string VocabularyInternalExternal::operator[](uint64_t i) const {
  auto fromInternal = internalVocab_[i];
  if (fromInternal.has_value()) {
    return std::string{fromInternal.value()};
  }
  return externalVocab_[i];
}

// _____________________________________________________________________________
void VocabularyInternalExternal::build(const std::vector<std::string>& words,
                                       const std::string& filename) {
  WordWriter writer = makeDiskWriter(filename);
  for (const auto& word : words) {
    writer(word, false);
  }
  writer.finish();
  open(filename);
}

// _____________________________________________________________________________
VocabularyInternalExternal::WordWriter::WordWriter(const std::string& filename,
                                                   size_t milestoneDistance)
    : internalWriter_{filename + ".internal"},
      externalWriter_{filename + ".external"},
      milestoneDistance_{milestoneDistance} {}

// _____________________________________________________________________________
void VocabularyInternalExternal::WordWriter::operator()(std::string_view str,
                                                        bool isExternal) {
  externalWriter_(str);
  if (!isExternal || sinceMilestone >= milestoneDistance_ || idx_ == 0) {
    internalWriter_(str, idx_);
    sinceMilestone = 0;
  }
  ++idx_;
  ++sinceMilestone;
}

// _____________________________________________________________________________
void VocabularyInternalExternal::WordWriter::finish() {
  internalWriter_.finish();
  externalWriter_.finish();
}
