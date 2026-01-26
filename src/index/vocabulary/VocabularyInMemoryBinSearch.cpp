// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#include "index/vocabulary/VocabularyInMemoryBinSearch.h"

using std::string;

// _____________________________________________________________________________
void VocabularyInMemoryBinSearch::open(const string& fileName) {
  AD_CORRECTNESS_CHECK(
      words_.size() == 0 && indices_.empty(),
      "Calling open on the same vocabulary twice is probably a bug");
  {
    ad_utility::serialization::FileReadSerializer file(fileName);
    file >> words_;
  }
  {
    ad_utility::serialization::FileReadSerializer idFile(fileName + ".ids");
    idFile >> indices_;
  }
}

// _____________________________________________________________________________
std::optional<std::string_view> VocabularyInMemoryBinSearch::operator[](
    uint64_t index) const {
  auto it = ql::ranges::lower_bound(indices_, index);
  if (it != indices_.end() && *it == index) {
    return words_[it - indices_.begin()];
  }
  return std::nullopt;
}

// _____________________________________________________________________________
WordAndIndex VocabularyInMemoryBinSearch::iteratorToWordAndIndex(
    ql::ranges::iterator_t<Words> it) const {
  if (it == words_.end()) {
    return WordAndIndex::end();
  }
  auto idx = static_cast<uint64_t>(it - words_.begin());
  WordAndIndex result{words_[idx], indices_[idx]};
  if (idx > 0) {
    result.previousIndex() = indices_[idx - 1];
  }
  return result;
}

// _____________________________________________________________________________
void VocabularyInMemoryBinSearch::close() {
  words_.clear();
  indices_.clear();
}

// _____________________________________________________________________________
VocabularyInMemoryBinSearch::WordWriter::WordWriter(const std::string& filename)
    : writer_{filename}, offsetWriter_{filename + ".ids"} {}

// _____________________________________________________________________________
uint64_t VocabularyInMemoryBinSearch::WordWriter::operator()(
    std::string_view str, uint64_t idx) {
  // Check that the indices are ascending.
  AD_CONTRACT_CHECK(!lastIndex_.has_value() || lastIndex_.value() < idx);
  lastIndex_ = idx;
  writer_.push(str.data(), str.size());
  offsetWriter_.push(idx);
  return idx;
}

// _____________________________________________________________________________
void VocabularyInMemoryBinSearch::WordWriter::finish() {
  writer_.finish();
  offsetWriter_.finish();
}
