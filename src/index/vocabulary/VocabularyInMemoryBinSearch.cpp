// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#include "./VocabularyInMemoryBinSearch.h"

using std::string;

// _____________________________________________________________________________
void VocabularyInMemoryBinSearch::open(const string& fileName) {
  words_.clear();
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
    uint64_t i) const {
  auto it = std::ranges::lower_bound(indices_, i);
  if (it != indices_.end() && *it == i) {
    return words_[it - indices_.begin()];
  }
  return std::nullopt;
}

// _____________________________________________________________________________
WordAndIndex VocabularyInMemoryBinSearch::boundImpl(
    std::ranges::iterator_t<Words> it) const {
  WordAndIndex result;
  auto idx = static_cast<uint64_t>(it - words_.begin());
  result._index = idx < size() ? indices_[idx] : getHighestId() + 1;
  if (idx > 0) {
    result._previousIndex = indices_[idx - 1];
  }
  if (idx + 1 < size()) {
    result._nextIndex = indices_[idx + 1];
  }
  result._word = idx < size() ? std::optional{words_[idx]} : std::nullopt;
  return result;
}
// _____________________________________________________________________________
void VocabularyInMemoryBinSearch::build(const std::vector<std::string>& words,
                                        const std::string& filename) {
  WordWriter writer = makeDiskWriter(filename);
  uint64_t idx = 0;
  for (const auto& word : words) {
    writer(word, idx);
    ++idx;
  }
  writer.finish();
  open(filename);
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
void VocabularyInMemoryBinSearch::WordWriter::operator()(std::string_view str,
                                                         uint64_t idx) {
  // Check that the indices are ascending.
  AD_CONTRACT_CHECK(!lastIndex_.has_value() || lastIndex_.value() < idx);
  lastIndex_ = idx;
  writer_.push(str.data(), str.size());
  offsetWriter_.push(idx);
}

// _____________________________________________________________________________
void VocabularyInMemoryBinSearch::WordWriter::finish() {
  writer_.finish();
  offsetWriter_.finish();
}
