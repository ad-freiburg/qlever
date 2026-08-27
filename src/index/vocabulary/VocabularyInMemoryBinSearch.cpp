// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#include "index/vocabulary/VocabularyInMemoryBinSearch.h"

using std::string;

// _____________________________________________________________________________
VocabularyInMemoryBinSearch::IndicesView VocabularyInMemoryBinSearch::indices()
    const {
  return std::visit(
      [](const auto& indices) -> IndicesView {
        return {indices.data(), indices.size()};
      },
      indices_);
}

// _____________________________________________________________________________
void VocabularyInMemoryBinSearch::open(const string& fileName) {
  AD_CORRECTNESS_CHECK(
      words_.size() == 0 && indices().empty(),
      "Calling open on the same vocabulary twice is probably a bug");
  {
    ad_utility::serialization::FileReadSerializer file(fileName);
    file >> words_;
  }
  {
    ad_utility::serialization::FileReadSerializer idFile(fileName + ".ids");
    idFile >> ownedIndices();
  }
}

// _____________________________________________________________________________
std::optional<size_t> VocabularyInMemoryBinSearch::positionOfIndex(
    uint64_t index) const {
  auto indices = this->indices();
  auto it = ql::ranges::lower_bound(indices, index);
  if (it != indices.end() && *it == index) {
    return static_cast<size_t>(it - indices.begin());
  }
  return std::nullopt;
}

// _____________________________________________________________________________
uint64_t VocabularyInMemoryBinSearch::indexAtPosition(size_t position) const {
  auto indices = this->indices();
  AD_CORRECTNESS_CHECK(position < indices.size());
  return indices[position];
}

// _____________________________________________________________________________
uint64_t VocabularyInMemoryBinSearch::endIndex() const {
  auto indices = this->indices();
  return indices.empty() ? 0 : indices[indices.size() - 1] + 1;
}

// _____________________________________________________________________________
std::optional<std::string_view> VocabularyInMemoryBinSearch::operator[](
    uint64_t index) const {
  auto position = positionOfIndex(index);
  if (!position.has_value()) {
    return std::nullopt;
  }
  return words_[position.value()];
}

// _____________________________________________________________________________
WordAndIndex VocabularyInMemoryBinSearch::iteratorToWordAndIndex(
    ql::ranges::iterator_t<Words> it) const {
  if (it == words_.end()) {
    return WordAndIndex::end();
  }
  auto idx = static_cast<uint64_t>(it - words_.begin());
  auto indices = this->indices();
  WordAndIndex result{words_[idx], indices[idx]};
  if (idx > 0) {
    result.previousIndex() = indices[idx - 1];
  }
  return result;
}

// _____________________________________________________________________________
[[noreturn]] std::unique_ptr<WordWriterBase>
VocabularyInMemoryBinSearch::makeDiskWriterPtr(
    [[maybe_unused]] const std::string& filename) {
  AD_THROW(
      "A vocabulary with holes cannot be built word by word, because the "
      "`WordWriterBase` interface cannot express the explicit indices. Such a "
      "vocabulary can only be created by filtering an existing vocabulary.");
}

// _____________________________________________________________________________
void VocabularyInMemoryBinSearch::close() {
  words_.clear();
  indices_.emplace<Indices>();
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
