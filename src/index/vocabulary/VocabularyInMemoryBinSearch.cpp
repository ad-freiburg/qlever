// Copyright 2024 - 2026, The QLever Authors, in particular:
//
// 2024 - 2026 Johannes Kalmbach <johannes.kalmbach@gmail.com>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

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
      words_->size() == 0 && indices().empty(),
      "Calling open on the same vocabulary twice is probably a bug");
  {
    // Deserialize into a mutable buffer first (`words_` stores `const Words`
    // for immutable sharing via `wordStorage()`, and moving on success ensures
    // strong exception safety).
    auto words = std::make_shared<Words>();
    ad_utility::serialization::FileReadSerializer file(fileName);
    file >> *words;
    words_ = std::move(words);
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
std::string_view VocabularyInMemoryBinSearch::wordAtPosition(
    size_t position) const {
  AD_CORRECTNESS_CHECK(position < words_->size());
  return (*words_)[position];
}

// _____________________________________________________________________________
std::optional<std::string_view> VocabularyInMemoryBinSearch::operator[](
    uint64_t index) const {
  auto position = positionOfIndex(index);
  if (!position.has_value()) {
    return std::nullopt;
  }
  return wordAtPosition(position.value());
}

// _____________________________________________________________________________
WordAndIndex VocabularyInMemoryBinSearch::iteratorToWordAndIndex(
    ql::ranges::iterator_t<Words> it) const {
  if (it == words().end()) {
    return WordAndIndex::end();
  }
  auto idx = static_cast<uint64_t>(it - words_->begin());
  auto indices = this->indices();
  WordAndIndex result{(*words_)[idx], indices[idx]};
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
  // Install a fresh empty buffer instead of clearing the existing one in place:
  // outstanding `VocabBatchLookupResult`s hold non-owning string_views into the
  // old character buffer along with a shared_ptr to it. Mutating the old buffer
  // in place would invalidate those views; replacing the pointer lets the old
  // buffer remain valid until all downstream results are destroyed.
  words_ = std::make_shared<const Words>();
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
