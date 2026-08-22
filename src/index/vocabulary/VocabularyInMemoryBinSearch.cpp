// Copyright 2024, 2026, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>
//         Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR

#include "index/vocabulary/VocabularyInMemoryBinSearch.h"

using std::string;

// _____________________________________________________________________________
void VocabularyInMemoryBinSearch::open(const string& fileName) {
  AD_CORRECTNESS_CHECK(
      words().size() == 0 && indices_.empty(),
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
    idFile >> indices_;
  }
}

// _____________________________________________________________________________
std::optional<std::string_view> VocabularyInMemoryBinSearch::operator[](
    uint64_t index) const {
  auto it = ql::ranges::lower_bound(indices_, index);
  if (it != indices_.end() && *it == index) {
    return words()[it - indices_.begin()];
  }
  return std::nullopt;
}

// _____________________________________________________________________________
WordAndIndex VocabularyInMemoryBinSearch::iteratorToWordAndIndex(
    ql::ranges::iterator_t<Words> it) const {
  if (it == words().end()) {
    return WordAndIndex::end();
  }
  auto idx = static_cast<uint64_t>(it - words().begin());
  WordAndIndex result{words()[idx], indices_[idx]};
  if (idx > 0) {
    result.previousIndex() = indices_[idx - 1];
  }
  return result;
}

// _____________________________________________________________________________
void VocabularyInMemoryBinSearch::close() {
  // Install a fresh empty buffer instead of clearing the existing one in place:
  // outstanding `VocabBatchLookupResult`s hold non-owning string_views into the
  // old character buffer along with a shared_ptr to it. Mutating the old buffer
  // in place would invalidate those views; replacing the pointer lets the old
  // buffer remain valid until all downstream results are destroyed.
  words_ = std::make_shared<const Words>();
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
