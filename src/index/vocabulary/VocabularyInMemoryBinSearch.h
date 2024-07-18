// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#pragma once

#include <string>
#include <string_view>

#include "global/Pattern.h"
#include "index/CompressedString.h"
#include "index/vocabulary/VocabularyBinarySearchMixin.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/Algorithm.h"
#include "util/Exception.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeVector.h"

//! A vocabulary. Wraps a `CompactVectorOfStrings<char>`
//! and provides additional methods for reading and writing to/from file,
//! and retrieval via binary search.
class VocabularyInMemoryBinSearch
    : public VocabularyBinarySearchMixin<VocabularyInMemoryBinSearch> {
 public:
  using CharType = char;
  using StringView = std::basic_string_view<CharType>;
  using String = std::basic_string<CharType>;
  using Words = CompactVectorOfStrings<CharType>;
  using Offsets = std::vector<uint64_t>;

 private:
  // The actual storage.
  Words _words;
  Offsets offsets_;

 public:
  /// Construct an empty vocabulary
  VocabularyInMemoryBinSearch() = default;

  /// Construct the vocabulary from `Words`
  explicit VocabularyInMemoryBinSearch(Words words, Offsets offsets)
      : _words{std::move(words)}, offsets_{std::move(offsets)} {}

  // Vocabularies are movable
  VocabularyInMemoryBinSearch& operator=(
      VocabularyInMemoryBinSearch&&) noexcept = default;
  VocabularyInMemoryBinSearch(VocabularyInMemoryBinSearch&&) noexcept = default;

  const auto& offsets() const { return offsets_; }

  /// Read the vocabulary from a file. The file must have been created by a call
  /// to `writeToFile` or using a `WordWriter`.
  void open(const string& fileName);

  /// Write the vocabulary to a file.
  void writeToFile(const string& fileName) const;

  /// Return the total number of words
  [[nodiscard]] size_t size() const {
    AD_CORRECTNESS_CHECK(offsets_.size() == _words.size());
    return _words.size();
  }

  /// Return the highest ID (= index) that occurs in this vocabulary. May only
  /// becalled if size() > 0.
  [[nodiscard]] uint64_t getHighestId() const {
    AD_CONTRACT_CHECK(size() > 0);
    return offsets_.back();
  }

  /// Return the `i-th` word. The behavior is undefined if `i >= size()`
  std::optional<std::string_view> operator[](uint64_t i) const {
    auto it = std::ranges::lower_bound(offsets_, i);
    if (it != offsets_.end() && *it == i) {
      return _words[i];
    }
    return std::nullopt;
  }

  WordAndIndex boundImpl(auto it) const {
    WordAndIndex result;
    auto idx = static_cast<uint64_t>(it - _words.begin());
    result._index = idx < size() ? offsets_[idx] : getHighestId() + 1;
    if (idx > 0) {
      result._previousIndex = offsets_[idx - 1];
    }
    if (idx + 1 < size()) {
      result._nextIndex = offsets_[idx + 1];
    }
    result._word = idx < size() ? std::optional{_words[idx]} : std::nullopt;
    return result;
  }

  /// A helper type that can be used to directly write a vocabulary to disk
  /// word-by-word, without having to materialize it in RAM first. See the
  /// documentation of `CompactVectorOfStrings` for details.
  struct WordWriter {
    typename Words::Writer writer_;
    using OffsetWriter = ad_utility::serialization::VectorIncrementalSerializer<
        uint64_t, ad_utility::serialization::FileWriteSerializer>;
    OffsetWriter offsetWriter_;
    explicit WordWriter(const std::string& filename)
        : writer_{filename}, offsetWriter_{filename + ".ids"} {}
    void operator()(std::string_view str, uint64_t idx) {
      writer_.push(str.data(), str.size());
      offsetWriter_.push(idx);
    }

    void finish() {
      writer_.finish();
      offsetWriter_.finish();
    }
  };

  // Return a `WordWriter` that directly writes the words to the given
  // `filename`. The words are not materialized in RAM, but the vocabulary later
  // has to be explicitly initizlied via `open(filename)`.
  WordWriter makeDiskWriter(const std::string& filename) const {
    return WordWriter{filename};
  }

  /// Clear the vocabulary.
  void close() {
    _words.clear();
    offsets_.clear();
  }

  /// Initialize the vocabulary from the given `words`.
  void build(const std::vector<std::string>& words,
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

  // Const access to the underlying words.
  auto begin() const { return _words.begin(); }
  auto end() const { return _words.end(); }
};
