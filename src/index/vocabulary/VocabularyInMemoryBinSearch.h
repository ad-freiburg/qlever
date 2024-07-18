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

// A vocabulary that stores all words in memory. The vocabulary supports
// "holes", meaning that the indices of the contained words don't have to be
// contiguous (but ascending). All accesses are implemented using binary search.
class VocabularyInMemoryBinSearch
    : public VocabularyBinarySearchMixin<VocabularyInMemoryBinSearch> {
 public:
  using CharType = char;
  using StringView = std::basic_string_view<CharType>;
  using String = std::basic_string<CharType>;
  using Words = CompactVectorOfStrings<CharType>;
  using Indices = std::vector<uint64_t>;

 private:
  // The actual storage.
  Words words_;
  Indices indices_;

 public:
  // Construct an empty vocabulary
  VocabularyInMemoryBinSearch() = default;

  // Construct the vocabulary from `Words` and `Indices`
  explicit VocabularyInMemoryBinSearch(Words words, Indices indices)
      : words_{std::move(words)}, indices_{std::move(indices)} {}

  // Vocabularies are movable (but not copyable).
  VocabularyInMemoryBinSearch& operator=(
      VocabularyInMemoryBinSearch&&) noexcept = default;
  VocabularyInMemoryBinSearch(VocabularyInMemoryBinSearch&&) noexcept = default;

  // Const access for the indices.
  const Indices& indices() const { return indices_; }

  // Read the vocabulary from a file. The file must have been created using a
  // `WordWriter`.
  void open(const string& fileName);

  // Return the total number of words
  [[nodiscard]] size_t size() const {
    AD_CORRECTNESS_CHECK(indices_.size() == words_.size());
    return words_.size();
  }

  // Return the highest ID (= index) that occurs in this vocabulary.
  [[nodiscard]] uint64_t getHighestId() const {
    if (indices_.empty()) {
      return 0;
    }
    return indices_.back();
  }

  // Return the word with index `index`. If this index is not part of the
  // vocabulary, return `std::nullopt`.
  std::optional<std::string_view> operator[](uint64_t index) const;

  // Convert an iterator to a `WordAndIndex`. Required for the mixin.
  WordAndIndex iteratorToWordAndIndex(std::ranges::iterator_t<Words> it) const;

  // A helper type that can be used to directly write a vocabulary to disk
  // word-by-word, without having to materialize it in RAM first.
  struct WordWriter {
    typename Words::Writer writer_;
    using OffsetWriter = ad_utility::serialization::VectorIncrementalSerializer<
        uint64_t, ad_utility::serialization::FileWriteSerializer>;
    OffsetWriter offsetWriter_;
    std::optional<uint64_t> lastIndex_ = std::nullopt;
    // Construct a `WordWriter` that will write to the given `filename`.
    explicit WordWriter(const std::string& filename);
    // Add the given `word` with the given `idx`. The `idx` must be greater than
    // all previous indices.
    void operator()(std::string_view word, uint64_t idx);

    // Finish writing and dump all contents that still reside in buffers to
    // disk.
    void finish();
  };

  // Return a `WordWriter` that directly writes the words to the given
  // `filename`. The words are not materialized in RAM, but the vocabulary later
  // has to be explicitly initialized via `open(filename)`.
  WordWriter makeDiskWriter(const std::string& filename) const {
    return WordWriter{filename};
  }

  // Clear the vocabulary.
  void close();

  // Build the vocabulary from the given `words`. The indices are assigned
  // contiguously starting from 0.
  void build(const std::vector<std::string>& words,
             const std::string& filename);

  // Const access to the underlying words.
  auto begin() const { return words_.begin(); }
  auto end() const { return words_.end(); }
};
