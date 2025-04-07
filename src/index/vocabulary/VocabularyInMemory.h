// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#ifndef QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYINMEMORY_H
#define QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYINMEMORY_H

#include <string>
#include <string_view>

#include "global/Pattern.h"
#include "index/CompressedString.h"
#include "index/StringSortComparator.h"
#include "index/vocabulary/VocabularyBinarySearchMixin.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/Exception.h"

//! A vocabulary. Wraps a `CompactVectorOfStrings<char>`
//! and provides additional methods for reading and writing to/from file,
//! and retrieval via binary search.
class VocabularyInMemory
    : public VocabularyBinarySearchMixin<VocabularyInMemory> {
 public:
  using CharType = char;
  using StringView = std::basic_string_view<CharType>;
  using String = std::basic_string<CharType>;
  using Words = CompactVectorOfStrings<CharType>;

 private:
  // The actual storage.
  Words _words;

 public:
  /// Construct an empty vocabulary
  VocabularyInMemory() = default;

  /// Construct the vocabulary from `Words`
  explicit VocabularyInMemory(Words words) : _words{std::move(words)} {}

  // Vocabularies are movable
  VocabularyInMemory& operator=(VocabularyInMemory&&) noexcept = default;
  VocabularyInMemory(VocabularyInMemory&&) noexcept = default;

  /// Read the vocabulary from a file. The file must have been created by a call
  /// to `writeToFile` or using a `WordWriter`.
  void open(const string& fileName);

  /// Write the vocabulary to a file.
  void writeToFile(const string& fileName) const;

  /// Return the total number of words
  [[nodiscard]] size_t size() const { return _words.size(); }

  /// Return the `i-th` word. The behavior is undefined if `i >= size()`
  auto operator[](uint64_t i) const { return _words[i]; }

  // Conversion function that is used by the Mixin base class.
  WordAndIndex iteratorToWordAndIndex(auto it) const {
    if (it == _words.end()) {
      return WordAndIndex::end();
    }
    size_t idx = it - _words.begin();
    return {_words[idx], idx};
  }

  /// A helper type that can be used to directly write a vocabulary to disk
  /// word-by-word, without having to materialize it in RAM first. See the
  /// documentation of `CompactVectorOfStrings` for details.
  struct WordWriter {
    typename Words::Writer writer_;
    explicit WordWriter(const std::string& filename) : writer_{filename} {}

    // Write a word. The `isExternalDummy` is only there to have a consistent
    // interface with the `VocabularyInternalExternal`.
    void operator()(std::string_view str,
                    [[maybe_unused]] bool isExternalDummy = false) {
      writer_.push(str.data(), str.size());
    }

    void finish() { writer_.finish(); }
  };

  // Return a `WordWriter` that directly writes the words to the given
  // `filename`. The words are not materialized in RAM, but the vocabulary later
  // has to be explicitly initialized via `open(filename)`.
  static WordWriter makeDiskWriter(const std::string& filename) {
    return WordWriter{filename};
  }

  // Same as `makeDiskWriter` above, but the result is returned via
  // `unique_ptr`.
  static std::unique_ptr<WordWriter> makeDiskWriterPtr(
      const std::string& filename) {
    return std::make_unique<WordWriter>(filename);
  }

  /// Clear the vocabulary.
  void close() { _words.clear(); }

  // Const access to the underlying words.
  auto begin() const { return _words.begin(); }
  auto end() const { return _words.end(); }
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYINMEMORY_H
