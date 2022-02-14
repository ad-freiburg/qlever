//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_COMPRESSEDVOCABULARY_H
#define QLEVER_COMPRESSEDVOCABULARY_H

#include "./VocabularyTypes.h"

/// TODO<joka921> Currently the settings of the compressor are not directly
/// serialized but have to be manually stored and initialized.

/// TODO<joka921> Make "Compressor" and "Vocabulary" a concept.

/// A vocabulary in which compression is performed per word via the `Compressor`
template <typename UnderlyingVocabulary, typename Compressor>
class CompressedVocabulary {
 private:
  UnderlyingVocabulary _underlyingVocabulary;
  Compressor _compressor;

 public:
  CompressedVocabulary(Compressor compressor = Compressor())
      : _compressor{std::move(compressor)} {}

  auto operator[](uint64_t id) const {
    return _compressor.decompress(_underlyingVocabulary[id]);
  }

  [[nodiscard]] uint64_t size() const { return _underlyingVocabulary.size(); }
  [[nodiscard]] uint64_t getHighestId() const {
    return _underlyingVocabulary.getHighestId();
  }

  /// Return a `WordAndIndex` that points to the first entry that is equal or
  /// greater than `word` wrt the `comparator`. Only works correctly if the
  /// `_words` are sorted according to the comparator (exactly like in
  /// `std::lower_bound`, which is used internally).
  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator) const {
    auto actualComparator = [this, &comparator](const auto& a, const auto& b) {
      return comparator(_compressor.decompress(a), b);
    };
    auto underlyingResult =
        _underlyingVocabulary.lower_bound(word, actualComparator);
    WordAndIndex result;
    result._index = underlyingResult._index;
    if (underlyingResult._word.has_value()) {
      result._word = _compressor.decompress(underlyingResult._word.value());
    }
    return result;
  }

  /// Return a `WordAndIndex` that points to the first entry that is greater
  /// than `word` wrt. to the `comparator`. Only works correctly if the `_words`
  /// are sorted according to the comparator (exactly like in
  /// `std::upper_bound`, which is used internally).
  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound(const InternalStringType& word,
                           Comparator comparator) const {
    auto actualComparator = [this, &comparator](const auto& a, const auto& b) {
      return comparator(a, _compressor.decompress(b));
    };
    auto underlyingResult =
        _underlyingVocabulary.upper_bound(word, actualComparator);
    // TODO:: make this a private helper function.
    WordAndIndex result;
    result._index = underlyingResult._index;
    if (underlyingResult._word.has_value()) {
      result._word = _compressor.decompress(underlyingResult._word.value());
    }
    return result;
  }

  /// Open the underlying vocabulary from a file. The vocabulary must have been
  /// created by using a `DiskWriterFromUncompressedWords`. Note that the
  /// settings of the `compressor` are not stored in the file, but currently
  /// have to be manually stored a set via the constructor.
  void open(const std::string& filename) {
    _underlyingVocabulary.open(filename);
  }

  /// Allows the incremental writing of the words to disk. Uses `WordWriter` of
  /// the underlying vocabulary.
  class DiskWriterFromUncompressedWords {
   private:
    const Compressor& _compressor;
    typename UnderlyingVocabulary::WordWriter _underlyingWriter;

   public:
    /// Constructor
    explicit DiskWriterFromUncompressedWords(const Compressor& compressor,
                                             const std::string& filename)
        : _compressor{compressor}, _underlyingWriter{filename} {}

    /// Compress the `uncompressedWord` and write it to disk.
    void push(std::string_view uncompressedWord) {
      auto compressedWord = _compressor.compress(uncompressedWord);
      _underlyingWriter.push(compressedWord.data(), compressedWord.size());
    }

    /// After calls to `finish()` no more wrods can be pushed.
    /// `finish()` is implicitly also called by the destructor.
    void finish() { _underlyingWriter.finish(); }
  };

  DiskWriterFromUncompressedWords makeDiskWriter(const std::string& filename) {
    return DiskWriterFromUncompressedWords{_compressor, filename};
  }

  UnderlyingVocabulary& getUnderlyingVocabulary() {
    return _underlyingVocabulary;
  }
  const UnderlyingVocabulary& getUnderlyingVocabulary() const {
    return _underlyingVocabulary;
  }
  Compressor& getCompressor() { return _compressor; }
  const Compressor& getCompressor() const { return _compressor; }

  void close() { _underlyingVocabulary.close(); }

  void build(std::vector<std::string> words) {
    for (auto& word : words) {
      word = _compressor.compress(word);
    }
    _underlyingVocabulary.build(words);
  }
};

#endif  // QLEVER_COMPRESSEDVOCABULARY_H
