//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_COMPRESSEDVOCABULARY_H
#define QLEVER_COMPRESSEDVOCABULARY_H

/// TODO<joka921> Currently the settings of the compressor are not directly
/// serialized but have to be manually stored and initialized.
template <typename UnderlyingVocabulary, typename Compressor>
class UnicodeVocabulary {
 public:
  struct SearchResult {
    uint64_t _id;
    std::optional<std::string> _word;
    bool operator==(const SearchResult& result) const = default;
  };

 private:
  UnderlyingVocabulary _underlyingVocabulary;
  Compressor _compressor;

 public:
  UnicodeVocabulary(Compressor compressor = Compressor())
      : _compressor{std::move(compressor)} {}

  auto operator[](uint64_t id) const {
    return _compressor.decompress(_underlyingVocabulary[id]);
  }

  uint64_t size() const { return _underlyingVocabulary.size(); }

  /// Return a `SearchResult` that points to the first entry that is equal or
  /// greater than `word` wrt. to the `comparator`. Only works correctly if the
  /// `_words` are sorted according to the comparator (exactly like in
  /// `std::lower_bound`, which is used internally).
  template <typename InternalStringType, typename Comparator>
  SearchResult lower_bound(const InternalStringType& word,
                           Comparator comparator) const {
    auto actualComparator = [this, &comparator](const auto& a, const auto& b) {
      return comparator(_compressor.decompress(a), b);
    };
    auto underlyingResult =
        _underlyingVocabulary.lower_bound(word, actualComparator);
    SearchResult result;
    result._id = underlyingResult._id;
    if (underlyingResult._word.has_value()) {
      result._word = _compressor.decompress(underlyingResult._word.value());
    }
    return result;
  }

  /// Return a `SearchResult` that points to the first entry that is greater
  /// than `word` wrt. to the `comparator`. Only works correctly if the `_words`
  /// are sorted according to the comparator (exactly like in
  /// `std::upper_bound`, which is used internally).
  template <typename InternalStringType, typename Comparator>
  SearchResult upper_bound(const InternalStringType& word,
                           Comparator comparator) const {
    auto actualComparator = [this, &comparator](const auto& a, const auto& b) {
      return comparator(a, _compressor.decompress(b));
    };
    auto underlyingResult =
        _underlyingVocabulary.upper_bound(word, actualComparator);
    // TODO:: make this a private helper function.
    SearchResult result;
    result._id = underlyingResult._id;
    if (underlyingResult._word.has_value()) {
      result._word = _compressor.decompress(underlyingResult._word.value());
    }
    return result;
  }

  /// Read the underlying vocabulary from a file. The vocabulary must have been
  /// created by using a `DiskWriterFromUncompressedWords`. Note that the
  /// settings of the `compressor` are not stored in the file, but currently
  /// have to be manually stored a set via the constructor.
  void readFromFile(const std::string& filename) {
    _underlyingVocabulary.readFromFile(filename);
  }

  // Allows the incremental writing of a `CompactVectorOfStrings` directly to a
  // file.
  struct DiskWriterFromUncompressedWords {
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

  DiskWriterFromUncompressedWords makeDiskWriter(const string& filename) {
    return DiskWriterFromUncompressedWords{_compressor, filename};
  }
};

#endif  // QLEVER_COMPRESSEDVOCABULARY_H
