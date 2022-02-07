//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_COMPRESSEDVOCABULARY_H
#define QLEVER_COMPRESSEDVOCABULARY_H

template <typename UnderlyingVocabulary, typename UnicodeComparator>
class UnicodeVocabulary {
 public:
  using SearchResult = typename UnderlyingVocabulary::SearchResult;
  using SortLevel = typename UnicodeComparator::Level;

 private:
  UnderlyingVocabulary _underlyingVocabulary;
  UnicodeComparator _comparator;

 public:
  explicit UnicodeVocabulary(UnicodeComparator comparator = UnicodeComparator())
      : _comparator{std::move(comparator)} {}

  auto operator[](uint64_t id) const { return _underlyingVocabulary[id]; }

  [[nodiscard]] uint64_t size() const { return _underlyingVocabulary.size(); }

  /// Return a `SearchResult` that points to the first entry that is equal or
  /// greater than `word` wrt. to the `comparator`. Only works correctly if the
  /// `_words` are sorted according to the comparator (exactly like in
  /// `std::lower_bound`, which is used internally).
  SearchResult lower_bound(std::string_view word, SortLevel level) const {
    auto actualComparator = [this, level](const auto& a, const auto& b) {
      return _comparator(a, b, level);
    };
    return _underlyingVocabulary.lower_bound(word, actualComparator);
  }

  /// Return a `SearchResult` that points to the first entry that is greater
  /// than `word` wrt. to the `comparator`. Only works correctly if the `_words`
  /// are sorted according to the comparator (exactly like in
  /// `std::upper_bound`, which is used internally).
  SearchResult upper_bound(std::string_view word, SortLevel level) const {
    auto actualComparator = [this, level](const auto& a, const auto& b) {
      return _comparator(a, b, level);
    };
    return _underlyingVocabulary.upper_bound(word, actualComparator);
  }

  /// Read the underlying vocabulary from a file. The file must have been
  /// written using the `UnderlyingVocabulary` class.
  void readFromFile(const std::string& filename) {
    _underlyingVocabulary.readFromFile(filename);
  }
};

#endif  // QLEVER_COMPRESSEDVOCABULARY_H
