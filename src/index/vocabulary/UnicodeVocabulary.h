//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_UNICODEVOCABULARY_H
#define QLEVER_UNICODEVOCABULARY_H

/// Vocabulary with multi-level `UnicodeComparator` that allows comparison
/// according to different Levels. Groups of words that are adjacent on a
/// stricter level can be all equal on a weaker level. The
/// `UnderlyingVocabulary` has to be sorted according to the strictest level.
template <typename UnderlyingVocabulary, typename UnicodeComparator>
class UnicodeVocabulary {
 public:
  using WordAndIndex = typename UnderlyingVocabulary::WordAndIndex;
  using SortLevel = typename UnicodeComparator::Level;

 private:
  UnicodeComparator _comparator;
  UnderlyingVocabulary _underlyingVocabulary;

 public:
  /// The additional `Args...` are used to construct the `UnderlyingVocabulary`
  template <typename... Args>
  explicit UnicodeVocabulary(UnicodeComparator comparator = UnicodeComparator{},
                             Args&&... args)
      : _comparator{std::move(comparator)},
        _underlyingVocabulary{std::forward<Args>(args)...} {}

  auto operator[](uint64_t id) const { return _underlyingVocabulary[id]; }

  [[nodiscard]] uint64_t size() const { return _underlyingVocabulary.size(); }

  /// Return a `WordAndIndex` that points to the first entry that is equal or
  /// greater than `word` wrt. to the `comparator`. Only works correctly if the
  /// `_words` are sorted according to the comparator (exactly like in
  /// `std::lower_bound`, which is used internally).
  /// T might be string_view or SortKey, TODO comment.
  template <typename T>
  WordAndIndex lower_bound(const T& word, SortLevel level) const {
    auto actualComparator = [this, level](const auto& a, const auto& b) {
      return _comparator(a, b, level);
    };
    return _underlyingVocabulary.lower_bound(word, actualComparator);
  }

  /// Return a `WordAndIndex` that points to the first entry that is greater
  /// than `word` wrt. to the `comparator`. Only works correctly if the `_words`
  /// are sorted according to the comparator (exactly like in
  /// `std::upper_bound`, which is used internally).
  /// T might be string_view or SortKey, TODO comment.
  template <typename T>
  WordAndIndex upper_bound(const T& word, SortLevel level) const {
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

  UnderlyingVocabulary& getUnderlyingVocabulary() {
    return _underlyingVocabulary;
  }
  const UnderlyingVocabulary& getUnderlyingVocabulary() const {
    return _underlyingVocabulary;
  }

  UnicodeComparator& getComparator() { return _comparator; }
  const UnicodeComparator& getComparator() const { return _comparator; }

  void clear() { _underlyingVocabulary.clear(); }

  void build(const std::vector<std::string>& v) {
    _underlyingVocabulary.build(v);
  }
};

#endif  // QLEVER_UNICODEVOCABULARY_H
