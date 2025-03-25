//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once
#include "index/vocabulary/VocabularyTypes.h"

/// Vocabulary with multi-level `UnicodeComparator` that allows comparison
/// according to different Levels. Groups of words that are adjacent on a
/// stricter level can be all equal on a weaker level. The
/// `UnderlyingVocabulary` has to be sorted according to the strictest level.
template <typename UnderlyingVocabulary, typename UnicodeComparator>
class UnicodeVocabulary {
 public:
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
  /// `words_` are sorted according to the comparator (exactly like in
  /// `std::lower_bound`, which is used internally).
  /// Type `T` can be a string-like type (`string, string_view`) or
  /// `UnicodeComparator::SortKey`
  template <typename T>
  WordAndIndex lower_bound(const T& word, SortLevel level) const {
    auto actualComparator = [this, level](const auto& a, const auto& b) {
      return _comparator(a, b, level);
    };
    return _underlyingVocabulary.lower_bound(word, actualComparator);
  }

  /// Return a `WordAndIndex` that points to the first entry that is greater
  /// than `word` wrt. to the `comparator`. Only works correctly if the `words_`
  /// are sorted according to the comparator (exactly like in
  /// `std::upper_bound`, which is used internally).
  /// Type `T` can be a string-like type (`string, string_view`) or
  /// `UnicodeComparator::SortKey`
  template <typename T>
  WordAndIndex upper_bound(const T& word, SortLevel level) const {
    auto actualComparator = [this, level](const auto& a, const auto& b) {
      return _comparator(a, b, level);
    };
    return _underlyingVocabulary.upper_bound(word, actualComparator);
  }

  /// Return the index range [lowest, highest) of words where a prefix of the
  /// word is equal to `prefix` on the `PRIMARY` level of the comparator.
  /// A value of `nullopt` in the entries means "the bound is higher than the
  /// largest word in the vocabulary".
  /// TODO<joka921> Also support other levels, but this requires intrusive
  /// hacking of ICU's SortKeys.
  [[nodiscard]] std::pair<std::optional<uint64_t>, std::optional<uint64_t>>
  prefix_range(std::string_view prefix) const {
    if (prefix.empty()) {
      return {std::nullopt, std::nullopt};
    }

    auto lb = lower_bound(prefix, SortLevel::PRIMARY);
    auto transformed = _comparator.transformToFirstPossibleBiggerValue(
        prefix, SortLevel::PRIMARY);

    auto ub = lower_bound(transformed, SortLevel::PRIMARY);

    auto toOptionalIndex = [](const WordAndIndex& wi) {
      return wi.isEnd() ? std::nullopt : std::optional{wi.index()};
    };

    return {toOptionalIndex(lb), toOptionalIndex(ub)};
  }

  /// Open the underlying vocabulary from a file. The file must have been
  /// written using the `UnderlyingVocabulary` class.
  void open(const std::string& filename) {
    _underlyingVocabulary.open(filename);
  }

  UnderlyingVocabulary& getUnderlyingVocabulary() {
    return _underlyingVocabulary;
  }
  const UnderlyingVocabulary& getUnderlyingVocabulary() const {
    return _underlyingVocabulary;
  }

  UnicodeComparator& getComparator() { return _comparator; }
  const UnicodeComparator& getComparator() const { return _comparator; }

  void close() { _underlyingVocabulary.close(); }

  void build(const std::vector<std::string>& v, const std::string& filename) {
    _underlyingVocabulary.build(v, filename);
  }
};
