//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_COMBINEDVOCABULARY_H
#define QLEVER_COMBINEDVOCABULARY_H

#include <concepts>

/// Define a `CombinedVocabulary` that consists of two Vocabularies (called the
/// "underlying" vocabularies). Each index that is used in the public interface
/// (operator[], lower_bound, upper_bound) is a "global index". The
/// `CombinedVocabulary` additionally has a type `IndexConverter` that computes,
/// whether a global index belongs to the first or the second vocabulary, and is
/// able to perform the transformations from global indices to local indices.
/// Private indices are the indices which the underlying vocabularies use
/// internally.

/// The `CombinedVocabulary` below needs a template parameter `IndexConverter`
/// that fulfills this concept to decide, whether a global index belongs to the
/// first or the second vocabulary, and to convert from global indices to
/// local indices.
template <typename CombinedVocabulary, typename T>
concept IndexConverterConcept = requires(const T& t, uint64_t i,
                                         const CombinedVocabulary& v) {
  // Return true, iff a word with index `i` belongs to the first vocabulary.
  { t.isInFirst(i, v) }
  ->std::same_as<bool>;
  // Transform a local index from the first vocabulary to a global index.
  { t.toGlobalFirst(i, v) }
  ->std::convertible_to<uint64_t>;
  // Transform a local index from the second vocabulary to a global index.
  { t.toGlobalSecond(i, v) }
  ->std::convertible_to<uint64_t>;
  // Transform a global index to a local index for the first vocabulary.
  // May only be called if `t.isInFirst(i, v)` is true.
  { t.toLocalFirst(i, v) }
  ->std::convertible_to<uint64_t>;
  // Transform a global index to a local index for the second vocabulary.
  // May only be called if `t.isInFirst(i, v)` is false.
  { t.toLocalSecond(i, v) }
  ->std::convertible_to<uint64_t>;
};

///
/// \tparam FirstVocabulary The first underlying vocabulary
/// \tparam SecondVocabulary The second underlying vocabulary
/// \tparam IndexConverter  must fulfill
/// `IndexConverterConcept<CombinedVocabulary<...>>` (checked in the
/// constructor)
template <typename FirstVocabulary, typename SecondVocabulary,
          typename IndexConverter>
class CombinedVocabulary {
 public:
  // TODO<joka92> Create a global `WordAndIndex` class that is used by all the
  // vocabularies and uses `std::optional<std::string>>` for _word`. A word and
  // its global index in the vocabulary.
  struct WordAndIndex {
    std::optional<std::string> _word;
    uint64_t _index;

    WordAndIndex() = default;
    WordAndIndex(std::optional<std::string> word, uint64_t index)
        : _word{std::move(word)}, _index{index} {}
    WordAndIndex(const std::string& word, uint64_t index)
        : _word{word}, _index{index} {}
    WordAndIndex(std::optional<std::string_view> word, uint64_t index)
        : _index{index} {
      if (word.has_value()) {
        _word.emplace(word.value());
      }
    }
    WordAndIndex(std::nullopt_t, uint64_t index) : _index{index} {}
    bool operator==(const WordAndIndex& result) const = default;
    [[nodiscard]] bool has_value() const { return _word.has_value(); }

    // Comparison is only done by the `_index`, since the `_word` is redundant
    // additional information.
    auto operator<=>(const WordAndIndex& rhs) const {
      return _index <=> rhs._index;
    }

    // This operator provides human-readable output for a `WordAndIndex`.
    friend std::ostream& operator<<(std::ostream& o, const WordAndIndex& w) {
      o << w._index << ", ";
      o << (w._word.value_or("nullopt"));
      return o;
    }
  };

 private:
  // The underlying vocabularies and the index converter
  FirstVocabulary _firstVocab;
  SecondVocabulary _secondVocab;
  IndexConverter _indexConverter;

 public:
  // Construct from pre-initialized vocabularies and `IndexConverter`.
  CombinedVocabulary(
      FirstVocabulary firstVocab, SecondVocabulary secondVocab,
      IndexConverter
          converter) requires IndexConverterConcept<CombinedVocabulary,
                                                    IndexConverter>
      : _firstVocab{std::move(firstVocab)},
        _secondVocab{std::move(secondVocab)},
        _indexConverter{converter} {}

  // Return the word with the global index `index`.
  auto operator[](uint64_t index) const {
    if (_indexConverter.isInFirst(index, *this)) {
      return _firstVocab[_indexConverter.toLocalFirst(index, *this)];
    } else {
      return _secondVocab[_indexConverter.toLocalSecond(index, *this)];
    }
  }

  /// The size of the single vocabs, and the the total size.
  [[nodiscard]] uint64_t sizeFirstVocab() const { return _firstVocab.size(); }
  [[nodiscard]] uint64_t sizeSecondVocab() const { return _secondVocab.size(); }
  [[nodiscard]] uint64_t size() const {
    return sizeFirstVocab() + sizeSecondVocab();
  }

  /// Return a `WordAndIndex` that points to the first entry that is equal or
  /// greater than `word` wrt the `comparator`.
  /// This requires that each of the underlying vocabularies is sorted wrt
  /// `comparator` and that for any two words x and y (each from either
  /// vocabulary), x < y wrt `comparator` if and only if
  /// global id(x) < global id(y).
  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator) const {
    auto resultA = fromA(_firstVocab.lower_bound(word, comparator));

    auto resultB = fromB(_secondVocab.lower_bound(word, comparator));
    WordAndIndex result;

    return std::min(resultA, resultB);
  }

  /// Return a `WordAndIndex` that points to the first entry that is
  /// greater than `word` wrt the `comparator`. The same requirements as for
  /// `lower_bound` have to hold.
  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound(const InternalStringType& word,
                           Comparator comparator) const {
    auto resultA = fromA(_firstVocab.upper_bound(word, comparator));

    auto resultB = fromB(_secondVocab.upper_bound(word, comparator));
    return std::min(resultA, resultB);
  }

 private:
  // TODO better name
  // Tranform a `WordAndIndex` from the `FirstVocabulary` to a global
  // `WordAndIndex` by transforming the index from local to global.
  WordAndIndex fromA(typename FirstVocabulary::WordAndIndex wi) const {
    auto index = wi._word.has_value()
                     ? _indexConverter.toGlobalFirst(wi._index, *this)
                     : getEndIndex();
    return WordAndIndex{std::move(wi._word), index};
  }

  // Tranform a `WordAndIndex` from the `SecondVocabulary` to a global
  // `WordAndIndex` by transforming the index from local to global.
  WordAndIndex fromB(typename SecondVocabulary::WordAndIndex wi) const {
    auto index = wi._word.has_value()
                     ? _indexConverter.toGlobalSecond(wi._index, *this)
                     : getEndIndex();
    return WordAndIndex{std::move(wi._word), index};
  }

  // Return a global index that is the largest global index occuring in either
  // of the underlying vocabularies plus 1. This index can be used as the "end"
  // index to report "not found".
  [[nodiscard]] uint64_t getEndIndex() const {
    uint64_t endA = _firstVocab.size() == 0
                        ? 0ul
                        : _indexConverter.toGlobalFirst(
                              _firstVocab.getHighestIndex(), *this) +
                              1;
    uint64_t endB = _secondVocab.size() == 0
                        ? 0ul
                        : _indexConverter.toGlobalSecond(
                              _secondVocab.getHighestIndex(), *this) +
                              1;

    return std::max(endA, endB);
  }
};

#endif  // QLEVER_COMBINEDVOCABULARY_H
