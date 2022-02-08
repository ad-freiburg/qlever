//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_COMPRESSEDVOCABULARY_H
#define QLEVER_COMPRESSEDVOCABULARY_H

#include <concepts>

/// Define a `CombinedVocabulary` that consists of two Vocabularies (called the
/// "underlying" vocabularies). Each index that is used in the public interface
/// (operator[], lower_bound, upper_bound) is a "public index". The
/// `CombinedVocabulary` additionally has a type `IndexConverter` that computes,
/// whether a public index belongs to the first or the second vocabulary, and is
/// able to perform the transformations from public indices to private indices.
/// Private indices are the indices which the underlying vocabularies use
/// internally.

/// The `CombinedVocabulary` below needs a template parameter `IndexConverter`
/// that fulfills this concept to decide, whether a public index belongs to the
/// first or the second vocabulary, and to convert from public indices to
/// private indices.
template <typename CombinedVocabulary, typename T>
concept IndexConverterConcept = requires(const T& t, uint64_t i,
                                         const CombinedVocabulary& v) {
  // Return true, iff a word with index `i` belongs to the first vocabulary.
  { t.isInFirst(i, v) }
  ->std::same_as<bool>;
  // Transform a private index from the first vocabulary to a public index.
  { t.fromFirst(i, v) }
  ->std::convertible_to<uint64_t>;
  // Transform a private index from the second vocabulary to a public index.
  { t.fromSecond(i, v) }
  ->std::convertible_to<uint64_t>;
  // Transform a public index to a private index for the first vocabulary.
  // May only be called if `t.isInFirst(i, v)` is true.
  { t.toFirst(i, v) }
  ->std::convertible_to<uint64_t>;
  // Transform a public index to a private index for the second vocabulary.
  // May only be called if `t.isInFirst(i, v)` is false.
  { t.toSecond(i, v) }
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
  // TODO<joka921> Make it always return a string, and make it a common type for
  // all the vocabularies.
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

    // This operator provides human-readable output.
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

  // Return the word with the public index `index`
  auto operator[](uint64_t index) const {
    if (_indexConverter.isInFirst(index, *this)) {
      return _firstVocab[_indexConverter.toFirst(index, *this)];
    } else {
      return _secondVocab[_indexConverter.toSecond(index, *this)];
    }
  }

  /// The size of the single vocabs, and the the total size.
  [[nodiscard]] uint64_t sizeFirstVocab() const { return _firstVocab.size(); }
  [[nodiscard]] uint64_t sizeSecondVocab() const { return _secondVocab.size(); }
  [[nodiscard]] uint64_t size() const {
    return sizeFirstVocab() + sizeSecondVocab();
  }

  /// Return a `WordAndIndex` that points to the first entry that is equal or
  /// greater than `word` wrt the `comparator`. Only works correctly if the
  /// two underlying vocabularies are individually sorted wrt `comparator`
  /// and if the public indices off all words (in both vocabularies) are also
  /// numerically ordered wrt. `comparator`.
  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator) const {
    auto resultA = fromA(_firstVocab.lower_bound(word, comparator));

    auto resultB = fromB(_secondVocab.lower_bound(word, comparator));
    WordAndIndex result;

    return std::min(resultA, resultB);
  }

  /// Return a `WordAndIndex` that points to the first entry that is
  /// greater than `word` wrt the `comparator`. Only works correctly if the
  /// two underlying vocabularies are individually sorted wrt `comparator`
  /// and if the public indices off all words (in both vocabularies) are also
  /// numerically ordered wrt. `comparator`.
  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound(const InternalStringType& word,
                           Comparator comparator) const {
    auto resultA = fromA(_firstVocab.upper_bound(word, comparator));

    auto resultB = fromB(_secondVocab.upper_bound(word, comparator));
    return std::min(resultA, resultB);
  }

 private:
  // TODO better name
  // Tranform a `WordAndIndex` from the `FirstVocabulary` to a public
  // `WordAndIndex` by transforming the index from private to public.
  WordAndIndex fromA(typename FirstVocabulary::WordAndIndex wi) const {
    auto index = wi._word.has_value()
                     ? _indexConverter.fromFirst(wi._index, *this)
                     : getEndIndex();
    return WordAndIndex{std::move(wi._word), index};
  }

  // Tranform a `WordAndIndex` from the `SecondVocabulary` to a public
  // `WordAndIndex` by transforming the index from private to public.
  WordAndIndex fromB(typename SecondVocabulary::WordAndIndex wi) const {
    auto index = wi._word.has_value()
                     ? _indexConverter.fromSecond(wi._index, *this)
                     : getEndIndex();
    return WordAndIndex{std::move(wi._word), index};
  }

  // Return a public index that is the largest public index occuring in either
  // of the underlying vocabularies plus 1. This index can be used as the `end`
  // index to report "not found".
  [[nodiscard]] uint64_t getEndIndex() const {
    uint64_t endA =
        _firstVocab.size() == 0
            ? 0ul
            : _indexConverter.fromFirst(_firstVocab.getHighestIndex(), *this) +
                  1;
    uint64_t endB = _secondVocab.size() == 0
                        ? 0ul
                        : _indexConverter.fromSecond(
                              _secondVocab.getHighestIndex(), *this) +
                              1;

    return std::max(endA, endB);
  }
};

#endif  // QLEVER_COMPRESSEDVOCABULARY_H
