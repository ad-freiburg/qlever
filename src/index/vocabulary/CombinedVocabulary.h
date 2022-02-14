//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_COMBINEDVOCABULARY_H
#define QLEVER_COMBINEDVOCABULARY_H

#include <concepts>

#include "../../util/Exception.h"
#include "./VocabularyTypes.h"

/// Define a `CombinedVocabulary` that consists of two Vocabularies (called the
/// "underlying" vocabularies). Each ID that is used in the public interface
/// (operator[], lower_bound, upper_bound) is a "global ID". The
/// `CombinedVocabulary` additionally has a type `IndexConverter` that computes,
/// whether a global ID belongs to the first or the second vocabulary, and is
/// able to perform the transformations from global indices to local indices.
/// Private indices are the indices which the underlying vocabularies use
/// internally.

/// The `CombinedVocabulary` below needs a template parameter `IndexConverter`
/// that fulfills this concept to decide, whether a global ID belongs to the
/// first or the second vocabulary, and to convert from global indices to
/// local indices.
template <typename CombinedVocabulary, typename T>
concept IndexConverterConcept = requires(const T& t, uint64_t i,
                                         const CombinedVocabulary& v) {
  // Return true, iff a word with ID `i` belongs to the first vocabulary.
  { t.isInFirst(i, v) }
  ->std::same_as<bool>;
  // Transform a local ID from the first vocabulary to a global ID.
  { t.localFirstToGlobal(i, v) }
  ->std::convertible_to<uint64_t>;
  // Transform a local ID from the second vocabulary to a global ID.
  { t.localSecondToGlobal(i, v) }
  ->std::convertible_to<uint64_t>;
  // Transform a global ID to a local ID for the first vocabulary.
  // May only be called if `t.isInFirst(i, v)` is true.
  { t.globalToLocalFirst(i, v) }
  ->std::convertible_to<uint64_t>;
  // Transform a global ID to a local ID for the second vocabulary.
  // May only be called if `t.isInFirst(i, v)` is false.
  { t.globalToLocalSecond(i, v) }
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
 private:
  // The underlying vocabularies and the ID converter
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

  // Return the word with the global ID `id`.
  auto operator[](uint64_t id) const {
    if (_indexConverter.isInFirst(id, *this)) {
      return _firstVocab[_indexConverter.globalToLocalFirst(id, *this)];
    } else {
      return _secondVocab[_indexConverter.globalToLocalSecond(id, *this)];
    }
  }

  /// The size of the single vocabs, and the the total size.
  [[nodiscard]] uint64_t sizeFirstVocab() const { return _firstVocab.size(); }
  [[nodiscard]] uint64_t sizeSecondVocab() const { return _secondVocab.size(); }
  [[nodiscard]] uint64_t size() const {
    return sizeFirstVocab() + sizeSecondVocab();
  }

  /// The highest ID (=index) that occurs in this vocabulary. May only be called
  /// if size() > 0
  auto getHighestId() const {
    AD_CHECK(size() > 0);
    return getEndIndex() - 1;
  }

  /// Return a `WordAndIndex` that points to the first entry that is equal or
  /// greater than `word` wrt the `comparator`. This requires that each of the
  /// underlying vocabularies is sorted wrt `comparator` and that for any two
  /// words x and y (each from either vocabulary), x < y wrt `comparator` if and
  /// only if global id(x) < global id(y).
  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator) const {
    auto resultA = fromA(_firstVocab.lower_bound(word, comparator));

    auto resultB = fromB(_secondVocab.lower_bound(word, comparator));
    WordAndIndex result;

    return std::min(resultA, resultB);
  }

  /// Return a `WordAndIndex` that points to the first entry that is greater
  /// than `word` wrt the `comparator`. The same requirements as for
  /// `lower_bound` have to hold.
  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound(const InternalStringType& word,
                           Comparator comparator) const {
    auto resultA = fromA(_firstVocab.upper_bound(word, comparator));

    auto resultB = fromB(_secondVocab.upper_bound(word, comparator));
    return std::min(resultA, resultB);
  }

 private:
  // Transform a `WordAndIndex` from the `FirstVocabulary` to a global
  // `WordAndIndex` by transforming the index from local to global.
  WordAndIndex fromA(WordAndIndex wi) const {
    wi._index = wi._word.has_value()
                    ? _indexConverter.localFirstToGlobal(wi._index, *this)
                    : getEndIndex();
    return wi;
  }

  // Transform a `WordAndIndex` from the `SecondVocabulary` to a global
  // `WordAndIndex` by transforming the index from local to global.
  WordAndIndex fromB(WordAndIndex wi) const {
    wi._index = wi._word.has_value()
                    ? _indexConverter.localSecondToGlobal(wi._index, *this)
                    : getEndIndex();
    return wi;
  }

  // Return a global ID (sometimes just an index) that is the largest global ID
  // occuring in either of the underlying vocabularies plus 1. This ID can be
  // used as the "end" ID to indicate "not found".
  [[nodiscard]] uint64_t getEndIndex() const {
    uint64_t endA = _firstVocab.size() == 0
                        ? 0ul
                        : _indexConverter.localFirstToGlobal(
                              _firstVocab.getHighestId(), *this) +
                              1;
    uint64_t endB = _secondVocab.size() == 0
                        ? 0ul
                        : _indexConverter.localSecondToGlobal(
                              _secondVocab.getHighestId(), *this) +
                              1;

    return std::max(endA, endB);
  }
};

#endif  // QLEVER_COMBINEDVOCABULARY_H
