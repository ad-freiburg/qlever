//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_COMPRESSEDVOCABULARY_H
#define QLEVER_COMPRESSEDVOCABULARY_H

#include <concepts>
/// A vocabulary that combines two vocabularies (vocabA, vocabB).
/// Additionally it consists of functions `IsIdInFirstVocabulary`

using IsInVocabA = bool (*)(uint64_t);

template <typename V, typename T>
concept IndexConverterConcept = requires(const T& t, uint64_t i, const V& v) {
  { t.isInFirst(i, v) }
  ->std::convertible_to<uint64_t>;
  { t.fromFirst(i, v) }
  ->std::convertible_to<uint64_t>;
  { t.fromSecond(i, v) }
  ->std::convertible_to<uint64_t>;
  { t.toFirst(i, v) }
  ->std::convertible_to<uint64_t>;
  { t.toSecond(i, v) }
  ->std::convertible_to<uint64_t>;
};

template <typename VocabA, typename VocabB, typename IndexConverter>
class CombinedVocabulary {
 public:
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
  VocabA _vocabA;
  VocabB _vocabB;
  IndexConverter _indexConverter;

 public:
  CombinedVocabulary(
      VocabA vocabA, VocabB vocabB,
      IndexConverter
          converter) requires IndexConverterConcept<CombinedVocabulary,
                                                    IndexConverter>
      : _vocabA{std::move(vocabA)},
        _vocabB{std::move(vocabB)},
        _indexConverter{converter} {}

  auto operator[](uint64_t id) const {
    if (_indexConverter.isInFirst(id, *this)) {
      return _vocabA[_indexConverter.toFirst(id, *this)];
    } else {
      auto index = _indexConverter.toSecond(id, *this);
      return _vocabB[index];
    }
  }

  [[nodiscard]] uint64_t sizeVocabA() const { return _vocabA.size(); }
  [[nodiscard]] uint64_t sizeVocabB() const { return _vocabB.size(); }
  [[nodiscard]] uint64_t size() const { return sizeVocabA() + sizeVocabB(); }

  // Todo comment helper function
  WordAndIndex fromA(typename VocabA::WordAndIndex wi) const {
    auto index = wi._word.has_value()
                     ? _indexConverter.fromFirst(wi._index, *this)
                     : getEndIndex();
    return WordAndIndex{std::move(wi._word), index};
  }
  WordAndIndex fromB(typename VocabB::WordAndIndex wi) const {
    auto index = wi._word.has_value()
                     ? _indexConverter.fromSecond(wi._index, *this)
                     : getEndIndex();
    return WordAndIndex{std::move(wi._word), index};
  }

  [[nodiscard]] uint64_t getEndIndex() const {
    uint64_t endA =
        _vocabA.size() == 0
            ? 0ul
            : _indexConverter.fromFirst(_vocabA.getHighestIndex(), *this) + 1;
    uint64_t endB =
        _vocabB.size() == 0
            ? 0ul
            : _indexConverter.fromSecond(_vocabB.getHighestIndex(), *this) + 1;

    return std::max(endA, endB);
  }

  /// Return a `WordAndIndex` that points to the first entry that is equal or
  /// greater than `word` wrt the `comparator`. Only works correctly if the
  /// `_words` are sorted according to the comparator (exactly like in
  /// `std::lower_bound`, which is used internally).
  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator) const {
    auto resultA = fromA(_vocabA.lower_bound(word, comparator));

    auto resultB = fromB(_vocabB.lower_bound(word, comparator));
    WordAndIndex result;

    return std::min(resultA, resultB);
  }

  /// Return a `WordAndIndex` that points to the first entry that is greater
  /// than `word` wrt. to the `comparator`. Only works correctly if the `_words`
  /// are sorted according to the comparator (exactly like in
  /// `std::upper_bound`, which is used internally).
  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound(const InternalStringType& word,
                           Comparator comparator) const {
    auto resultA = fromA(_vocabA.upper_bound(word, comparator));

    auto resultB = fromB(_vocabB.upper_bound(word, comparator));
    return std::min(resultA, resultB);
  }
};

#endif  // QLEVER_COMPRESSEDVOCABULARY_H
