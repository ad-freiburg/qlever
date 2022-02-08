//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_COMPRESSEDVOCABULARY_H
#define QLEVER_COMPRESSEDVOCABULARY_H

/// A vocabulary that combines two vocabularies (vocabA, vocabB).
/// Additionally it consists of functions `IsIdInFirstVocabulary`

using IsInVocabA = bool(*)(uint64_t);

struct IndexConverter {
  using Converter = uint64_t (*)(uint64_t);
  Converter to;
  Converter from;
};

template <typename VocabA, typename VocabB>
class CombinedVocabulary {
 public:
  struct WordAndIndex {
    std::optional<std::string> _word;
    uint64_t _index;
    bool operator==(const WordAndIndex& result) const = default;
    bool has_value() const {return _word.has_value();}

    // Comparison is only done by the `_index`, since the `_word` is redundant
    // additional information.
    auto operator<=>(const WordAndIndex& rhs) const {
      return _index <=> rhs._index;
    }
  };

 private:
  VocabA _vocabA;
  VocabB _vocabB;
  IsInVocabA _isInVocabA;
  IndexConverter _converterA;
  IndexConverter _converterB;

 public:
  CombinedVocabulary(VocabA vocabA, VocabB vocabB, IsInVocabA isInVocabA, IndexConverter converterA, IndexConverter converterB)
      : _vocabA{std::move(vocabA)}, _vocabB{std::move(vocabB)}, _isInVocabA{isInVocabA}, _converterA{converterA}, _converterB{converterB} {}

  auto operator[](uint64_t id) const {
    if (_isInVocabA(id)) {
      return _vocabA[_converterA.to(id)];
    } else {
      return _vocabB[_converterB.to(id)];
    }
  }

  [[nodiscard]] uint64_t sizeVocabA() const { return _vocabA.size() ; }
  [[nodiscard]] uint64_t sizeVocabB() const { return _vocabB.size() ; }
  [[nodiscard]] uint64_t size() const { return sizeVocabA() + sizeVocabB(); }


  // Todo comment helper function
  WordAndIndex fromA(typename VocabA::WordAndIndex wi) {
    auto index = wi._word.has_value() ? _converterA.from(wi._index) : getEndIndex();
    return {std::move(wi._word), index};
  }
  WordAndIndex fromB(typename VocabB::WordAndIndex wi) {
    auto index = wi._word.has_value() ? _converterB.from(wi._index) : getEndIndex();
    return {std::move(wi._word), index};
  }

  uint64_t getEndIndex() const {
    uint64_t endA = _vocabA.size() == 0 ? 0ul : _converterA.from(_vocabA.getHighestIndex()) + 1;
    uint64_t endB = _vocabB.size() == 0 ? 0ul : _converterB.from(_vocabB.getHighestIndex()) + 1;

    return std::max(endA, endB);
  }

  /// Return a `WordAndIndex` that points to the first entry that is equal or
  /// greater than `word` wrt the `comparator`. Only works correctly if the
  /// `_words` are sorted according to the comparator (exactly like in
  /// `std::lower_bound`, which is used internally).
  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator) const {
    auto resultA =
        fromA(_vocabA.lower_bound(word, comparator));

    auto resultB =
        fromB(_vocabB.lower_bound(word, comparator));
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
    auto resultA =
        fromA(_vocabA.upper_bound(word, comparator));

    auto resultB =
        fromB(_vocabB.upper_bound(word, comparator));
    return std::min(resultA, resultB);
  }
};

#endif  // QLEVER_COMPRESSEDVOCABULARY_H
