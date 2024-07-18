// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#pragma once

#include <algorithm>
#include <cstdint>
#include <optional>
#include <utility>

#include "index/vocabulary/VocabularyTypes.h"
#include "util/Algorithm.h"

// A CRTP Mixin class that implements the binary search functions `lower_bound`,
// `upper_bound`, `lower_bound_iterator`, and `upper_bound_iterator` for
// different vocabulary classes. These classes require functions `begin()` and
// `end()` which return const random-access iterators to the (sorted) words, as
// well as a function `iteratorToWordAndIndex()` that takes an iterator (the
// return type of `begin()/end()`) and converts it to a `WordAndIndex`.
template <typename VocabularyImpl>
class VocabularyBinarySearchMixin {
 private:
  using Idx = std::optional<uint64_t>;

 public:
  // The following functions all have arguments `beginIdx` and `endIdx`.
  // These denote the subrange of the vocabulary that is to be searched.
  // If `beginIdx` is `nullopt`, then the search will start at the beginning,
  // if `endIdx`is `nullopt`, then the search will end at the end of the
  // vocabulary.

  // Return a `WordAndIndex` that points to the first entry that is equal to or
  // greater than `word` wrt. to the `comparator`. Only works correctly if the
  // `words_` are sorted according to the comparator (exactly like in
  // `std::lower_bound`, which is used internally).
  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator, Idx beginIdx = std::nullopt,
                           Idx endIdx = std::nullopt) const {
    auto [begin, end] = getIterators(beginIdx, endIdx);
    return impl().iteratorToWordAndIndex(
        std::ranges::lower_bound(begin, end, word, comparator));
  }

  // Return the first entry that is greater than `word`. The interface is the
  // same as in `lowerBound` above.
  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound(const InternalStringType& word,
                           Comparator comparator, Idx beginIdx = std::nullopt,
                           Idx endIdx = std::nullopt) const {
    auto [begin, end] = getIterators(beginIdx, endIdx);
    return impl().iteratorToWordAndIndex(
        std::ranges::upper_bound(begin, end, word, comparator));
  }

  // These functions are similar to `lower_bound` and `upper_bound` (see above),
  // but the `comparator` compares the `word` to an iterator, and not to a
  // dereferenced iterator.
  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound_iterator(const InternalStringType& word,
                                    Comparator comparator,
                                    Idx beginIdx = std::nullopt,
                                    Idx endIdx = std::nullopt) const {
    auto [begin, end] = getIterators(beginIdx, endIdx);
    return impl().iteratorToWordAndIndex(
        ad_utility::lower_bound_iterator(begin, end, word, comparator));
  }

  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound_iterator(const InternalStringType& word,
                                    Comparator comparator,
                                    Idx beginIdx = std::nullopt,
                                    Idx endIdx = std::nullopt) const {
    auto [begin, end] = getIterators(beginIdx, endIdx);
    return impl().iteratorToWordAndIndex(
        ad_utility::upper_bound_iterator(begin, end, word, comparator));
  }

 private:
  // Cast to the child class.
  auto& impl() { return static_cast<VocabularyImpl&>(*this); }
  const auto& impl() const { return static_cast<const VocabularyImpl&>(*this); }

  // Convert `beginIdx` and `endIdx` to the corresponding iterators.
  auto getIterators(Idx beginIdx, Idx endIdx) const {
    auto begin = impl().begin() + beginIdx.value_or(0);
    auto end =
        endIdx.has_value() ? impl().begin() + endIdx.value() : impl().end();
    return std::pair{begin, end};
  }
};
