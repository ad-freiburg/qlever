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

template <typename VocabularyImpl>
class VocabularyBinarySearchMixin {
  using Idx = std::optional<uint64_t>;
  auto& impl() { return static_cast<VocabularyImpl&>(*this); }
  const auto& impl() const { return static_cast<const VocabularyImpl&>(*this); }

  auto getIterators(Idx beginIdx, Idx endIdx) const {
    auto begin = impl().begin() + beginIdx.value_or(0);
    auto end =
        endIdx.has_value() ? impl().begin() + endIdx.value() : impl().end();
    return std::pair{begin, end};
  }

 public:
  /// Return a `WordAndIndex` that points to the first entry that is equal or
  /// greater than `word` wrt. to the `comparator`. Only works correctly if the
  /// `_words` are sorted according to the comparator (exactly like in
  /// `std::lower_bound`, which is used internally).
  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator, Idx beginIdx = std::nullopt,
                           Idx endIdx = std::nullopt) const {
    auto [begin, end] = getIterators(beginIdx, endIdx);
    return impl().boundImpl(
        std::ranges::lower_bound(begin, end, word, comparator));
  }

  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound(const InternalStringType& word,
                           Comparator comparator, Idx beginIdx = std::nullopt,
                           Idx endIdx = std::nullopt) const {
    auto [begin, end] = getIterators(beginIdx, endIdx);
    return impl().boundImpl(
        std::ranges::upper_bound(begin, end, word, comparator));
  }

  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound_iterator(const InternalStringType& word,
                                    Comparator comparator,
                                    Idx beginIdx = std::nullopt,
                                    Idx endIdx = std::nullopt) const {
    auto [begin, end] = getIterators(beginIdx, endIdx);
    return impl().boundImpl(
        ad_utility::lower_bound_iterator(begin, end, word, comparator));
  }

  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound_iterator(const InternalStringType& word,
                                    Comparator comparator,
                                    Idx beginIdx = std::nullopt,
                                    Idx endIdx = std::nullopt) const {
    auto [begin, end] = getIterators(beginIdx, endIdx);
    return impl().boundImpl(
        ad_utility::upper_bound_iterator(begin, end, word, comparator));
  }
};
