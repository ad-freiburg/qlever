// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_CONCEPTS_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_CONCEPTS_H

#include <cstdint>
#include <string_view>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "util/Exception.h"
#include "util/TypeTraits.h"

namespace ad_utility::vocabulary_merger {
// Concept for a callback that can be called with a `string_view` and a `bool`.
// If the `bool` is true, then the word is to be stored in the external
// vocabulary else in the internal vocabulary.
template <typename T>
CPP_concept WordCallback =
    ad_utility::InvocableWithExactReturnType<T, uint64_t, std::string_view,
                                             bool>;
// Concept for a callable that compares two words (represented as
// `string_view`s) according to the order of the merged vocabulary.
template <typename T>
CPP_concept WordComparator =
    ::ranges::predicate<T, std::string_view, std::string_view>;

// Concept for a callable that compares two merged words given as (geo sort
// key, word) pairs, see `detail::QueueWord`. This is the order of the merged
// vocabulary when the index has a geo cell grid; without a grid, all keys are
// 0 and the order is that of the `WordComparator`.
template <typename T>
CPP_concept KeyedWordComparator =
    ::ranges::predicate<T, uint64_t, std::string_view, uint64_t,
                        std::string_view>;

// Check that the word `previous` comes before the word `next` in the order of
// the `comparator`, and throw otherwise. This is the check that the stages of
// the merger run on consecutive distinct words; it is only performed if the
// expensive checks are enabled (see `AD_EXPENSIVE_CHECK`).
CPP_template(typename W)(requires WordComparator<W>) void checkVocabularyOrder(
    [[maybe_unused]] const W& comparator,
    [[maybe_unused]] std::string_view previous,
    [[maybe_unused]] std::string_view next) {
  AD_EXPENSIVE_CHECK(comparator(previous, next),
                     "Total vocabulary order violated for ", previous, " and ",
                     next);
}

// The same check for two merged words given as (geo sort key, word) pairs,
// see `KeyedWordComparator`. The two overloads differ in their arity, so this
// one can be a plain template.
template <typename W>
void checkVocabularyOrder([[maybe_unused]] const W& comparator,
                          [[maybe_unused]] uint64_t previousKey,
                          [[maybe_unused]] std::string_view previous,
                          [[maybe_unused]] uint64_t nextKey,
                          [[maybe_unused]] std::string_view next) {
  static_assert(KeyedWordComparator<W>);
  AD_EXPENSIVE_CHECK(comparator(previousKey, previous, nextKey, next),
                     "Total vocabulary order violated for ", previous, " and ",
                     next);
}
}  // namespace ad_utility::vocabulary_merger

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_CONCEPTS_H
