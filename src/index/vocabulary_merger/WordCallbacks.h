// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_WORDCALLBACKS_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_WORDCALLBACKS_H

#include <cstdint>
#include <string_view>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "util/TypeTraits.h"

namespace ad_utility::vocabulary_merger {
// Concept for a callback that can be called with a `string_view` and a `bool`.
// If the `bool` is true, then the word is to be stored in the external
// vocabulary else in the internal vocabulary.
template <typename T>
CPP_concept WordCallback =
    ad_utility::InvocableWithExactReturnType<T, uint64_t, std::string_view,
                                             bool>;
// Concept for a callable that compares two `string_view`s with respective
// `isExternal` flags.
template <typename T>
CPP_concept WordComparator =
    ranges::predicate<T, std::string_view, bool, std::string_view, bool>;
}  // namespace ad_utility::vocabulary_merger

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_WORDCALLBACKS_H
