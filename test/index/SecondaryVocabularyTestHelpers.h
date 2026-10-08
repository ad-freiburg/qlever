// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_INDEX_SECONDARYVOCABULARYTESTHELPERS_H
#define QLEVER_TEST_INDEX_SECONDARYVOCABULARYTESTHELPERS_H

#include <gmock/gmock.h>

#include <optional>
#include <string>
#include <vector>

#include "../util/GTestHelpers.h"
#include "index/vocabulary/SecondaryVocabulary.h"

namespace secondaryVocabTestHelpers {

// Return a matcher for a `SecondaryVocabulary` that consists of
// `numSegments` segments and contains exactly the `wordsInIdOrder`, where the
// word at position `i` has the `SecondaryVocabIndex` `i`.
inline ::testing::Matcher<const SecondaryVocabulary&> secondaryVocabIs(
    size_t numSegments, const std::vector<std::string>& wordsInIdOrder) {
  using namespace ::testing;
  std::vector<Matcher<const SecondaryVocabulary&>> matchers{
      AD_PROPERTY(SecondaryVocabulary, numSegments, Eq(numSegments)),
      AD_PROPERTY(SecondaryVocabulary, numWords, Eq(wordsInIdOrder.size()))};
  for (size_t i = 0; i < wordsInIdOrder.size(); ++i) {
    const auto& word = wordsInIdOrder[i];
    matchers.push_back(ResultOf(
        "getId(" + word + ")",
        [word](const SecondaryVocabulary& vocab) { return vocab.getId(word); },
        Eq(std::optional{SecondaryVocabIndex::make(i)})));
  }
  return AllOfArray(matchers);
}

}  // namespace secondaryVocabTestHelpers

#endif  // QLEVER_TEST_INDEX_SECONDARYVOCABULARYTESTHELPERS_H
