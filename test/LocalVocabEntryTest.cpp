// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "./util/GTestHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "index/IndexImpl.h"
#include "index/LocalVocabEntry.h"

// _____________________________________________________________________________
TEST(LocalVocabEntry, compareThreeWayRequiresMatchingContexts) {
  if constexpr (!ad_utility::areExpensiveChecksEnabled) {
    GTEST_SKIP() << "This test only makes sense with expensive checks enabled.";
  }
  using ad_utility::testing::makeTestIndex;
  Index index1 = makeTestIndex(gtestCurrentTestName() + "-1", "<a> <b> <c> .");
  Index index2 = makeTestIndex(gtestCurrentTestName() + "-2", "<a> <b> <c> .");

  LocalVocabEntry entry1 = LocalVocabEntry::fromIriref("<x>", index1.getImpl());
  LocalVocabEntry entry2 = LocalVocabEntry::fromIriref("<x>", index2.getImpl());

  AD_EXPECT_THROW_WITH_MESSAGE(
      (void)(entry1 < entry2),
      ::testing::HasSubstr(
          "Contexts of LocalVocabEntries have to be identical"));
}
