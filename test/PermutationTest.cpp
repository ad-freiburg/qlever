//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include "index/ConstantsIndexBuilding.h"
#include "index/Permutation.h"

// _____________________________________________________________________________
TEST(Permutation, fileNames) {
  using enum Permutation::Enum;
  EXPECT_THAT(Permutation::fileNames(PSO, "foo/index"),
              ::testing::ElementsAre("foo/index.index.pso",
                                     "foo/index.index.pso.meta"));
  EXPECT_THAT(Permutation::fileNames(OSP, "base"),
              ::testing::ElementsAre("base.index.osp", "base.index.osp.meta"));
  // For an internal permutation, the caller appends the infix to the base name.
  EXPECT_THAT(Permutation::fileNames(
                  POS, absl::StrCat("index", QLEVER_INTERNAL_INDEX_INFIX)),
              ::testing::ElementsAre("index.internal.index.pos",
                                     "index.internal.index.pos.meta"));
}
