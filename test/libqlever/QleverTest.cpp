// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "libqlever/Qlever.h"

using namespace qlever;
using namespace testing;

// _____________________________________________________________________________
TEST(IndexBuilderConfig, validate) {
  IndexBuilderConfig c;
  EXPECT_NO_THROW(c.validate());

  c.kScoringParam_ = -3;
  AD_EXPECT_THROW_WITH_MESSAGE(c.validate(), HasSubstr("must be >= 0"));

  c = IndexBuilderConfig{};
  c.bScoringParam_ = -3;
  AD_EXPECT_THROW_WITH_MESSAGE(c.validate(), HasSubstr("must be between"));
  c.bScoringParam_ = 1.1;
  AD_EXPECT_THROW_WITH_MESSAGE(c.validate(), HasSubstr("must be between"));

  c = IndexBuilderConfig{};
  c.wordsfile_ = "blibb";
  AD_EXPECT_THROW_WITH_MESSAGE(c.validate(),
                               HasSubstr("Only specified wordsfile"));
  c.docsfile_ = "blabb";
  EXPECT_NO_THROW(c.validate());
  c.wordsfile_ = "";
  AD_EXPECT_THROW_WITH_MESSAGE(c.validate(),
                               HasSubstr("Only specified docsfile"));
}

// TODO<joka921> Add some initial tests for the building and querying of
// indices.
