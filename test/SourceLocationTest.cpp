// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "util/SourceLocation.h"

namespace {
auto getSourceLoc(ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  return loc;
}
}  // namespace

// Test that the `AD_CURRENT_SOURCE_LOC()` behaves as expected (it captures the
// location of the call-site).
TEST(SourceLocation, Current) {
  auto file = __builtin_FILE();
  auto function = __builtin_FUNCTION();
  auto line = __builtin_LINE();
  auto loc = getSourceLoc();

  EXPECT_EQ(loc.file_name(), file);
  EXPECT_EQ(loc.line(), line + 1);
  // The details of the function name are different
  // (`TestBody` vs `SourceLocation_Current_Test::TestBody`), so we cannot
  // use `EXPECT_STREQ` etc.
  EXPECT_THAT(loc.function_name(), testing::HasSubstr("TestBody"));
  EXPECT_THAT(function, testing::HasSubstr("TestBody"));

#ifdef QLEVER_CPP_17
  // In C++17 mode the `column()` is currently a dummy.
  EXPECT_EQ(loc.column(), 0);
#else
  EXPECT_NE(loc.column(), 0);
#endif
}
