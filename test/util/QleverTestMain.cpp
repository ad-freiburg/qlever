// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <memory>

#include "RuntimeParametersResetCheck.h"

// The `main` function of the QLever unit tests, which is linked instead of
// `gmock_main` (see `linkTest` in `test/CMakeLists.txt`). Like `gmock_main`, it
// runs all tests. Checks and the setup and teardown that apply to all tests are
// implemented as `testing::TestEventListener`s and registered below.
int main(int argc, char** argv) {
  ::testing::InitGoogleMock(&argc, argv);
  // `Append` takes the ownership of the listeners (GTest deletes them). GTest
  // notifies the listeners of the end of a test in reverse order, so failures
  // that a listener generates there are attributed to that test.
  auto& listeners = ::testing::UnitTest::GetInstance()->listeners();
  listeners.Append(std::make_unique<RuntimeParametersResetCheck>().release());
  return RUN_ALL_TESTS();
}
