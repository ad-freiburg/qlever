// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_RUNTIMEPARAMETERSRESETCHECK_H
#define QLEVER_TEST_UTIL_RUNTIMEPARAMETERSRESETCHECK_H

#include <gtest/gtest.h>

#include <string>

#include "util/HashMap.h"

// A listener that checks after each test that all runtime parameters and the
// runtime log level have the values that they had when the test program
// started. If not, the test fails and the values are reset, so that only the
// test that changed them fails, and not the tests that run after it.
class RuntimeParametersResetCheck : public ::testing::EmptyTestEventListener {
  ad_utility::HashMap<std::string, std::string> initialParameters_;

  void OnTestProgramStart(const ::testing::UnitTest&) override;
  void OnTestEnd(const ::testing::TestInfo&) override;
};

#endif  // QLEVER_TEST_UTIL_RUNTIMEPARAMETERSRESETCHECK_H
