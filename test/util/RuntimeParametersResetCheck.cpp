// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "RuntimeParametersResetCheck.h"

#include "global/RuntimeParameters.h"
#include "util/Log.h"

// _____________________________________________________________________________
void RuntimeParametersResetCheck::OnTestProgramStart(
    const ::testing::UnitTest&) {
  initialParameters_ = globalRuntimeParameters.rlock()->toMap();
}

// _____________________________________________________________________________
void RuntimeParametersResetCheck::OnTestEnd(const ::testing::TestInfo&) {
  auto parameters = globalRuntimeParameters.wlock();
  for (const auto& [name, value] : parameters->toMap()) {
    const auto& initialValue = initialParameters_.at(name);
    if (value != initialValue) {
      ADD_FAILURE() << "The runtime parameter `" << name
                    << "` was not reset at the end of the test, its value is "
                    << "\"" << value << "\" instead of \"" << initialValue
                    << "\"";
      parameters->setFromString(name, initialValue);
    }
  }
  // `ScopedLogLevel` changes the runtime log level without the parameter.
  auto logLevel = parameters->logLevel_.get();
  if (ad_utility::getRuntimeLogLevel().value() != logLevel.value()) {
    ADD_FAILURE() << "The runtime log level was not reset at the end of the "
                     "test, it is "
                  << ad_utility::getRuntimeLogLevel().toString()
                  << " instead of " << logLevel.toString();
    ad_utility::setRuntimeLogLevel(logLevel);
  }
}
