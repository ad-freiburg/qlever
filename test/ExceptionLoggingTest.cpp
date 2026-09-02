// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tomas Damek <tomas.damek@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <vector>

#include "util/ExceptionLogging.h"
#include "util/GTestHelpers.h"
#include "util/ParseException.h"

using namespace ad_utility::exceptionLogging;

// _____________________________________________________________________________
TEST(ExceptionLogging, logErrorWithHighlighting) {
  ENFORCE_LOG_LEVEL_OR_SKIP(ERROR);
  auto [cleanup, logStream] = setGlobalLoggingStreamToStringStream();

  // Call `logErrorWithHighlighting` on a copy of `initialErrorMsg`, and check
  // that the resulting message equals `expectedErrorMsg` and that the
  // (accumulating) log contains every string in `expectedLogParts`.
  auto expectLogged = [&logStream](
                          std::string initialErrorMsg,
                          const std::string& expectedErrorMsg,
                          const std::optional<ExceptionMetadata>& metadata,
                          const std::vector<std::string>& expectedLogParts) {
    logErrorWithHighlighting(initialErrorMsg, metadata);
    EXPECT_EQ(initialErrorMsg, expectedErrorMsg);
    for (const auto& substring : expectedLogParts) {
      EXPECT_THAT(logStream.str(), ::testing::HasSubstr(substring));
    }
  };

  // Without metadata, only `errorMsg` is logged, and it is left unchanged.
  expectLogged("something went wrong", "something went wrong", std::nullopt,
               {"something went wrong"});

  // Highlighting succeeds, so the query is logged with color codes, and
  // `errorMsg` (later sent to the client) is left unchanged.
  ExceptionMetadata validMetadata{"SELECT A ?var WHERE", 7, 7, 1, 7};
  expectLogged("parse error", "parse error", validMetadata,
               {validMetadata.coloredError()});

  // A truncated multi-byte UTF-8 character at the end of `query_` (a lead
  // byte with no continuation byte) makes `coloredError()` throw, the same
  // fallback path a real QLever/ANTLR Unicode-handling mismatch would take.
  // The fallback logs the raw query and appends the failure reason to
  // `errorMsg`.
  ExceptionMetadata malformedUtf8Metadata{"SELECT A ?var WHERE \xC3", 7, 7, 1,
                                          7};
  expectLogged(
      "parse error",
      "parse error Highlighting an error for the command line log failed: "
      "Illegal UTF sequence in ad_utility::getUTF8Prefix",
      malformedUtf8Metadata,
      {"Failed to highlight error in operation.",
       "Illegal UTF sequence in ad_utility::getUTF8Prefix",
       malformedUtf8Metadata.query_});
}
