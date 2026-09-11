//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "./util/ParsedQueryTestHelpers.h"
#include "util/GTestHelpers.h"
#include "util/ParseException.h"
#include "util/SourceLocation.h"

using ad_utility::testing::parseQuery;

// _____________________________________________________________________________
TEST(ParseException, coloredError) {
  auto exampleQuery = "SELECT A ?var WHERE";
  EXPECT_EQ((ExceptionMetadata{exampleQuery, 7, 7, 1, 7}).coloredError(),
            "SELECT \x1B[1m\x1B[4m\x1B[31mA\x1B[0m ?var WHERE");
  EXPECT_EQ((ExceptionMetadata{exampleQuery, 9, 12, 1, 9}).coloredError(),
            "SELECT A \x1B[1m\x1B[4m\x1B[31m?var\x1B[0m WHERE");
  // Start index is greater than stop index.
  EXPECT_ANY_THROW(
      (ExceptionMetadata{exampleQuery, 8, 6, 1, 3}).coloredError());
}

// _____________________________________________________________________________
TEST(ParseException, illegalConstructorArguments) {
  auto exampleQuery = "SELECT A ?var WHERE";
  // Start index is greater than stop index.
  EXPECT_ANY_THROW((ParseException{
      "illegal query", ExceptionMetadata{exampleQuery, 8, 6, 1, 3}}));
}

// _____________________________________________________________________________
void expectParseExceptionWithMetadata(
    const std::string& input, const std::optional<ExceptionMetadata>& metadata,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  try {
    parseQuery(input);
    FAIL();  // Should be unreachable.
  } catch (const ParseException& e) {
    // The constructor has to be bracketed because EXPECT_EQ is a macro.
    EXPECT_EQ(e.metadata(), metadata);
  }
}

// _____________________________________________________________________________
TEST(ParseException, MetadataGeneration) {
  // A is not a valid argument for select.
  expectParseExceptionWithMetadata(
      "SELECT A ?a WHERE { ?a ?b ?c }",
      {{"SELECT A ?a WHERE { ?a ?b ?c }", 7, 7, 1, 7}});
  // Error is the undefined Prefix "a".
  expectParseExceptionWithMetadata(
      "SELECT * WHERE { ?a a:b ?b }",
      {{"SELECT * WHERE { ?a a:b ?b }", 20, 22, 1, 20}});
  // "%" doesn't match any valid token. So in this case we will get an Error
  // from the Lexer.
  expectParseExceptionWithMetadata("SELECT * WHERE { % }",
                                   {{"SELECT * WHERE { % }", 17, 17, 1, 17}});
  // Error is the undefined Prefix "f".
  expectParseExceptionWithMetadata(
      "SELECT * WHERE {\n ?a ?b ?c . \n f:d ?d ?e\n}",
      {{"SELECT * WHERE {\n ?a ?b ?c . \n f:d ?d ?e\n}", 31, 33, 3, 1}});
}

// _____________________________________________________________________________
TEST(ParseException, logErrorAndHighlightedMetadata) {
  ENFORCE_LOG_LEVEL_OR_SKIP(ERROR);
  auto [cleanup, logStream] = setGlobalLoggingStreamToStringStream();

  // Call `logErrorAndHighlightedMetadata` on a copy of `initialErrorMsg`, and
  // check that the resulting message equals `expectedErrorMsg` and that the
  // (accumulating) log contains every string in `expectedLogParts`.
  auto expectLogged = [&logStream](
                          std::string initialErrorMsg,
                          const std::string& expectedErrorMsg,
                          const std::optional<ExceptionMetadata>& metadata,
                          const std::vector<std::string>& expectedLogParts) {
    logErrorAndHighlightedMetadata(initialErrorMsg, metadata);
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
