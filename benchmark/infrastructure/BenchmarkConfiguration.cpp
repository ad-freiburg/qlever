// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include "../benchmark/infrastructure/BenchmarkConfiguration.h"

#include <ANTLRInputStream.h>
#include <CommonTokenStream.h>
#include <antlr4-runtime.h>

#include <regex>
#include <string>

#include "../benchmark/infrastructure/generated/BenchmarkConfigurationShorthandLexer.h"
#include "../benchmark/infrastructure/generated/BenchmarkConfigurationShorthandParser.h"
#include "BenchmarkConfigurationShorthandVisitor.h"
#include "util/Exception.h"
#include "util/antlr/ANTLRErrorHandling.h"
#include "util/json.h"

namespace ad_benchmark {

/*
@brief A custom exception for `parseShortHand`, for when the short hand
syntax wasn't followed.
*/
class ShortHandSyntaxException : public std::exception {
 private:
  // The error message.
  std::string message_;

 public:
  /*
  @param shortHandString The string, that was parsed.
  */
  explicit ShortHandSyntaxException(const std::string& shortHandString) {
    message_ =
        "The following string doesn't follow short hand string syntax "
        "and couldn't be parsed:\n" +
        shortHandString;
  }

  const char* what() const throw() override { return message_.c_str(); }
};

// ____________________________________________________________________________
nlohmann::json BenchmarkConfiguration::parseShortHand(
    const std::string& shortHandString) {
  // I use ANTLR expressions to parse the short hand.
  antlr4::ANTLRInputStream input(shortHandString);
  BenchmarkConfigurationShorthandLexer lexer(&input);
  antlr4::CommonTokenStream tokens(&lexer);
  BenchmarkConfigurationShorthandParser parser(&tokens);

  // The default in ANTLR is to log all errors to the console and to continue
  // the parsing. We need to turn parse errors into exceptions instead to
  // propagate them to the user.
  ThrowingErrorListener errorListener{};
  parser.removeErrorListeners();
  parser.addErrorListener(&errorListener);
  lexer.removeErrorListeners();
  lexer.addErrorListener(&errorListener);

  // Get the top node. That is, the node of the first grammar rule.
  BenchmarkConfigurationShorthandParser::ShortHandStringContext*
      shortHandStringContext{parser.shortHandString()};

  // Walk through the parser tree and build the json equivalent out of the short
  // hand.
  return ToJsonBenchmarkConfigurationShorthandVisitor{}.visitShortHandString(
      shortHandStringContext);
}

// ____________________________________________________________________________
void BenchmarkConfiguration::setShortHand(const std::string& shortHandString) {
  data_ = parseShortHand(shortHandString);
}

// ____________________________________________________________________________
void BenchmarkConfiguration::addShortHand(const std::string& shortHandString) {
  // This will cause an exception, if `data_` contains a json literal, or array.
  // But that is intended, because, trying to add dictionary like entries to
  // an array, sounds more like a problem on the user side of things.
  data_.update(parseShortHand(shortHandString));
}

// ____________________________________________________________________________
void BenchmarkConfiguration::setJsonString(const std::string& jsonString) {
  data_ = nlohmann::json::parse(jsonString);
  // It should only possible for `data_` to be an json object.
  if (!data_.is_object()) {
    throw ad_utility::Exception(
        "A BenchmarkConfiguration"
        " should only be set to a json object.");
  }
}

// ____________________________________________________________________________
void BenchmarkConfiguration::addJsonString(const std::string& jsonString) {
  nlohmann::json::const_reference parsedJsonString =
      nlohmann::json::parse(jsonString);

  // Only a `jsonString` representing a json object is allowed.
  if (!parsedJsonString.is_object()) {
    throw ad_utility::Exception(
        "The given json string must"
        " represent a valid json object.");
  }

  data_.update(parsedJsonString);
}

// JSON serialization.
void to_json(nlohmann::json& j, const BenchmarkConfiguration& configuration) {
  j = configuration.data_;
}
}  // namespace ad_benchmark
