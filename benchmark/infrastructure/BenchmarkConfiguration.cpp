// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include "../benchmark/infrastructure/BenchmarkConfiguration.h"

#include <regex>
#include <string>

#include "util/Exception.h"
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
  // I use regular expressions to parse the short hand. In order to easier
  // reuse parts of my patterns, I defined some contants here.

  // Boolean literal, integer literal, or string literal.
  const std::string valueLiterals{R"--(true|false|-?\d+|".*")--"};
  // How a list of `valueLiterals` looks like.
  const std::string listOfValueLiterals{R"--(\[(\s*()--" + valueLiterals +
                                        R"--()\s*,)*\s*()--" + valueLiterals +
                                        R"--()\s*\])--"};
  // What kind of names can the left side of the assigment
  // `variableName = variableContent;` have?
  const std::string variableName{R"--([\w]+)--"};
  // What kind of names can the right side of the assigment
  // `variableName = variableContent;` have?
  const std::string variableContent{valueLiterals + R"(|)" +
                                    listOfValueLiterals};
  // How does one assigment look like?
  // Note: I made the variableName and variableContent into their own groups
  // within regular expressions, because `regex` allows direct access to
  // sub-matches when it found a match. That should make iteration and parsing
  // easier.
  const std::string assigment{R"--(\s*()--" + variableName +
                              R"--()\s*=\s*()--" + variableContent +
                              R"--()\s*;)--"};

  // Use regular expressions to check, if the given string uses the correct
  // grammar/syntax.
  if (!std::regex_match(shortHandString,
                        std::regex{R"--(()--" + assigment + R"--()*)--"})) {
    throw ShortHandSyntaxException{shortHandString};
  }

  // The json object for returning. Will always be an 'object' in json terms.
  nlohmann::json jsonObject(nlohmann::json::value_t::object);

  // Create the regular expression of an assigment.
  std::regex assigmentRegex(assigment);
  // `std::sregex_iterator` have no `begin()`, or `end()`. Instead, the default
  // constructed `std::sregex_iterator` is the end-of-sequence iterator.
  const std::sregex_iterator endOfIterator;
  // Iterate over all assigments in the short hand string.
  for (auto iterator = std::sregex_iterator(
           shortHandString.begin(), shortHandString.end(), assigmentRegex);
       iterator != endOfIterator; iterator++) {
    std::smatch match{*iterator};

    // Get the variable name. It should be in the first sub-match.
    std::string assigmentVariableName{match.str(1)};
    // Get the not yet interpreted variable content. It should be in the second
    // sub-match.
    std::string assigmentVariableContentUninterpreted{match.str(2)};

    jsonObject[assigmentVariableName] =
        nlohmann::json::parse(assigmentVariableContentUninterpreted);
  }

  return jsonObject;
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
