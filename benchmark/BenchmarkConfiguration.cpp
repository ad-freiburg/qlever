// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include <regex>
#include <string>

#include "util/json.h"
#include "../benchmark/BenchmarkConfiguration.h"
#include "util/Exception.h"

// ____________________________________________________________________________
void BenchmarkConfiguration::parseJsonString(const std::string& jsonString){
  data_ = nlohmann::json::parse(jsonString);
}

// ____________________________________________________________________________
void BenchmarkConfiguration::parseShortHand(const std::string& shortHandString){
  // I use regular expressions to parse the short hand. In order to easier
  // reuse parts of my patterns, I defined some contants here.

  // Boolean literal, or integer literal.
  const std::string valueLiterals{R"(true|false|-?\d+)"};
  // How a list of `valueLiterals` looks like.
  const std::string listOfValueLiterals{R"(\{\s*()" + valueLiterals + R"()\s*,)*\s*()" + valueLiterals + R"()\s*\})"};
  // What kind of names can the left side of the assigment
  // `variableName = variableContent;` have?
  const std::string variableName{R"([\w]+)"};
  // What kind of names can the right side of the assigment
  // `variableName = variableContent;` have?
  const std::string variableContent{valueLiterals + R"(|)" + listOfValueLiterals};
  // How does one assigment look like?
  // Note: I made the variableName and variableContent into their own groups
  // within regular expressions, because `regex` allows direct access to
  // sub-matches when it found a match. That should make iteration and parsing
  // easier.
  const std::string assigment{R"(\s*()" + variableName + R"()\s*=\s*()" + variableContent + R"()\s*;)"};
  
  // Use regular expressions to check, if the given string uses the correct
  // grammar/syntax.
  AD_CHECK(std::regex_match(shortHandString, std::regex{R"(()" + assigment + R"()*)"}));

  // Create the regular expression of an assigment.
  std::regex assigmentRegex(assigment);
  // `std::sregex_iterator` have no `begin()`, or `end()`. Instead, the default
  // constructed `std::sregex_iterator` is the end-of-sequence iterator.
  const std::sregex_iterator endOfIterator;
  // Iterate over all assigments in the short hand string.
  for (auto iterator =
    std::sregex_iterator(shortHandString.begin(), shortHandString.end(),
    assigmentRegex); iterator != endOfIterator; iterator++){
    std::smatch match{*iterator};

    // Get the variable name. It should be in the first sub-match.
    std::string assigmentVariableName{match.str(1)};
    // Get the not yet interpreted variable content. It should be in the second sub-match.
    std::string assigmentVariableContentUninterpreted{match.str(2)};

    /*
     * Because `data_` is a `nlohmann::json` object, we can parse the assigment
     * variables using `nlohmann::json::parse`, geeting a `nlohmann::json`
     * object, and then add them to `data_`, instead of parsing and holding the
     * interim result ourselves. 
     * For this, only the list of values needs any manipulation, because it
     * doesn't follow the json syntax. We use the wrong braces.
     */
    if (assigmentVariableContentUninterpreted.at(0) == '{'){
      // We got a list. Change it to the json syntax.
      assigmentVariableContentUninterpreted.replace(0, 1, R"([)");
      assigmentVariableContentUninterpreted.replace(
        assigmentVariableName.length() - 1, 1, R"(])");
    }
    data_[assigmentVariableName] =
      nlohmann::json::parse(assigmentVariableContentUninterpreted);
  }
}
