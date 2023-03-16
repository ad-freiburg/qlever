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
  const std::string valueLiterals = R"(true|false|-?\d+)";
  // How a list of `valueLiterals` looks like.
  const std::string listOfValueLiterals = R"(\{\s*()" + valueLiterals + R"()\s*,)*\s*()" + valueLiterals + R"()\s*\})";
  // What kind of names can the left side of the assigment
  // `variableName = variableContent;` have?
  const std::string variableName = R"([\w]+)";
  // What kind of names can the right side of the assigment
  // `variableName = variableContent;` have?
  const std::string variableContent = valueLiterals + R"(|)" + listOfValueLiterals;
  // How does one assigment look like?
  const std::string assigment = R"(\s*)" + variableName + R"(\s*=\s*()" + variableContent + R"()\s*;)";
  
  // Use regular expressions to check, if the given string uses the correct
  // grammar/syntax.
  AD_CHECK(std::regex_match(shortHandString, std::regex{R"(()" + assigment + R"()*)"}));
  // TODO Actually parse this.
  return;
}
