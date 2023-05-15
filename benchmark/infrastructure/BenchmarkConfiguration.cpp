// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>

#include <iostream>
#include <regex>
#include <sstream>
#include <string>

#include "../benchmark/infrastructure/BenchmarkConfiguration.h"
#include "BenchmarkConfigurationOption.h"
#include "BenchmarkResultToString.h"
#include "nlohmann/json.hpp"
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
const std::vector<BenchmarkConfigurationOption>&
BenchmarkConfiguration::getConfigurationOptions() const {
  return configurationOptions_;
}

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
  setJsonString(parseShortHand(shortHandString).dump());
}

// ____________________________________________________________________________
void BenchmarkConfiguration::setJsonString(const std::string& jsonString) {
  const nlohmann::json& stringAsJson = nlohmann::json::parse(jsonString);
  // Anything else but a literal json object is not something, we want.
  if (!stringAsJson.is_object()) {
    throw ad_utility::Exception(
        "A BenchmarkConfiguration"
        " should only be set with a json object literal.");
  }

  /*
  According to the documentation, when iterating over the entries of a
  `nlohmann::json` object using `items()`, there can be error/problems, IF the
  life time of the object, on which one called `items()` on, doesn't exceeds the
  life time of the iteration. (See the documentation for more information.)

  Because we iterate over the 'flattend' verions of two `nlohmann::json`
  objects, I gave them const references here.
  */
  const auto& stringAsJsonFlattend = stringAsJson.flatten();
  const auto& keyToConfigurationOptionIndexFlattend =
      keyToConfigurationOptionIndex_.flatten();

  /*
  Does `stringAsJson` only contain valid configuration options?
  That is, does it only contain paths to entries, that are the same paths as we
  have saved there?

  For example: If on of our paths in `keyToConfigurationOptionIndex_` was
  `{"classA": [..., {"entryNumber5" : 5}]}`, then a path like `{"clasA": [...,
  {"entryNumber5" : 5}]}` would be invalid, because of the typo.
  */
  for (const auto& item : stringAsJsonFlattend.items()) {
    /*
    We go over ALL valid json pointers in `stringAsJson` and check, if they, or
    a valid sub json pointer of them, are contained in
    `keyToConfigurationOptionIndex_`. This will only find paths, that point to
    non existent configuration options, or have typos.
    */
    nlohmann::json::json_pointer currentPtr{item.key()};

    while (!keyToConfigurationOptionIndex_.contains(currentPtr)) {
      // Go to the parent pointer.
      currentPtr = currentPtr.parent_pointer();

      // If we reach the root, it's a completly non-valid path.
      if (currentPtr.empty()) {
        throw ad_utility::Exception(absl::StrCat(
            "Error while trying to set configuration option: There is no valid "
            "sub json pointer in '",
            item.key(), "', that points to a valid configuration option.\n",
            static_cast<std::string>(*this), "\n"));
      }
    }
  }

  /*
  Alright, time to actually set the configuration options. This will only throw
  an exception, if a configuration option was given a value of the wrong type,
  or if it HAD to be set, but wasn't.
  */
  for (const auto& configurationOptionIndex :
       keyToConfigurationOptionIndexFlattend.items()) {
    // Pointer to the position of the current configuration in json.
    const nlohmann::json::json_pointer configurationOptionJsonPosition{
        configurationOptionIndex.key()};

    BenchmarkConfigurationOption* configurationOption =
        &(configurationOptions_.at(
            keyToConfigurationOptionIndex_.at(configurationOptionJsonPosition)
                .get<size_t>()));

    // Set the option, if possible.
    if (stringAsJson.contains(configurationOptionJsonPosition)) {
      // This will throw an exception, if the json object can't be interpreted
      // with the wanted type.
      configurationOption->setValueWithJson(
          stringAsJson.at(configurationOptionJsonPosition));
    }

    // If the option has no value now, that means, it didn't have a default
    // value, and needs to be set by the user at runtime, but wasn't.
    if (!configurationOption->hasValue()) {
      throw ad_utility::Exception(
          absl::StrCat("Error while trying to set configuration options: The "
                       "configuration option at '",
                       configurationOptionJsonPosition.to_string(),
                       "' wasn't defined by the user, even though, this "
                       "configuration option has no default value.\n",
                       static_cast<std::string>(*this), "\n"));
    }
  }
}

// ____________________________________________________________________________
BenchmarkConfiguration::operator std::string() const {
  // For listing all available configuration options.
  std::ostringstream stream;

  /*
  `nlohmann::json` has a weird bug with iterating over `items()`, where there
  will be bugs/glitches, if the object, one called `items()` with, doesn't
  have a longer life time than the iteration itself.
  */
  const nlohmann::json& flattendKeyToConfigurationOptionIndex{
      keyToConfigurationOptionIndex_.flatten()};

  // Setup for printing the locations of the option in json format, so that
  // people can easier underrstand, where everything is.
  nlohmann::json prettyKeyToConfigurationOptionIndex(
      keyToConfigurationOptionIndex_);
  // Replace the numbers in the 'leaves' of the 'tree' with a reference to the
  // option list.
  for (const auto& keyToLeaf : flattendKeyToConfigurationOptionIndex.items()) {
    prettyKeyToConfigurationOptionIndex.at(
        nlohmann::json::json_pointer{keyToLeaf.key()}) =
        absl::StrCat("See configuration option '",
                     configurationOptions_.at(keyToLeaf.value().get<size_t>())
                         .getIdentifier(),
                     "' in the list of available configuration options below.");
  }

  // List the configuration options themselves.
  for (const auto& keyValuePair :
       flattendKeyToConfigurationOptionIndex.items()) {
    // Add the location of the option and the option itself.
    stream << "Location : " << keyValuePair.key() << "\n"
           << static_cast<std::string>(
                  configurationOptions_.at(keyValuePair.value().get<size_t>()));

    // The last entry doesn't get linebreaks.
    if (keyValuePair.value() != flattendKeyToConfigurationOptionIndex.back()) {
      stream << "\n\n";
    }
  }

  return absl::StrCat(
      "Locations of available configuration options:\n",
      addIndentation(prettyKeyToConfigurationOptionIndex.dump(2), 1),
      "\n\nAvailable configuration options:\n",
      addIndentation(stream.str(), 1));
}
}  // namespace ad_benchmark
