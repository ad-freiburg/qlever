// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (June of 2023, schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>

#include <exception>
#include <string>

#include "util/ConfigManager/ConfigExceptions.h"

namespace ad_utility {
//_____________________________________________________________________________
const char* ExceptionWithMessage::what() const throw() {
  return message_.c_str();
}

//_____________________________________________________________________________
NoConfigOptionFoundException::NoConfigOptionFoundException(
    std::string_view pathToOption, std::string_view availableOptions) {
  message_ =
      absl::StrCat("Key error: There was no configuration option found at '",
                   pathToOption, "'\n", availableOptions, "\n");
}

//_____________________________________________________________________________
ConfigManagerParseConfigNotJsonObjectLiteralException::
    ConfigManagerParseConfigNotJsonObjectLiteralException(
        const nlohmann::json& j) {
  message_ = absl::StrCat(
      "A ConfigManager should only parse configurations, that "
      "are a json object literal. The configuration: \n\n",
      j.dump(2), "\n\n is not a json object literal, but a '",
      jsonToTypeString(j), "'.");
}

//_____________________________________________________________________________
ConfigOptionWasntSetException::ConfigOptionWasntSetException(
    std::string_view pathToOption) {
  message_ = absl::StrCat(
      "Parsing error: The configuration option at '", pathToOption,
      "' has no default value, yet no value was set at runtime.\n");
}

//_____________________________________________________________________________
ConfigManagerOptionPathDoesntStartWithStringException::
    ConfigManagerOptionPathDoesntStartWithStringException(
        std::string_view pathToOption) {
  message_ = absl::StrCat(
      "Key error, while trying to add a configuration "
      "option: The first key in '",
      pathToOption,
      "' isn't a string. It needs to be a string, because "
      "internally we save locations in a json format, more "
      "specificly in a json object literal.");
}

//_____________________________________________________________________________
ConfigManagerOptionPathDoesntEndWithStringException::
    ConfigManagerOptionPathDoesntEndWithStringException(
        std::string_view pathToOption) {
  message_ = absl::StrCat(
      "Key error, while trying to add a configuration "
      "option: The last key in '",
      pathToOption,
      "' isn't a string. It needs to be a string, because it will be used as "
      "the name of the configuration option.");
}

//_____________________________________________________________________________
NotValidShortHandNameException::NotValidShortHandNameException(
    std::string_view notValidName, std::string_view pathToOption) {
  message_ = absl::StrCat(
      "Key error: The key '", notValidName, "' in '", pathToOption,
      "' doesn't describe a valid name, according to the short hand "
      "grammar.");
}

//_____________________________________________________________________________
ConfigManagerOptionPathAlreadyinUseException::
    ConfigManagerOptionPathAlreadyinUseException(
        std::string_view pathToOption,
        std::string_view allPathsCurrentlyInUse) {
  message_ = absl::StrCat(
      "Key error: There is already a configuration option with the path '",
      pathToOption, "'\n", allPathsCurrentlyInUse, "\n");
}

//_____________________________________________________________________________
ConfigOptionSetWrongJsonTypeException::ConfigOptionSetWrongJsonTypeException(
    std::string_view optionIdentifier, std::string_view valueTypeOfOption,
    std::string_view valueTypeRepresentedByJson) {
  message_ =
      absl::StrCat("The type of value, that configuration option '",
                   optionIdentifier, "' can hold, is '", valueTypeOfOption,
                   "'. The given json however represents a value of type '",
                   valueTypeRepresentedByJson, "'.\n");
}

//_____________________________________________________________________________
ConfigOptionSetWrongTypeException::ConfigOptionSetWrongTypeException(
    std::string_view optionIdentifier, std::string_view valueTypeOfOption,
    std::string_view valueTypeOfValue) {
  message_ = absl::StrCat("The type of the value in configuration option '",
                          optionIdentifier, "' is '", valueTypeOfOption,
                          "'. It can't be set to a value of type '",
                          valueTypeOfValue, "'.");
}

//_____________________________________________________________________________
ConfigOptionValueNotSetException::ConfigOptionValueNotSetException(
    std::string_view optionIdentifier, std::string_view valueName) {
  message_ = absl::StrCat("The ", valueName, " of configuration option '",
                          optionIdentifier, "' wasn't set.");
}

//_____________________________________________________________________________
ConfigOptionGetWrongTypeException::ConfigOptionGetWrongTypeException(
    std::string_view optionIdentifier, std::string_view valueTypeOfOption,
    std::string_view templateParameterAsString) {
  message_ = absl::StrCat("The type of the value in configuration option '",
                          optionIdentifier, "' is '", valueTypeOfOption,
                          "'. It can't be returned as a value of type '",
                          templateParameterAsString, "'.");
}

//_____________________________________________________________________________
ConfigOptionConstructorNullPointerException::
    ConfigOptionConstructorNullPointerException(
        std::string_view optionIdentifier) {
  message_ =
      absl::StrCat("Error, while trying to construct configuration option '",
                   optionIdentifier,
                   "': The variable pointer must point to an actual variable. "
                   "A null pointer is not allowed.");
}

//_____________________________________________________________________________
ConfigManagerPathToConfigOptionDoesntEndWithConfigOptionNameException::
    ConfigManagerPathToConfigOptionDoesntEndWithConfigOptionNameException(
        std::string_view optionIdentifier, std::string_view pathToOption) {
  message_ = absl::StrCat(
      "Error while trying to add a ConfigOption to a ConfigManager: The path '",
      pathToOption, "' doesn't end with the name of ConfigOption '",
      optionIdentifier, "'.");
}
}  // namespace ad_utility
