// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (June of 2023, schlegea@informatik.uni-freiburg.de)

#include "util/ConfigManager/ConfigExceptions.h"

#include <absl/strings/str_cat.h>

#include <exception>
#include <string>

namespace ad_utility {
//_____________________________________________________________________________
std::string& ExceptionWithMessage::getMessage() { return message_; }

//_____________________________________________________________________________
const std::string& ExceptionWithMessage::getMessage() const { return message_; }

//_____________________________________________________________________________
const char* ExceptionWithMessage::what() const throw() {
  return getMessage().c_str();
}

//_____________________________________________________________________________
NoConfigOptionFoundException::NoConfigOptionFoundException(
    std::string_view pathToOption, std::string_view availableOptions) {
  getMessage() =
      absl::StrCat("Key error: There was no configuration option found at '",
                   pathToOption, "'\n", availableOptions, "\n");
}

//_____________________________________________________________________________
ConfigManagerParseConfigNotJsonObjectLiteralException::
    ConfigManagerParseConfigNotJsonObjectLiteralException(
        const nlohmann::json& j) {
  getMessage() = absl::StrCat(
      "A ConfigManager should only parse configurations, that "
      "are a json object literal. The configuration: \n\n",
      j.dump(2), "\n\n is not a json object literal, but a '",
      jsonToTypeString(j), "'.");
}

//_____________________________________________________________________________
ConfigOptionWasntSetException::ConfigOptionWasntSetException(
    std::string_view pathToOption) {
  getMessage() = absl::StrCat(
      "Parsing error: The configuration option at '", pathToOption,
      "' has no default value, yet no value was set at runtime.\n");
}

//_____________________________________________________________________________
NotValidShortHandNameException::NotValidShortHandNameException(
    std::string_view notValidName, std::string_view pathToOption) {
  getMessage() = absl::StrCat(
      "Key error: The key '", notValidName, "' in '", pathToOption,
      "' doesn't describe a valid name, according to the short hand "
      "grammar.");
}

//_____________________________________________________________________________
ConfigOptionSetWrongJsonTypeException::ConfigOptionSetWrongJsonTypeException(
    std::string_view optionIdentifier, std::string_view valueTypeOfOption,
    std::string_view valueTypeRepresentedByJson) {
  getMessage() =
      absl::StrCat("The type of value, that configuration option '",
                   optionIdentifier, "' can hold, is '", valueTypeOfOption,
                   "'. The given json however represents a value of type '",
                   valueTypeRepresentedByJson, "'.\n");
}

//_____________________________________________________________________________
ConfigOptionSetWrongTypeException::ConfigOptionSetWrongTypeException(
    std::string_view optionIdentifier, std::string_view valueTypeOfOption,
    std::string_view valueTypeOfValue) {
  getMessage() = absl::StrCat("The type of the value in configuration option '",
                              optionIdentifier, "' is '", valueTypeOfOption,
                              "'. It can't be set to a value of type '",
                              valueTypeOfValue, "'.");
}

//_____________________________________________________________________________
ConfigOptionValueNotSetException::ConfigOptionValueNotSetException(
    std::string_view optionIdentifier, std::string_view valueName) {
  getMessage() = absl::StrCat("The ", valueName, " of configuration option '",
                              optionIdentifier, "' wasn't set.");
}

//_____________________________________________________________________________
ConfigOptionGetWrongTypeException::ConfigOptionGetWrongTypeException(
    std::string_view optionIdentifier, std::string_view valueTypeOfOption,
    std::string_view templateParameterAsString) {
  getMessage() = absl::StrCat("The type of the value in configuration option '",
                              optionIdentifier, "' is '", valueTypeOfOption,
                              "'. It can't be returned as a value of type '",
                              templateParameterAsString, "'.");
}

//_____________________________________________________________________________
ConfigOptionConstructorNullPointerException::
    ConfigOptionConstructorNullPointerException(
        std::string_view optionIdentifier) {
  getMessage() =
      absl::StrCat("Error, while trying to construct configuration option '",
                   optionIdentifier,
                   "': The variable pointer must point to an actual variable. "
                   "A null pointer is not allowed.");
}
}  // namespace ad_utility
