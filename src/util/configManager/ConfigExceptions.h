// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (June of 2023, schlegea@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_UTIL_CONFIGMANAGER_CONFIGEXCEPTIONS_H
#define QLEVER_SRC_UTIL_CONFIGMANAGER_CONFIGEXCEPTIONS_H

#include <exception>
#include <string>
#include <string_view>

#include "util/ParseException.h"
#include "util/json.h"

namespace ad_utility {

/*
@brief A custom exception, for when there are parsing error with the short hand.
*/
class InvalidConfigShortHandParseException : public ParseException {
 public:
  explicit InvalidConfigShortHandParseException(
      std::string_view cause,
      std::optional<ExceptionMetadata> metadata = std::nullopt)
      : ParseException{cause, std::move(metadata),
                       "Invalid config short hand:"} {}
};

/*
@brief Provides the normal structure for exceptions, where the class constructor
builds a string message, which will be later returned.
*/
class ExceptionWithMessage : public std::exception {
  // The error message.
  std::string message_;

 public:
  std::string& getMessage();
  const std::string& getMessage() const;

  const char* what() const throw() final;
};

/*
@brief A custom exception, for when there was no configuration option found at
the end of a path.
*/
class NoConfigOptionFoundException : public ExceptionWithMessage {
 public:
  /*
  @param pathToOption The path, at which end no configuration option was found.
  @param availableOptions The options, that are actually available.
  */
  explicit NoConfigOptionFoundException(std::string_view pathToOption,
                                        std::string_view availableOptions);
};

/*
@brief A custom exception, for when `ConfigManager::parseConfig` is given a
`nlohmann::json` object, which doesn't represent a json object literal.
*/
class ConfigManagerParseConfigNotJsonObjectLiteralException
    : public ExceptionWithMessage {
 public:
  /*
  @param j The `nlohmann::json` object, which represent something different than
  a json object literal.
  */
  explicit ConfigManagerParseConfigNotJsonObjectLiteralException(
      const nlohmann::json& j);
};

/*
@brief A custom exception, for when a `ConfigOption` has no default value and
wasn't set at runtime.
*/
class ConfigOptionWasntSetException : public ExceptionWithMessage {
 public:
  /*
  @param pathToOption The path to the option.
  */
  explicit ConfigOptionWasntSetException(std::string_view pathToOption);
};

/*
@brief A custom exception, for when the content of a string describes a name,
that is not allowed by the short hand grammar.
*/
class NotValidShortHandNameException : public ExceptionWithMessage {
 public:
  /*
  @param pathToOption The path to the option.
  */
  explicit NotValidShortHandNameException(std::string_view notValidName,
                                          std::string_view pathToOption);
};

/*
@brief A custom exception, for when somebody tries to set a `ConfigOption` with
a json value, that represents the wrong type.
*/
class ConfigOptionSetWrongJsonTypeException : public ExceptionWithMessage {
 public:
  /*
  @param optionIdentifier The name of the option.
  @param valueTypeOfOption A string representation of the type of value, that
  the configuration option can hold.
  @param valueTypeRepresentedByJson The type of value, that the json represents.
  */
  explicit ConfigOptionSetWrongJsonTypeException(
      std::string_view optionIdentifier, std::string_view valueTypeOfOption,
      std::string_view valueTypeRepresentedByJson);
};

/*
@brief A custom exception, for when somebody tries to set a `ConfigOption` with
a value of the wrong type.
*/
class ConfigOptionSetWrongTypeException : public ExceptionWithMessage {
 public:
  /*
  @param optionIdentifier The name of the option.
  @param valueTypeOfOption A string representation of the type of value, that
  the configuration option can hold.
  @param valueTypeOfValue The type of the value, that the user tried to set the
  `ConfigOption` to.
  */
  explicit ConfigOptionSetWrongTypeException(std::string_view optionIdentifier,
                                             std::string_view valueTypeOfOption,
                                             std::string_view valueTypeOfValue);
};

/*
@brief A custom exception, for when somebody tries to get a value from a
`ConfigOption`, but the value wasn't set.
*/
class ConfigOptionValueNotSetException : public ExceptionWithMessage {
 public:
  /*
  @param optionIdentifier The name of the option.
  @param valueName The name of the value, that the user tried to get.
  */
  explicit ConfigOptionValueNotSetException(std::string_view optionIdentifier,
                                            std::string_view valueName);
};

/*
@brief A custom exception, for when somebody tries to get a value from a
`ConfigOption`, but gives the wrong type as template parameter.
*/
class ConfigOptionGetWrongTypeException : public ExceptionWithMessage {
 public:
  /*
  @param optionIdentifier The name of the option.
  @param valueTypeOfOption A string representation of the type of value, that
  the configuration option can hold.
  @param templateParameterAsString The name of the type, that was given as a
  template parameter.
  */
  explicit ConfigOptionGetWrongTypeException(
      std::string_view optionIdentifier, std::string_view valueTypeOfOption,
      std::string_view templateParameterAsString);
};

/*
@brief A custom exception, for when somebody tries to construct a `ConfigOption`
and gives the constructor a `nullptr`.
*/
class ConfigOptionConstructorNullPointerException
    : public ExceptionWithMessage {
 public:
  /*
  @param optionIdentifier The name of the option.
  */
  explicit ConfigOptionConstructorNullPointerException(
      std::string_view optionIdentifier);
};
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_CONFIGMANAGER_CONFIGEXCEPTIONS_H
