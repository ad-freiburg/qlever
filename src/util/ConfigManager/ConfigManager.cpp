// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include "util/ConfigManager/ConfigManager.h"

#include <ANTLRInputStream.h>
#include <CommonTokenStream.h>
#include <absl/strings/str_cat.h>
#include <antlr4-runtime.h>

#include <iostream>
#include <iterator>
#include <ranges>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string>

#include "util/Algorithm.h"
#include "util/ConfigManager/ConfigExceptions.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConfigManager/ConfigShorthandVisitor.h"
#include "util/ConfigManager/ConfigUtil.h"
#include "util/ConfigManager/generated/ConfigShorthandLexer.h"
#include "util/ConfigManager/generated/ConfigShorthandParser.h"
#include "util/Exception.h"
#include "util/StringUtils.h"
#include "util/antlr/ANTLRErrorHandling.h"
#include "util/json.h"

namespace ad_utility {

// ____________________________________________________________________________
std::string ConfigManager::createJsonPointerString(
    const std::vector<std::string>& keys) {
  /*
  Escape the characters `/` and `~`, which have a special meaning inside JSON
  pointers, inside the given string.
  */
  auto escapeSpecialCharacters = [](std::string_view key) {
    // Replace special character `~` with `~0` and `/` with `~1`.
    return absl::StrReplaceAll(key, {{"~", "~0"}, {"/", "~1"}});
  };

  // Creating the string for the pointer.
  std::ostringstream pointerString;

  // We don't use a `lazyStrJoin` here, so that an empty `keys` produces an
  // emptry string.
  std::ranges::for_each(
      keys, [&escapeSpecialCharacters, &pointerString](std::string_view key) {
        pointerString << "/" << escapeSpecialCharacters(key);
      });

  return std::move(pointerString).str();
}

// ____________________________________________________________________________
void ConfigManager::verifyPathToConfigOption(
    const std::vector<std::string>& pathToOption,
    std::string_view optionName) const {
  // We need at least a name in the path.
  if (pathToOption.empty()) {
    throw std::runtime_error(
        "The vector 'pathToOption' is empty, which is not allowed. We need at "
        "least a name for a working path to a configuration option.");
  }

  /*
  The last entry in the path is the name of the configuration option. If it
  isn't, something has gone wrong.
  */
  AD_CORRECTNESS_CHECK(pathToOption.back() == optionName);

  /*
  A string must be a valid `NAME` in the short hand. Otherwise, the option can't
  get accessed with the short hand.
  */
  if (auto failedKey =
          std::ranges::find_if_not(pathToOption, isNameInShortHand);
      failedKey != pathToOption.end()) {
    /*
    One of the keys failed. `failedKey` is an iterator pointing to the key.
    */
    throw NotValidShortHandNameException(
        *failedKey, vectorOfKeysForJsonToString(pathToOption));
  }

  // Is there already a configuration option with the same identifier at the
  // same location?
  if (configurationOptions_.contains(createJsonPointerString(pathToOption))) {
    throw ConfigManagerOptionPathAlreadyinUseException(
        vectorOfKeysForJsonToString(pathToOption), printConfigurationDoc(true));
  }
}

// ____________________________________________________________________________
void ConfigManager::addConfigOption(
    const std::vector<std::string>& pathToOption, ConfigOption&& option) {
  // Is the path valid?
  verifyPathToConfigOption(pathToOption, option.getIdentifier());

  // Add the configuration option.
  configurationOptions_.insert(
      {createJsonPointerString(pathToOption), std::move(option)});
}

// ____________________________________________________________________________
const ConfigOption& ConfigManager::getConfigurationOptionByNestedKeys(
    const std::vector<std::string>& keys) const {
  // If there is an config option with that described location, then this should
  // point to the configuration option.
  const std::string ptr{createJsonPointerString(keys)};

  if (configurationOptions_.contains(ptr)) {
    return configurationOptions_.at(ptr);
  } else {
    throw NoConfigOptionFoundException(vectorOfKeysForJsonToString(keys),
                                       printConfigurationDoc(true));
  }
}

// ____________________________________________________________________________
nlohmann::json ConfigManager::parseShortHand(
    const std::string& shortHandString) {
  // I use ANTLR expressions to parse the short hand.
  antlr4::ANTLRInputStream input(shortHandString);
  ConfigShorthandLexer lexer(&input);
  antlr4::CommonTokenStream tokens(&lexer);
  ConfigShorthandParser parser(&tokens);

  // The default in ANTLR is to log all errors to the console and to continue
  // the parsing. We need to turn parse errors into exceptions instead to
  // propagate them to the user.
  ThrowingErrorListener errorListener{};
  parser.removeErrorListeners();
  parser.addErrorListener(&errorListener);
  lexer.removeErrorListeners();
  lexer.addErrorListener(&errorListener);

  // Get the top node. That is, the node of the first grammar rule.
  ConfigShorthandParser::ShortHandStringContext* shortHandStringContext{
      parser.shortHandString()};

  // Walk through the parser tree and build the json equivalent out of the short
  // hand.
  return ToJsonConfigShorthandVisitor{}.visitShortHandString(
      shortHandStringContext);
}

// ____________________________________________________________________________
void ConfigManager::parseConfig(const nlohmann::json& j) {
  // Anything else but a literal json object is not something, we want.
  if (!j.is_object()) {
    throw ConfigManagerParseConfigNotJsonObjectLiteralException(j);
  }

  /*
  We can't write something along the lines of `for (const auto& bla :
  j.flatten().items())` for iteration, because, when iterating over the entries
  of a `nlohmann::json` object using `items()`, there can be error/problems, IF
  the life time of the object, on which one called `items()` on, doesn't exceeds
  the life time of the iteration. (See the nlohmann json documentation for more
  information.)
  */
  const auto& jFlattend = j.flatten();

  /*
  We can skip the following check, if `j` is empty. Note: Even if the JSON
  object is empty, its flattened version contains a single dummy entry, so
  this check is necessary.
  */
  if (!j.empty()) {
    /*
    Does `j` only contain valid configuration options? That is, does it only
    contain paths to entries, that are the same paths as we have saved here?

    For example: If on of our paths in `configurationOptions` was
    `/classA/5/entryNumber5`, then a path like `/clasA/5/entryNumber5`` would be
    invalid, because of the typo.
    */
    for (const auto& item : jFlattend.items()) {
      // Only returns true, if the given pointer is the path to a
      // configuration option.
      auto isPointerToConfigurationOption =
          [this](const nlohmann::json::json_pointer& ptr) {
            return configurationOptions_.contains(ptr.to_string());
          };

      /*
      Because a configuration option can only hold json literal primitives, or
      json literal arrays, we only have to to look at `currentPtr` and its
      father.
      `currentPtr` is valid if either:
      - It's the exact path to a configuration option
      - Its father is the exact path to a configuration option, and it points to
      an array.
      */
      const nlohmann::json::json_pointer currentPtr{item.key()};

      if (!isPointerToConfigurationOption(currentPtr) &&
          (!isPointerToConfigurationOption(currentPtr.parent_pointer()) ||
           !j.at(currentPtr.parent_pointer()).is_array())) {
        throw j.at(currentPtr.parent_pointer()).is_array()
            ? NoConfigOptionFoundException(
                  currentPtr.parent_pointer().to_string(),
                  printConfigurationDoc(false))
            : NoConfigOptionFoundException(currentPtr.to_string(),
                                           printConfigurationDoc(false));
      }
    }
  }

  /*
  Alright, time to actually set the configuration options. This will only throw
  an exception, if a configuration option was given a value of the wrong type,
  or if it HAD to be set, but wasn't.
  */
  for (auto& [key, option] : configurationOptions_) {
    // Set the option, if possible, with the pointer to the position of the
    // current configuration in json.
    if (const nlohmann::json::json_pointer configurationOptionJsonPosition{key};
        j.contains(configurationOptionJsonPosition)) {
      // This will throw an exception, if the json object can't be interpreted
      // with the wanted type.
      option.setValueWithJson(j.at(configurationOptionJsonPosition));
    }

    /*
    If the option hasn't set the variable, that it's internal variable pointer
    points to, that means, it doesn't have a default value, and needs to be set
    by the user at runtime, but wasn't.
    */
    if (!option.wasSet()) {
      throw ConfigOptionWasntSetException(key);
    }
  }
}

// ____________________________________________________________________________
std::string ConfigManager::printConfigurationDoc(
    bool printCurrentJsonConfiguration) const {
  // Handeling, for when there are no configuration options.
  if (configurationOptions_.empty()) {
    return "No configuration options were defined.";
  }

  // Setup for printing the locations of the option in json format, so that
  // people can easier understand, where everything is.
  nlohmann::json configuratioOptionsVisualization;

  /*
  Add the paths of `configOptions_` and have them point to either:
  - The current value of the configuration option.
  - A "value was never initialized", if we are not sure, the current value of
    the configuration option was initialized.
  - The default value of the configuration option.
  - An example value, of the correct type.
  */
  for (const auto& [path, option] : configurationOptions_) {
    // Pointer to the position of this option in
    // `configuratioOptionsVisualization`.
    const nlohmann::json::json_pointer jsonOptionPointer{path};

    if (printCurrentJsonConfiguration) {
      // We can only use the value, if we are sure, that the value was
      // initialized.
      configuratioOptionsVisualization[jsonOptionPointer] =
          option.wasSet() ? option.getValueAsJson()
                          : "value was never initialized";
    } else {
      configuratioOptionsVisualization[jsonOptionPointer] =
          option.hasDefaultValue() ? option.getDefaultValueAsJson()
                                   : option.getDummyValueAsJson();
    }
  }

  const std::string& configuratioOptionsVisualizationAsString =
      configuratioOptionsVisualization.dump(2);

  // List the configuration options themselves.
  const std::string& listOfConfigurationOptions = ad_utility::lazyStrJoin(
      std::views::transform(configurationOptions_,
                            [](const auto& pair) {
                              // Add the location of the option and the option
                              // itself.
                              return absl::StrCat(
                                  "Location : ", pair.first, "\n",
                                  static_cast<std::string>(pair.second));
                            }),
      "\n\n");

  return absl::StrCat(
      "Locations of available configuration options with",
      (printCurrentJsonConfiguration ? " their current values"
                                     : " example values"),
      ":\n",
      ad_utility::addIndentation(configuratioOptionsVisualizationAsString,
                                 "    "),
      "\n\nAvailable configuration options:\n",
      ad_utility::addIndentation(listOfConfigurationOptions, "    "),
      "\n\nConfiguration options, that kept their default values:\n",
      ad_utility::addIndentation(
          getListOfNotChangedConfigOptionsWithDefaultValuesAsString(), "    "));
}

// ____________________________________________________________________________
std::string ConfigManager::vectorOfKeysForJsonToString(
    const std::vector<std::string>& keys) {
  std::ostringstream keysToString;
  std::ranges::for_each(keys, [&keysToString](std::string_view key) {
    keysToString << "[" << key << "]";
  });
  return std::move(keysToString).str();
}

// ____________________________________________________________________________
std::string
ConfigManager::getListOfNotChangedConfigOptionsWithDefaultValuesAsString()
    const {
  // For only looking at the configuration options in our map.
  auto onlyConfigurationOptionsView = std::views::transform(
      configurationOptions_, [](const auto& pair) { return pair.second; });

  // Returns true, if the `ConfigOption` has a default value and wasn't set at
  // runtime.
  auto valueIsUnchangedFromDefault = [](const ConfigOption& option) {
    return option.hasDefaultValue() && !option.wasSetAtRuntime();
  };

  // Prints the default value of a configuration option and the accompanying
  // text.
  auto defaultConfigurationOptionToString = [](const ConfigOption& option) {
    return absl::StrCat("Configuration option '", option.getIdentifier(),
                        "' was not set at runtime, using default value '",
                        option.getDefaultValueAsString(), "'.");
  };

  // A string vector view of all our unchanged configuration options in the
  // string form from `defaultConfigurationOptionToString`.
  auto unchangedFromDefaultConfigOptions =
      onlyConfigurationOptionsView |
      std::views::filter(valueIsUnchangedFromDefault) |
      std::views::transform(defaultConfigurationOptionToString);

  return ad_utility::lazyStrJoin(unchangedFromDefaultConfigOptions, "\n");
}
}  // namespace ad_utility
