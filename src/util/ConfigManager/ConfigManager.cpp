// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include <ANTLRInputStream.h>
#include <CommonTokenStream.h>
#include <absl/strings/str_cat.h>
#include <antlr4-runtime.h>

#include <algorithm>
#include <functional>
#include <iostream>
#include <iterator>
#include <ranges>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

#include "util/Algorithm.h"
#include "util/ConfigManager/ConfigExceptions.h"
#include "util/ConfigManager/ConfigManager.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConfigManager/ConfigShorthandVisitor.h"
#include "util/ConfigManager/ConfigUtil.h"
#include "util/ConfigManager/generated/ConfigShorthandLexer.h"
#include "util/ConfigManager/generated/ConfigShorthandParser.h"
#include "util/Exception.h"
#include "util/StringUtils.h"
#include "util/TypeTraits.h"
#include "util/antlr/ANTLRErrorHandling.h"
#include "util/json.h"

namespace ad_utility {
// ____________________________________________________________________________
template <typename ReturnPointer>
std::vector<std::pair<std::string, ReturnPointer>>
ConfigManager::configurationOptionsImpl(auto& configurationOptions,
                                        std::string_view pathPrefix) {
  std::vector<std::pair<std::string, ReturnPointer>> collectedOptions;

  std::ranges::for_each(
      configurationOptions,
      [&collectedOptions, &pathPrefix](auto pair) {
        const std::string& pathToCurrentEntry =
            absl::StrCat(pathPrefix, std::get<0>(pair));

        std::visit(
            [&collectedOptions, &pathToCurrentEntry]<typename T>(T& var) {
              // A normal `ConfigOption` can be directly added. For a
              // `ConfigManager` we have to recursively collect the options.
              if constexpr (isSimilar<T, ConfigOption>) {
                collectedOptions.push_back(
                    std::make_pair(pathToCurrentEntry, &var));
              } else {
                AD_CORRECTNESS_CHECK((isSimilar<T, ConfigManager>));
                ad_utility::appendVector(
                    collectedOptions,
                    configurationOptionsImpl<ReturnPointer>(
                        var.configurationOptions_, pathToCurrentEntry));
              }
            },
            std::get<1>(pair));
      },
      [&pathPrefix](auto& pair) {
        // Make sure, that there is no null pointer.
        AD_CORRECTNESS_CHECK(pair.second != nullptr);

        // An empty sub manager tends to point to a logic error on the user
        // side.
        if (const ConfigManager* ptr =
                std::get_if<ConfigManager>(pair.second.get());
            ptr != nullptr && ptr->configurationOptions_.empty()) {
          throw std::runtime_error(absl::StrCat(
              "The sub manager at '", pathPrefix, std::get<0>(pair),
              "' is empty. Either fill it, or delete it."));
        }

        // Return a dereferenced reference.
        return std::tie(pair.first, *pair.second);
      });

  return collectedOptions;
}

// ____________________________________________________________________________
std::vector<std::pair<std::string, ConfigOption*>>
ConfigManager::configurationOptions(std::string_view pathPrefix) {
  return configurationOptionsImpl<ConfigOption*>(configurationOptions_,
                                                 pathPrefix);
}

// ____________________________________________________________________________
std::vector<std::pair<std::string, const ConfigOption*>>
ConfigManager::configurationOptions(std::string_view pathPrefix) const {
  return configurationOptionsImpl<const ConfigOption*>(configurationOptions_,
                                                       pathPrefix);
}

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
void ConfigManager::verifyPath(const std::vector<std::string>& path) const {
  // We need at least a name in the path.
  if (path.empty()) {
    throw std::runtime_error(
        "It is forbidden to call `addConfigOption`, or `addSubManager`, with "
        "an empty vector as the first argument.");
  }

  /*
  A string must be a valid `NAME` in the short hand. Otherwise, an option can't
  get accessed with the short hand.
  */
  if (auto failedKey = std::ranges::find_if_not(path, isNameInShortHand);
      failedKey != path.end()) {
    /*
    One of the keys failed. `failedKey` is an iterator pointing to the key.
    */
    throw NotValidShortHandNameException(*failedKey,
                                         vectorOfKeysForJsonToString(path));
  }

  /*
  Checks for path collisions, with already added config options and sub
  managers.

  The following cases are not allowed:
  - Same path. Makes it impossible for the user to later identify the correct
  one.
  - Prefix of the path of an already exiting option/manager. This would mean,
  that the old config option, or sub manager, are part of the new config option,
  or sub manager from the view of json. This is not allowed for a new config
  option, because there is currently no support to put config options, or sub
  managers, inside config options. For a new sub manager it's not allowed,
  because nesting should be done on the `C++` level, not on the json path level.
  - The path of an already exiting option/manager is a prefix of the new path.
  The reasons, why it's not allowed, are basically the same.
  */
  std::ranges::for_each(
      std::views::keys(configurationOptions_),
      [&path, this](std::string_view alreadyAddedPath) {
        const std::string pathAsJsonPointerString =
            createJsonPointerString(path);

        // Is there already a path, that is the exact same?
        if (pathAsJsonPointerString == alreadyAddedPath) {
          throw std::runtime_error(
              absl::StrCat("Key error: There is already a configuration "
                           "option, or sub manager, with the path '",
                           vectorOfKeysForJsonToString(path), "'\n",
                           printConfigurationDoc(true), "\n"));
        }

        // Is the new path a prefix of the path of an already
        // existing path?
        if (alreadyAddedPath.starts_with(pathAsJsonPointerString)) {
          throw std::runtime_error(absl::StrCat(
              "Key error: The given path '", vectorOfKeysForJsonToString(path),
              "' is a prefix of a path, '", alreadyAddedPath,
              "', that is already in use.", "'\n", printConfigurationDoc(true),
              "\n"));
        }

        // Is the already existing path a prefix of the new path?
        if (pathAsJsonPointerString.starts_with(alreadyAddedPath)) {
          throw std::runtime_error(absl::StrCat(
              "Key error: The given path '", vectorOfKeysForJsonToString(path),
              "' has an already in use path '", alreadyAddedPath,
              "' as an prefix.", "'\n", printConfigurationDoc(true), "\n"));
        }
      });
}

// ____________________________________________________________________________
ConfigOption& ConfigManager::addConfigOption(
    const std::vector<std::string>& pathToOption, ConfigOption&& option) {
  // Is the path valid?
  verifyPath(pathToOption);

  // Add the configuration option and return the inserted element.
  return std::get<ConfigOption>(
      *configurationOptions_
           .insert({createJsonPointerString(pathToOption),
                    std::make_unique<HashMapEntry::element_type>(
                        std::move(option))})
           .first->second);
}

// ____________________________________________________________________________
ConfigManager& ConfigManager::addSubManager(
    const std::vector<std::string>& path) {
  // Is the path valid?
  verifyPath(path);

  // The path in json format.
  const std::string jsonPath = createJsonPointerString(path);

  // Add the configuration manager and return the inserted element.
  return std::get<ConfigManager>(
      *configurationOptions_
           .emplace(jsonPath, std::make_unique<HashMapEntry::element_type>(
                                  ConfigManager{}))
           .first->second);
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
  antlr_utility::ThrowingErrorListener<InvalidConfigShortHandParseException>
      errorListener{};
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

  // All the configuration options together with their paths.
  std::vector<std::pair<std::string, ConfigOption*>> allConfigOption =
      configurationOptions("");
  ad_utility::HashMap<std::string, ConfigOption*> allConfigOptionHashMap(
      allConfigOption.begin(), allConfigOption.end());

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
          [&allConfigOptionHashMap](const nlohmann::json::json_pointer& ptr) {
            return allConfigOptionHashMap.contains(ptr.to_string());
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
  for (auto&& [key, option] : std::views::transform(
           allConfigOption,
           [](auto& pair) { return std::tie(pair.first, *pair.second); })) {
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
    points to, that means, it doesn't have a default value, and needs to be
    set by the user at runtime, but wasn't.
    */
    if (!option.wasSet()) {
      throw ConfigOptionWasntSetException(key);
    }
  }

  // Check with the validators, if all the new values are valid.
  verifyWithValidators();
}

// ____________________________________________________________________________
std::string ConfigManager::printConfigurationDoc(
    bool printCurrentJsonConfiguration) const {
  // All the configuration options together with their paths.
  const std::vector<std::pair<std::string, const ConfigOption*>>
      allConfigOption = configurationOptions("");
  auto allConfigOptionDereferencedView = std::views::transform(
      allConfigOption,
      [](auto& pair) { return std::tie(pair.first, *pair.second); });

  // Handeling, for when there are no configuration options.
  if (allConfigOptionDereferencedView.empty()) {
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
  for (const auto& [path, option] : allConfigOptionDereferencedView) {
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
      std::views::transform(allConfigOptionDereferencedView,
                            [](const auto& pair) {
                              // Add the location of the option and the option
                              // itself.
                              return absl::StrCat(
                                  "Location : ", std::get<0>(pair), "\n",
                                  static_cast<std::string>(std::get<1>(pair)));
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
  // All the configuration options together with their paths.
  const std::vector<std::pair<std::string, const ConfigOption*>>
      allConfigOption = configurationOptions("");
  auto allConfigOptionDereferencedView = std::views::transform(
      allConfigOption,
      [](auto& pair) { return std::tie(pair.first, *pair.second); });

  // For only looking at the configuration options in our map.
  auto onlyConfigurationOptionsView =
      std::views::values(allConfigOptionDereferencedView);

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
// ____________________________________________________________________________
void ConfigManager::verifyWithValidators() const {
  // Check all the validators in sub managers.
  std::ranges::for_each(
      configurationOptions_,
      [](auto pair) {
        std::visit(
            []<typename T>(T& var) {
              // Nothing to do, if we are not looking at a config manager.
              if constexpr (isSimilar<T, ConfigManager>) {
                var.verifyWithValidators();
              }
            },
            std::get<1>(pair));
      },
      [](auto& pair) {
        // Make sure, that there is no null pointer.
        AD_CORRECTNESS_CHECK(pair.second != nullptr);

        // Return a dereferenced reference.
        return std::tie(pair.first, *pair.second);
      });

  // Check all validators, that were directly registered.
  std::ranges::for_each(validators_,
                        [](auto& validator) { std::invoke(validator); });
};

// ____________________________________________________________________________
bool ConfigManager::containsOption(const ConfigOption& opt) const {
  const auto allOptions = configurationOptions();
  const auto allOptionsValues = std::views::values(allOptions);
  return std::ranges::find(allOptionsValues, &opt) != allOptionsValues.end();
}
}  // namespace ad_utility
