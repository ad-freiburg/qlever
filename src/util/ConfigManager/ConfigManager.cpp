// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include "util/ConfigManager/ConfigManager.h"

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
#include <unordered_map>
#include <utility>
#include <variant>

#include "util/Algorithm.h"
#include "util/ComparisonWithNan.h"
#include "util/ConfigManager/ConfigExceptions.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConfigManager/ConfigShorthandVisitor.h"
#include "util/ConfigManager/ConfigUtil.h"
#include "util/ConfigManager/generated/ConfigShorthandLexer.h"
#include "util/ConfigManager/generated/ConfigShorthandParser.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/StringUtils.h"
#include "util/TypeTraits.h"
#include "util/antlr/ANTLRErrorHandling.h"
#include "util/json.h"

namespace ad_utility {
// ____________________________________________________________________________
ConfigManager::HashMapEntry::HashMapEntry(Data&& data)
    : data_{std::make_unique<Data>(std::move(data))} {}

// ____________________________________________________________________________
template <typename T>
requires isTypeContainedIn<T, ConfigManager::HashMapEntry::Data>
bool ConfigManager::HashMapEntry::implHolds() const {
  // Make sure, that it is not a null pointer.
  AD_CORRECTNESS_CHECK(data_);

  return std::holds_alternative<T>(*data_);
}

// ____________________________________________________________________________
bool ConfigManager::HashMapEntry::holdsConfigOption() const {
  return implHolds<ConfigOption>();
}

// ____________________________________________________________________________
bool ConfigManager::HashMapEntry::holdsSubManager() const {
  return implHolds<ConfigManager>();
}

// ____________________________________________________________________________
std::optional<ConfigOption*> ConfigManager::HashMapEntry::getConfigOption()
    const {
  if (holdsConfigOption()) {
    return std::get_if<ConfigOption>(data_.get());
  } else {
    return std::nullopt;
  }
}

// ____________________________________________________________________________
size_t ConfigManager::HashMapEntry::getInitializationId() const {
  return initializationId_;
}

// ____________________________________________________________________________
std::optional<ConfigManager*> ConfigManager::HashMapEntry::getSubManager()
    const {
  if (holdsSubManager()) {
    return std::get_if<ConfigManager>(data_.get());
  } else {
    return std::nullopt;
  }
}

// ____________________________________________________________________________
void ConfigManager::verifyHashMapEntry(std::string_view jsonPathToEntry,
                                       const HashMapEntry& entry) {
  // An empty sub manager tends to point to a logic error on the user
  // side.
  if (const std::optional<ConfigManager*>& ptr = entry.getSubManager();
      ptr.has_value() && ptr.value()->configurationOptions_.empty()) {
    throw std::runtime_error(
        absl::StrCat("The sub manager at '", jsonPathToEntry,
                     "' is empty. Either fill it, or delete it."));
  }
}

// ____________________________________________________________________________
template <ad_utility::InvocableWithExactReturnType<
    bool, const ConfigManager::HashMapEntry&>
              Predicate>
std::vector<std::pair<std::string, const ConfigManager::HashMapEntry&>>
ConfigManager::allHashMapEntries(
    const ad_utility::HashMap<std::string, HashMapEntry>& entries,
    const bool sortByInitialization, std::string_view pathPrefix,
    const Predicate& predicate) {
  // `std::reference_wrapper` works with `std::ranges::sort`. `const
  // HashMapEntry&` does not.
  std::vector<
      std::pair<std::string, std::reference_wrapper<const HashMapEntry>>>
      allHashMapEntry;

  /*
  Takes one entry of an instance of `entries`, checks it with
  `verifyHashMapEntry` and passes it on.
  */
  auto verifyEntry = [&pathPrefix](const auto& pair) {
    const auto& [jsonPath, hashMapEntry] = pair;

    // Check the hash map entry.
    verifyHashMapEntry(absl::StrCat(pathPrefix, jsonPath), hashMapEntry);

    // Return a reference.
    return std::tie(jsonPath, hashMapEntry);
  };

  /*
  Takes one entry of an instance of `entries`, that was transformed
  with `verifyEntry`, and adds the `HashMapEntry`, that can be found inside it,
  together with the paths to them, to `allHashMapEntry`, iff the predicate
  returns true.
  */
  auto addHashMapEntryToCollectedOptions = [&allHashMapEntry, &pathPrefix,
                                            &predicate](const auto pair) {
    const auto& [jsonPath, hashMapEntry] = pair;

    const std::string& pathToCurrentEntry = absl::StrCat(pathPrefix, jsonPath);

    // Only add, if the predicate returns true.
    if (std::invoke(predicate, hashMapEntry)) {
      allHashMapEntry.emplace_back(pathToCurrentEntry, hashMapEntry);
    }

    // Recursively add, if we have a sub manager.
    if (hashMapEntry.holdsSubManager()) {
      /*
      Move the recursive results.
      Note: There is no use in calling with `sortByInitialization` set to true.
      If there was a config option 'optionA' created, after creating this sub
      manager and before adding configuration options to this sub manager, then
      the configuration options of the sub manager would be added to
      `allHashMapEntries` before 'optionA'.
      Which is no the creation order.
      */
      auto recursiveResults = allHashMapEntries(
          hashMapEntry.getSubManager().value()->configurationOptions_, false,
          pathToCurrentEntry, predicate);
      allHashMapEntry.reserve(recursiveResults.size());
      std::ranges::move(recursiveResults, std::back_inserter(allHashMapEntry));
    }
  };

  // Collect all the entries in the given `entries`.
  std::ranges::for_each(entries, addHashMapEntryToCollectedOptions,
                        verifyEntry);

  // Sort the collected `HashMapEntry`s, if wanted.
  if (sortByInitialization) {
    std::ranges::sort(allHashMapEntry, {}, [](const auto& pair) {
      const auto& [jsonPath, hashMapEntry] = pair;
      return hashMapEntry.get().getInitializationId();
    });
  }

  return ad_utility::transform(std::move(allHashMapEntry), [](auto&& pair) {
    return std::pair<std::string, const ConfigManager::HashMapEntry&>(
        AD_FWD(pair).first, pair.second.get());
  });
}

// ____________________________________________________________________________
template <typename ReturnReference>
requires std::same_as<ReturnReference, ConfigOption&> ||
         std::same_as<ReturnReference, const ConfigOption&>
std::vector<std::pair<std::string, ReturnReference>>
ConfigManager::configurationOptionsImpl(
    const ad_utility::HashMap<std::string, HashMapEntry>& configurationOptions,
    const bool sortByInitialization) {
  return ad_utility::transform(
      allHashMapEntries(
          configurationOptions, sortByInitialization, "",
          [](const HashMapEntry& entry) { return entry.holdsConfigOption(); }),
      [](auto&& pair) {
        return std::pair<std::string, ReturnReference>(
            AD_FWD(pair).first, *pair.second.getConfigOption().value());
      });
  ;
}

// ____________________________________________________________________________
std::vector<std::pair<std::string, ConfigOption&>>
ConfigManager::configurationOptions(const bool sortByInitialization) {
  return configurationOptionsImpl<ConfigOption&>(configurationOptions_,
                                                 sortByInitialization);
}

// ____________________________________________________________________________
std::vector<std::pair<std::string, const ConfigOption&>>
ConfigManager::configurationOptions(const bool sortByInitialization) const {
  return configurationOptionsImpl<const ConfigOption&>(configurationOptions_,
                                                       sortByInitialization);
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

        /*
        Returns true, iff `prefix` describes a json pointer, that is a prefix
        of the json pointer described with `jsonPointerString` and not equal
        to `jsonPointerString`.
        */
        auto isTrueJsonPointerPrefix = [](std::string_view jsonPointerString,
                                          std::string_view prefix) {
          /*
          We don't want a prefix in string terms, but in json pointer terms.

          The general json pointer syntax is `/x1/x2/x3/.../xN`, with all `x`
          valid strings, or natural numbers, in json and `N` a natural number.
          We define a true prefix as `/y1/y2/y3/.../yU`, with `U` a natural
          number, `U <= N` and `x1 = y1, x2 = y2, ..., xU = yU`. (The grammar of
          json pointer is a bit more complicated in reality, but this is enough
          to understand the problem.)

          Now, this IS different from a normal string prefix, because it
          requires `xi = yi`, for `all i in [0, U]`, to be EQUAL. A string
          prefix has weaker requirements, because it only requires `xi = yi`,
          for `all i in [0, U - 1]`, and for `yU` to be a string prefix of `xU`.
          Example: The json pointer `some/option` is not a prefix of
          `some/options/optionA` in json pointer terms, but in string terms,
          because `"option"` is a prefix of `"options"`.

          This can be fixed, by adding the seperator `/` at the end of both
          string representation. Because the symbol `/` is not allowed in `xi`,
          for `any i in [0, N]`, but must be between them, it forces all the
          `xi` and `yi` to be equal.

          Now, we already covered the equality case, so we only need to add the
          `/` to the (maybe) prefix, for it to work right.
          */
          return jsonPointerString.starts_with(absl::StrCat(prefix, "/"));
        };

        // Is the new path a prefix of the path of an already
        // existing path?
        if (isTrueJsonPointerPrefix(alreadyAddedPath,
                                    pathAsJsonPointerString)) {
          throw std::runtime_error(absl::StrCat(
              "Key error: The given path '", vectorOfKeysForJsonToString(path),
              "' is a prefix of a path, '", alreadyAddedPath,
              "', that is already in use.", "'\n", printConfigurationDoc(true),
              "\n"));
        }

        // Is the already existing path a prefix of the new path?
        if (isTrueJsonPointerPrefix(pathAsJsonPointerString,
                                    alreadyAddedPath)) {
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
  return *(configurationOptions_.insert({createJsonPointerString(pathToOption),
                                         HashMapEntry(std::move(option))}))
              .first->second.getConfigOption()
              .value();
}

// ____________________________________________________________________________
ConfigManager& ConfigManager::addSubManager(
    const std::vector<std::string>& path) {
  // Is the path valid?
  verifyPath(path);

  // The path in json format.
  const std::string jsonPath = createJsonPointerString(path);

  // Add the configuration manager and return the inserted element.
  return *(configurationOptions_.emplace(jsonPath,
                                         HashMapEntry(ConfigManager{})))
              .first->second.getSubManager()
              .value();
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
  const auto allConfigOptions{[this]() {
    std::vector<std::pair<std::string, ConfigOption&>> allConfigOption =
        configurationOptions(false);

    // `absl::flat_hash_map` doesn't allow the values to be l-value references,
    // but `std::unordered_map` does.
    return std::unordered_map<std::string, ConfigOption&>(
        allConfigOption.begin(), allConfigOption.end());
  }()};

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
          [&allConfigOptions](const nlohmann::json::json_pointer& ptr) {
            return allConfigOptions.contains(ptr.to_string());
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
  for (auto&& [key, option] : allConfigOptions) {
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
  const std::vector<std::pair<std::string, const ConfigOption&>>
      allConfigOptions = configurationOptions(true);

  // Handeling, for when there are no configuration options.
  if (allConfigOptions.empty()) {
    return "No configuration options were defined.";
  }

  /*
  Setup for printing the locations of the option in json format, so that
  people can easier understand, where everything is.
  Note: This json remembers the insertion order.
  */
  nlohmann::ordered_json configuratioOptionsVisualization;

  /*
  Add the paths of `configOptions_` and have them point to either:
  - The current value of the configuration option.
  - A "value was never initialized", if we are not sure, the current value of
    the configuration option was initialized.
  - The default value of the configuration option.
  - An example value, of the correct type.
  */
  for (const auto& [path, option] : allConfigOptions) {
    /*
    Pointer to the position of this option in
    `configuratioOptionsVisualization`.
    */
    const nlohmann::ordered_json::json_pointer jsonOptionPointer{path};

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
      std::views::transform(allConfigOptions,
                            [](const auto& pair) {
                              // Add the location of the option and the option
                              // itself.
                              return absl::StrCat(
                                  "Location : ", pair.first, "\n",
                                  static_cast<std::string>(pair.second));
                            }),
      "\n\n");

  // List of the validators.
  const std::string& listOfAllValidators = ad_utility::lazyStrJoin(
      ad_utility::transform(validators(true),
                            [](const ConfigOptionValidatorManager& validator) {
                              return absl::StrCat("- ",
                                                  validator.getDescription());
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
      "\n\nRequired invariants of the configuration options:",
      listOfAllValidators.empty()
          ? " None."
          : absl::StrCat(
                "\n", ad_utility::addIndentation(listOfAllValidators, "    ")),
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
  const std::vector<std::pair<std::string, const ConfigOption&>>
      allConfigOptions = configurationOptions(true);

  // For only looking at the configuration options in our map.
  auto onlyConfigurationOptionsView = std::views::values(allConfigOptions);

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
std::vector<std::reference_wrapper<const ConfigOptionValidatorManager>>
ConfigManager::validators(const bool sortByInitialization) const {
  // For the collected validators. Initialized with the validators inside this
  // manager.
  std::vector<std::reference_wrapper<const ConfigOptionValidatorManager>>
      allValidators(ad_utility::transform(
          validators_, [](const auto& val) { return std::cref(val); }));

  // Collect the validators from the sub managers.
  std::vector<std::pair<std::string, const ConfigManager::HashMapEntry&>>
      allSubManager{allHashMapEntries(
          configurationOptions_, false, "",
          [](const HashMapEntry& entry) { return entry.holdsSubManager(); })};
  std::ranges::for_each(
      std::views::values(allSubManager),
      [&allValidators](const ConfigManager::HashMapEntry& entry) {
        appendVector(allValidators,
                     entry.getSubManager().value()->validators(false));
      });

  // Sort the validators, if wanted.
  if (sortByInitialization) {
    std::ranges::sort(allValidators, {},
                      [](const ConfigOptionValidatorManager& validator) {
                        return validator.getInitializationId();
                      });
  }

  return allValidators;
}

// ____________________________________________________________________________
void ConfigManager::verifyWithValidators() const {
  std::ranges::for_each(validators(false), [](auto& validator) {
    validator.get().checkValidator();
  });
};

// ____________________________________________________________________________
bool ConfigManager::containsOption(const ConfigOption& opt) const {
  const auto allOptions = configurationOptions(false);
  return ad_utility::contains(
      std::views::values(allOptions) |
          std::views::transform(
              [](const ConfigOption& option) { return &option; }),
      &opt);
}

}  // namespace ad_utility
