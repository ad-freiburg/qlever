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
template <typename Visitor>
requires ad_utility::InvocableWithExactReturnType<
             Visitor, void, std::string_view, ConfigManager&> &&
         ad_utility::InvocableWithExactReturnType<
             Visitor, void, std::string_view, ConfigOption&>
void ConfigManager::visitHashMapEntries(Visitor&& vis, bool sortByCreationOrder,
                                        std::string_view pathPrefix) const {
  // `std::reference_wrapper` works with `std::ranges::sort`. `const
  // HashMapEntry&` does not.
  auto transformedHashMap =
      std::views::transform(configurationOptions_, [&pathPrefix](auto& pair) {
        const auto& [jsonPath, hashMapEntry] = pair;
        // Check the hash map entry, before doing anything.
        verifyHashMapEntry(absl::StrCat(pathPrefix, jsonPath), hashMapEntry);
        return std::pair<std::string_view,
                         std::reference_wrapper<const HashMapEntry>>(
            jsonPath, hashMapEntry);
      });
  std::vector<
      std::pair<std::string_view, std::reference_wrapper<const HashMapEntry>>>
      hashMapEntries(transformedHashMap.begin(), transformedHashMap.end());

  // Sort the collected `HashMapEntry`s, if wanted.
  if (sortByCreationOrder) {
    std::ranges::sort(hashMapEntries, {}, [](const auto& pair) {
      const HashMapEntry& hashMapEntry = pair.second;
      return hashMapEntry.getInitializationId();
    });
  }

  // Call a wrapper for `vis` with the `HashMapEntry::visit` of every entry.
  std::ranges::for_each(hashMapEntries, [&vis](auto& pair) {
    auto& [jsonPath, hashMapEntry] = pair;
    hashMapEntry.get().visit(
        [&jsonPath, &vis](auto& data) { std::invoke(vis, jsonPath, data); });
  });
}

// ____________________________________________________________________________
template <ad_utility::InvocableWithExactReturnType<
    bool, const ConfigManager::HashMapEntry&>
              Predicate>
std::vector<std::pair<std::string, const ConfigManager::HashMapEntry&>>
ConfigManager::allHashMapEntries(std::string_view pathPrefix,
                                 const Predicate& predicate) const {
  std::vector<std::pair<std::string, const ConfigManager::HashMapEntry&>>
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
      // Move the recursive results.
      auto recursiveResults =
          hashMapEntry.getSubManager().value()->allHashMapEntries(
              pathToCurrentEntry, predicate);
      allHashMapEntry.reserve(recursiveResults.size());
      std::ranges::move(std::move(recursiveResults),
                        std::back_inserter(allHashMapEntry));
    }
  };

  // Collect all the entries in the given `entries`.
  std::ranges::for_each(configurationOptions_,
                        addHashMapEntryToCollectedOptions, verifyEntry);

  return allHashMapEntry;
}

// ____________________________________________________________________________
std::vector<std::pair<std::string, ConfigOption&>>
ConfigManager::configurationOptions() const {
  return ad_utility::transform(
      allHashMapEntries(
          "",
          [](const HashMapEntry& entry) { return entry.holdsConfigOption(); }),
      [](auto&& pair) {
        return std::pair<std::string, ConfigOption&>(
            AD_FWD(pair).first, *pair.second.getConfigOption().value());
      });
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
        configurationOptions();

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
nlohmann::ordered_json ConfigManager::generateConfigurationDocJson(
    std::string_view pathPrefix) const {
  nlohmann::ordered_json configurationDocJson;

  visitHashMapEntries(
      [&configurationDocJson, &pathPrefix]<typename T>(std::string_view path,
                                                       T& optionOrSubManager) {
        /*
        Pointer to the position of this option, or sub manager, in
        `configurationDocJson`.
        */
        const nlohmann::ordered_json::json_pointer jsonPointer{
            std::string{path}};

        // Either add the option, or recursively add the sub manager.
        if constexpr (isSimilar<T, ConfigOption>) {
          /*
          Add the paths of `configOptions_` and have them point to either:
          - The current value of the configuration option. Which is the default
          value, if the option was never set and has a default value.
          - A "[must be specified]", if we are not sure, the current
          value of the configuration option was initialized.
          */
          configurationDocJson[jsonPointer] =
              optionOrSubManager.wasSet() ? optionOrSubManager.getValueAsJson()
                                          : "[must be specified]";
        } else if constexpr (isSimilar<T, ConfigManager>) {
          // `visitHashMapEntries` already checks, that sub manager aren't
          // empty.
          configurationDocJson[jsonPointer] =
              optionOrSubManager.generateConfigurationDocJson(
                  absl::StrCat(pathPrefix, path));
        } else {
          AD_FAIL();
        }
      },
      true, pathPrefix);

  return configurationDocJson;
}

// ____________________________________________________________________________
std::string ConfigManager::generateConfigurationDocDetailedList(
    std::string_view pathPrefix) const {
  // For collecting the string representations of the hash map entries.
  std::vector<std::string> stringRepresentations;
  stringRepresentations.reserve(configurationOptions_.size());

  visitHashMapEntries(
      [&pathPrefix, &stringRepresentations]<typename T>(std::string_view path,
                                                        T& optionOrSubManager) {
        // Getting rid of the first `/` for printing, based on user feedback.
        std::string_view adjustedPath = path.substr(1, path.length());

        // Either add the string representation of the option, or recursively
        // add the sub manager.
        if constexpr (isSimilar<T, ConfigOption>) {
          stringRepresentations.emplace_back(absl::StrCat(
              "Option '", adjustedPath, "' [",
              optionOrSubManager.getActualValueTypeAsString(), "]\n",
              static_cast<std::string>(optionOrSubManager)));
        } else if constexpr (isSimilar<T, ConfigManager>) {
          stringRepresentations.emplace_back(absl::StrCat(
              "Sub manager '", adjustedPath, "'\n",
              ad_utility::addIndentation(
                  optionOrSubManager.generateConfigurationDocDetailedList(
                      absl::StrCat(pathPrefix, path)),
                  "    ")));
        } else {
          AD_FAIL();
        }
      },
      true, pathPrefix);

  /*
  List of local validators. Validators inside sub managers can be found
  there in the string representation.
  Also note, that `validators_` is always sorted by their creation order.
  */
  AD_CORRECTNESS_CHECK(std::ranges::is_sorted(
      validators_, {}, [](const ConfigOptionValidatorManager& validator) {
        return validator.getInitializationId();
      }));
  const std::string& listValidators = ad_utility::lazyStrJoin(
      ad_utility::transform(validators_,
                            [](const ConfigOptionValidatorManager& validator) {
                              return absl::StrCat("- ",
                                                  validator.getDescription());
                            }),
      "\n");

  return absl::StrCat(ad_utility::lazyStrJoin(stringRepresentations, "\n\n"),
                      "\n\nRequired invariants:",
                      listValidators.empty()
                          ? " None."
                          : absl::StrCat("\n", ad_utility::addIndentation(
                                                   listValidators, "    ")));
}

// ____________________________________________________________________________
std::string ConfigManager::printConfigurationDoc(bool detailed) const {
  /*
  This works, because sub managers are not allowed to be empty. (This invariant
  is checked by the helper function for walking over the hash map entries, that
  is used by the `generateConfigurationDoc...` helper functions.)
  So, the only way for a valid lack of configuration options to be true, is on
  the top level. A.k.a. the object, on which `printConfigurationDoc` was called.
  */
  if (configurationOptions_.empty()) {
    return "No configuration options were defined.";
  }
  return absl::StrCat(
      "Configuration:\n", generateConfigurationDocJson("").dump(2),
      detailed ? absl::StrCat("\n\n", generateConfigurationDocDetailedList(""))
               : "");
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
std::vector<std::reference_wrapper<const ConfigOptionValidatorManager>>
ConfigManager::validators() const {
  // For the collected validators. Initialized with the validators inside this
  // manager.
  std::vector<std::reference_wrapper<const ConfigOptionValidatorManager>>
      allValidators(ad_utility::transform(
          validators_, [](const auto& val) { return std::cref(val); }));

  // Collect the validators from the sub managers.
  std::vector<std::pair<std::string, const ConfigManager::HashMapEntry&>>
      allSubManager{allHashMapEntries("", [](const HashMapEntry& entry) {
        return entry.holdsSubManager();
      })};
  std::ranges::for_each(
      std::views::values(allSubManager),
      [&allValidators](const ConfigManager::HashMapEntry& entry) {
        appendVector(allValidators,
                     entry.getSubManager().value()->validators());
      });

  return allValidators;
}

// ____________________________________________________________________________
void ConfigManager::verifyWithValidators() const {
  std::ranges::for_each(
      validators(), [](auto& validator) { validator.get().checkValidator(); });
};

// ____________________________________________________________________________
bool ConfigManager::containsOption(const ConfigOption& opt) const {
  const auto allOptions = configurationOptions();
  return ad_utility::contains(
      std::views::values(allOptions) |
          std::views::transform(
              [](const ConfigOption& option) { return &option; }),
      &opt);
}

}  // namespace ad_utility
