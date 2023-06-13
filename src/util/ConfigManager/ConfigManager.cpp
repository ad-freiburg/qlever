// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include <ANTLRInputStream.h>
#include <CommonTokenStream.h>
#include <absl/strings/str_cat.h>
#include <antlr4-runtime.h>

#include <iostream>
#include <regex>
#include <sstream>
#include <string>

#include "util/ConfigManager/ConfigExceptions.h"
#include "util/ConfigManager/ConfigManager.h"
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
nlohmann::json::json_pointer ConfigManager::createJsonPointer(
    const VectorOfKeysForJson& keys) {
  /*
  A json pointer needs special characters, if a `/`, or `~`, is used in a
  key. So here a special conversion function for our keys, that adds those,
  if needed.
  */
  auto toStringVisitor = []<typename T>(const T& key) {
    // Our transformed key.
    std::string transformedKey;

    /*
    Transforming the key. We simply check through the way, we can convert
    them into a string and do the one, that works first.
    */
    if constexpr (isString<std::decay_t<T>>) {
      transformedKey = std::string(key);
    } else {
      /*
      Must have been a number. I mean, `KeyForJson` doesn't allow anything
      else, than those 2 possibilities and this the the last one.
      */
      static_assert(std::integral<std::decay_t<T>>);
      transformedKey = std::to_string(key);
    }

    // Replace special character `~` with `~0` and `/` with `~1`.
    return absl::StrReplaceAll(transformedKey, {{"~", "~0"}, {"/", "~1"}});
  };
  auto toString = [&toStringVisitor](const KeyForJson& key) {
    return std::visit(toStringVisitor, key);
  };

  // Creating the string for the pointer.
  std::ostringstream pointerString;
  std::ranges::for_each(keys,
                        [&toString, &pointerString](const KeyForJson& key) {
                          pointerString << "/" << toString(key);
                        });

  return nlohmann::json::json_pointer(pointerString.str());
}

// ____________________________________________________________________________
void ConfigManager::addConfigOption(const VectorOfKeysForJson& pathToOption,
                                    ConfigOption&& option) {
  // We need at least a name in the path.
  AD_CONTRACT_CHECK(pathToOption.size() > 0);

  /*
  The first key must be a string, not a number. Having an array at the
  highest level, would be bad practice, because setting and reading options,
  that are just identified with numbers, is rather difficult.
  */
  if (!std::holds_alternative<std::string>(pathToOption.front())) {
    throw ConfigManagerOptionPathDoesntStartWithStringException(
        vectorOfKeysForJsonToString(pathToOption));
  }

  /*
  The last entry in the path is the name of the configuration option. If it
  isn't, something has gone wrong.
  */
  if (!std::holds_alternative<std::string>(pathToOption.back())) {
    throw ConfigManagerOptionPathDoesntEndWithStringException(
        vectorOfKeysForJsonToString(pathToOption));
  } else if (std::get<std::string>(pathToOption.back()) !=
             option.getIdentifier()) {
    throw ConfigManagerPathToConfigOptionDoesntEndWithConfigOptionNameException(
        option.getIdentifier(), vectorOfKeysForJsonToString(pathToOption));
  }

  /*
  The string keys must be a valid `NAME` in the short hand. Otherwise, the
  option can't get accessed with the short hand.
  */
  auto checkIfValidNameVisitor = []<typename T>(const T& key) {
    // Only actually check, if we have a string.
    if constexpr (isString<std::decay_t<T>>) {
      return isNameInShortHand(AD_FWD(key));
    } else {
      return true;
    }
  };

  if (auto failedKey = std::ranges::find_if_not(
          pathToOption,
          [&checkIfValidNameVisitor](const KeyForJson& key) {
            return std::visit(checkIfValidNameVisitor, key);
          });
      failedKey != pathToOption.end()) {
    /*
    One of the keys failed. `failedKey` is an iterator pointing to the key.
    */
    throw NotValidShortHandNameException(
        std::get<std::string>(*failedKey),
        vectorOfKeysForJsonToString(pathToOption));
  }

  // The position in the json object literal, our `pathToOption` points to.
  const nlohmann::json::json_pointer ptr{createJsonPointer(pathToOption)};

  // Is there already a configuration option with the same identifier at the
  // same location?
  if (keyToConfigurationOptionIndex_.contains(ptr)) {
    throw ConfigManagerOptionPathAlreadyinUseException(
        vectorOfKeysForJsonToString(pathToOption), printConfigurationDoc());
  }

  // Add the location of the configuration option to the json.
  keyToConfigurationOptionIndex_[ptr] = configurationOptions_.size();
  // Add the configuration option.
  configurationOptions_.push_back(std::move(option));
}

// ____________________________________________________________________________
const std::vector<ConfigOption>& ConfigManager::getConfigurationOptions()
    const {
  return configurationOptions_;
}

// ____________________________________________________________________________
const ConfigOption& ConfigManager::getConfigurationOptionByNestedKeys(
    const VectorOfKeysForJson& keys) const {
  // If there is an entry at the described location, this should point to the
  // index number of the configuration option in `configurationOptions_`.
  const nlohmann::json::json_pointer ptr{createJsonPointer(keys)};

  if (keyToConfigurationOptionIndex_.contains(ptr)) {
    return configurationOptions_.at(
        keyToConfigurationOptionIndex_.at(ptr).get<size_t>());
  } else {
    throw NoConfigOptionFoundException(vectorOfKeysForJsonToString(keys),
                                       printConfigurationDoc());
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
  According to the documentation, when iterating over the entries of a
  `nlohmann::json` object using `items()`, there can be error/problems, IF the
  life time of the object, on which one called `items()` on, doesn't exceeds the
  life time of the iteration. (See the documentation for more information.)

  Because we iterate over the 'flattend' verions of two `nlohmann::json`
  objects, I gave them const references here.
  */
  const auto& jFlattend = j.flatten();
  const auto& keyToConfigurationOptionIndexFlattend =
      keyToConfigurationOptionIndex_.flatten();

  // We can skip the following check, if `j` is empty.
  if (!j.empty()) {
    /*
    Does `j` only contain valid configuration options? That is, does it only
    contain paths to entries, that are the same paths as we have saved there?

    For example: If on of our paths in `keyToConfigurationOptionIndex_` was
    `{"classA": [..., {"entryNumber5" : 5}]}`, then a path like `{"clasA": [...,
    {"entryNumber5" : 5}]}` would be invalid, because of the typo.
    */
    for (const auto& item : jFlattend.items()) {
      // Only returns true, if the given pointer is the EXACT path to a
      // configuration option. Partial doesn't count!
      auto isPointerToConfigurationOption =
          [this](const nlohmann::json::json_pointer& ptr) {
            // We only have numbers at the end of paths to configuration
            // options.
            return keyToConfigurationOptionIndex_.contains(ptr) &&
                   keyToConfigurationOptionIndex_.at(ptr).is_number_integer();
          };

      /*
      Because a configuration option can only hold json literal primitives, or
      json literal arrays, we only have to to look at `currentPtr` and its
      father. If `currentPtr` points at a json literal primitive, then it's
      valid, if it's the exact path to a configuration option, or if its father
      is the exact path to a configuration option, and in the `stringAsJson` it
      points to an array.
      */
      const nlohmann::json::json_pointer currentPtr{item.key()};

      if ((!isPointerToConfigurationOption(currentPtr) ||
           !j.at(currentPtr).is_primitive()) &&
          (!isPointerToConfigurationOption(currentPtr.parent_pointer()) ||
           !j.at(currentPtr.parent_pointer()).is_array())) {
        throw j.at(currentPtr.parent_pointer()).is_array()
            ? NoConfigOptionFoundException(
                  currentPtr.parent_pointer().to_string(),
                  printConfigurationDoc())
            : NoConfigOptionFoundException(currentPtr.to_string(),
                                           printConfigurationDoc());
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

    ConfigOption* configurationOption = &(configurationOptions_.at(
        keyToConfigurationOptionIndex_.at(configurationOptionJsonPosition)
            .get<size_t>()));

    // Set the option, if possible.
    if (j.contains(configurationOptionJsonPosition)) {
      // This will throw an exception, if the json object can't be interpreted
      // with the wanted type.
      configurationOption->setValueWithJson(
          j.at(configurationOptionJsonPosition));
    }

    // If the option has no value now, that means, it didn't have a default
    // value, and needs to be set by the user at runtime, but wasn't.
    if (!configurationOption->hasValue()) {
      throw ConfigOptionWasntSetException(
          configurationOptionJsonPosition.to_string());
    }
  }
}

// ____________________________________________________________________________
std::string ConfigManager::printConfigurationDoc() const {
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
  // people can easier understand, where everything is.
  nlohmann::json prettyKeyToConfigurationOptionIndex(
      keyToConfigurationOptionIndex_);

  /*
  Replace the numbers in the 'leaves' of the 'tree' with the default value of
  the option, or a random value of that type, if it doesn't have a
  default value.
  Note: Users can indirectly create null values in
  `keyToConfigurationOptionIndex_`, by adding a configuration option with a path
  containing numbers, for arrays accesses, that are bigger than zero. Those
  indirectly declared arrays, will always be created/modified in such a way,
  that the used index numbers are valid. Which means creating empty array
  entries, if the numbers are bigger than `0` and the arrays don't already have
  entries in all positions lower than the numbers.
  Example: A configuration option with the path `"options", 3`, would create 3
  empty array entries in `"options"`.
  We will simply ignore those `null` entries, because they are signifiers, that
  the user didn't think things through and should re-work some stuff. I mean,
  there is never a good reason, to have empty array elements.
  */
  for (const auto& keyToLeaf : flattendKeyToConfigurationOptionIndex.items()) {
    // Skip empty array 'leafs'.
    if (keyToLeaf.value().is_null()) {
      continue;
    }

    // Pointer to the position of this option in
    // `prettyKeyToConfigurationOptionIndex`.
    const nlohmann::json::json_pointer jsonOptionPointer{keyToLeaf.key()};
    // What configuration option are we currently, indirectly, looking at?
    const ConfigOption& option =
        configurationOptions_.at(keyToLeaf.value().get<size_t>());

    prettyKeyToConfigurationOptionIndex.at(jsonOptionPointer) =
        option.hasDefaultValue() ? option.getDefaultValueAsJson()
                                 : option.getDummyValueAsJson();
  }

  // List the configuration options themselves.
  for (const auto& keyValuePair :
       flattendKeyToConfigurationOptionIndex.items()) {
    // Skip empty array 'leafs'.
    if (keyValuePair.value().is_null()) {
      continue;
    }

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
      "Locations of available configuration options with example "
      "values:\n",
      ad_utility::addIndentation(prettyKeyToConfigurationOptionIndex.dump(2),
                                 1),
      "\n\nAvailable configuration options:\n",
      ad_utility::addIndentation(stream.str(), 1));
}

// ____________________________________________________________________________
std::string ConfigManager::vectorOfKeysForJsonToString(
    const VectorOfKeysForJson& keys) {
  std::ostringstream keysToString;
  std::ranges::for_each(keys, [&keysToString](const auto& key) {
    std::visit(
        [&keysToString](const auto& k) { keysToString << "[" << k << "]"; },
        key);
  });
  return keysToString.str();
}
}  // namespace ad_utility
