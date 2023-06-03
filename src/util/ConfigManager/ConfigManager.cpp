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

#include "util/ConfigManager/ConfigManager.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConfigManager/ConfigShorthandVisitor.h"
#include "util/ConfigManager/generated/ConfigShorthandLexer.h"
#include "util/ConfigManager/generated/ConfigShorthandParser.h"
#include "util/Exception.h"
#include "util/StringUtils.h"
#include "util/antlr/ANTLRErrorHandling.h"
#include "util/json.h"

namespace ad_utility {

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
    throw ad_utility::Exception(
        absl::StrCat("Key error: There was no configuration option found at '",
                     ptr.to_string(), "'\n", printConfigurationDoc(), "\n"));
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
    throw ad_utility::Exception(
        absl::StrCat("A ConfigManager should only parse configurations, that "
                     "are a json object literal. The configuration: \n\n",
                     j.dump(2), "\n\n is not a json object literal, but a '",
                     jsonToTypeString(j), "'."));
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
          // We only have numbers at the end of paths to configuration options.
          return keyToConfigurationOptionIndex_.contains(ptr) &&
                 keyToConfigurationOptionIndex_.at(ptr).is_number_integer();
        };

    /*
    Because a configuration option can only hold json literal primitives, or
    json literal arrays, we only have to to look at `currentPtr` and its father.
    If `currentPtr` points at a json literal primitive, then it's valid, if
    it's the exact path to a configuration option, or if its father is the exact
    path to a configuration option, and in the `stringAsJson` it points to an
    array.
    */
    const nlohmann::json::json_pointer currentPtr{item.key()};

    if ((!isPointerToConfigurationOption(currentPtr) ||
         !j.at(currentPtr).is_primitive()) &&
        (!isPointerToConfigurationOption(currentPtr.parent_pointer()) ||
         !j.at(currentPtr.parent_pointer()).is_array())) {
      throw ad_utility::Exception(absl::StrCat(
          "Error while trying to set configuration option: '",
          currentPtr.to_string(), "'",
          currentPtr.parent_pointer().empty()
              ? " doesn't"
              : absl::StrCat(" and '", currentPtr.parent_pointer().to_string(),
                             "' both don't"),
          " point to a valid configuration option.\n", printConfigurationDoc(),
          "\n"));
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
      throw ad_utility::Exception(
          absl::StrCat("Error while trying to set configuration options: The "
                       "configuration option at '",
                       configurationOptionJsonPosition.to_string(),
                       "' wasn't defined by the user, even though, this "
                       "configuration option has no default value.\n",
                       printConfigurationDoc(), "\n"));
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

    if (option.hasDefaultValue()) {
      option.visitDefaultValue(
          [&jsonOptionPointer,
           &prettyKeyToConfigurationOptionIndex](const auto& defaultValue) {
            prettyKeyToConfigurationOptionIndex.at(jsonOptionPointer) =
                defaultValue.value();
          });
    } else {
      option.visitDefaultValue(
          [&jsonOptionPointer,
           &prettyKeyToConfigurationOptionIndex]<typename T>(const T&) {
            if constexpr (std::is_same_v<typename T::value_type, bool>) {
              prettyKeyToConfigurationOptionIndex.at(jsonOptionPointer) = false;
            } else if constexpr (std::is_same_v<typename T::value_type,
                                                std::string>) {
              prettyKeyToConfigurationOptionIndex.at(jsonOptionPointer) =
                  "Example string";
            } else if constexpr (std::is_same_v<typename T::value_type, int>) {
              prettyKeyToConfigurationOptionIndex.at(jsonOptionPointer) = 42;
            } else if constexpr (std::is_same_v<typename T::value_type,
                                                float>) {
              prettyKeyToConfigurationOptionIndex.at(jsonOptionPointer) = 4.2;
            } else if constexpr (std::is_same_v<typename T::value_type,
                                                std::vector<bool>>) {
              prettyKeyToConfigurationOptionIndex.at(jsonOptionPointer) =
                  std::vector{true, false};
            } else if constexpr (std::is_same_v<typename T::value_type,
                                                std::vector<std::string>>) {
              prettyKeyToConfigurationOptionIndex.at(jsonOptionPointer) =
                  std::vector{"Example", "string", "list"};
            } else if constexpr (std::is_same_v<typename T::value_type,
                                                std::vector<int>>) {
              prettyKeyToConfigurationOptionIndex.at(jsonOptionPointer) =
                  std::vector{40, 41, 42};
            } else if constexpr (std::is_same_v<typename T::value_type,
                                                std::vector<float>>) {
              prettyKeyToConfigurationOptionIndex.at(jsonOptionPointer) =
                  std::vector{40.0, 41.1, 42.2};
            }
          });
    }
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
}  // namespace ad_utility
