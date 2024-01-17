//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

/*
Convenience header for Nlohmann::Json that sets the default options. Also
 adds support for `std::optional` and `std::variant`.
*/

#ifndef QLEVER_JSON_H
#define QLEVER_JSON_H

#ifndef EOF
#define EOF std::char_traits<char>::eof()
#endif

// Disallow implicit conversions from nlohmann::json to other types,
// most notably to std::string.
#include <stdexcept>

#include "util/File.h"
#define JSON_USE_IMPLICIT_CONVERSIONS 0

#include <absl/strings/str_cat.h>

#include <concepts>
#include <memory>
#include <nlohmann/json.hpp>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>

#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/SourceLocation.h"

/*
@brief Read the specified json file and build a json object out of it.

@param jsonFileName Name of the file, or path to the file. Must describe a
`.json` file.
*/
inline nlohmann::json fileToJson(std::string_view jsonFileName) {
  // Check, if the filename/-path ends with ".json". Checking, if it's a valid
  // file, is done by `ad_utility::makeIfstream`.
  if (!jsonFileName.ends_with(".json")) {
    throw std::runtime_error(absl::StrCat(
        "The given filename/-path '", jsonFileName,
        "' doesn't end with '.json'. Therefore, it can't be a json file."));
  }

  try {
    return nlohmann::json::parse(ad_utility::makeIfstream(jsonFileName));
  } catch (const nlohmann::json::exception& e) {
    throw std::runtime_error(absl::StrCat(
        "The contents of the file ", jsonFileName,
        " could not be parsed as JSON. The error was: ", e.what()));
  }
}

/*
@brief Returns the string representation of the type of the given
`nlohmann::json`. Only supports official json types.
*/
inline std::string jsonToTypeString(const nlohmann::json& j) {
  if (j.is_array()) {
    return "array";
  } else if (j.is_boolean()) {
    return "boolean";
  } else if (j.is_null()) {
    return "null";
  } else if (j.is_number()) {
    return "number";
  } else if (j.is_object()) {
    return "object";
  } else {
    AD_CONTRACT_CHECK(j.is_string());
    return "string";
  }
}

/*
Added support for serializing `std::optional` using `nlohmann::json`.
The serialized format of a `std::optional<T>` is simply the serialized format
of the contained value of type `T`, if `std::optional<T>` contains a value.
In the case, that `std::optional<T>` contains no value, it is serialized as a
`nullptr`.

Note: In the case, that the contained value of `std::optional` converts to a
`nullptr` in the json representation, it will not be possible to distinguish
between the json representation of a `std::optional`, that contains this
value, and the json representation of a `std::optional`, that doesn't
contain a value. Because of this, the deserialization of `std::optional`
in such cases will always return a `std::optional`, that contains no value.
*/
namespace nlohmann {
template <typename T>
struct adl_serializer<std::optional<T>> {
  static void to_json(nlohmann::json& j, const std::optional<T>& opt) {
    if (opt.has_value()) {
      j = opt.value();
    } else {
      j = nullptr;
    }
  }

  static void from_json(const nlohmann::json& j, std::optional<T>& opt) {
    if (j.is_null()) {
      opt = std::nullopt;
    } else {
      opt = j.get<T>();
    }
  }
};
}  // namespace nlohmann

// Added support for serializing `std::monostate` using `nlohmann::json`.
// This is needed to serialize `std::variant`.
namespace nlohmann {
template <>
struct adl_serializer<std::monostate> {
  static void to_json(nlohmann::json& j, const std::monostate&) {
    // Monostate is just an empty placeholder value.
    j = nullptr;
  }

  static void from_json(const nlohmann::json& j, const std::monostate&) {
    /*
    Monostate holds no values, so the given monostate is already identical to
    the serialized one.
    However, because of this, there also must be an error/mistake, if anybody
    tries to interpret an actual value as a `std::monostate`.
    */
    if (!j.is_null()) {
      throw nlohmann::json::type_error::create(
          302,
          absl::StrCat("Custom type converter (see `",
                       ad_utility::source_location::current().file_name(),
                       "`) from json",
                       " to `std::monostate`: type must be null, but wasn't."),
          nullptr);
    }
  }
};
}  // namespace nlohmann

/*
Added support for serializing `std::variant` using `nlohmann::json`.
The serialized format for `std::variant<Type0, Type1, ...>` is a json string
with the json object literal keys `index` and `value`.
`index` is the number of the type, that the `std::variant<Type0, Type1, ...>`
contains the value of. `value` is the value, that the `std::variant` contains.
Example: The serialized format for a `std::variant<int, float>` containing a
`3.5` would be `{"index":1, "value":3.5}`
*/
namespace nlohmann {
template <typename... Types>
struct adl_serializer<std::variant<Types...>> {
  static void to_json(nlohmann::json& j, const std::variant<Types...>& var) {
    // We need to save, which of the types the std::variant actually
    // uses.
    j["index"] = var.index();

    /*
    This is needed, because `var` is only known at compile time. And by
    extension, so is `var.index()`, which means we can't simply assign it
    and have to go over `std::visit`.
    */
    std::visit([&j](const auto& value) { j["value"] = value; }, var);
  }

  static void from_json(const nlohmann::json& j, std::variant<Types...>& var) {
    // Which of the `sizeof...(Types)` possible value types, was the
    // serialized std::variant using?
    size_t index = j["index"].get<size_t>();

    // Quick check, if the index is even a possible value.
    if (index >= sizeof...(Types)) {
      throw nlohmann::json::out_of_range::create(
          401,
          absl::StrCat("The given index ", index,
                       " for a std::variant was"
                       " out of range, because the biggest possible index was ",
                       sizeof...(Types) - 1, "."),
          nullptr);
    }

    // Interpreting the value based on its type.
    ad_utility::RuntimeValueToCompileTimeValue<
        sizeof...(Types) - 1>(index, [&j, &var]<size_t Index>() {
      var =
          j["value"]
              .get<std::variant_alternative_t<Index, std::variant<Types...>>>();
    });
  }
};
}  // namespace nlohmann

/*
Added support for serializing `std::unique_ptr` using `nlohmann::json`.
The serialized format for `std::variant<Type0, Type1, ...>` is a json string
with the json object literal keys `index` and `value`.
*/
namespace nlohmann {
template <typename T>
requires std::is_copy_constructible_v<T>
struct adl_serializer<std::unique_ptr<T>> {
  static void to_json(nlohmann::json& j, const std::unique_ptr<T>& ptr) {
    // Does the `unique_ptr` hold anything? If yes, save the dereferenced
    // object, if no, save a `nullptr`.
    if (ptr) {
      j = *ptr;
    } else {
      j = nullptr;
    }
  }

  static void from_json(const nlohmann::json& j, std::unique_ptr<T>& ptr) {
    if (j.is_null()) {
      // If `json` is null, we just delete the content of ptr, because it
      // should be an empty `unique_ptr`.
      ptr = nullptr;
    } else {
      ptr = std::make_unique<T>(j.get<T>());
    }
  }
};
}  // namespace nlohmann

#endif  // QLEVER_JSON_H
