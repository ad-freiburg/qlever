//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

/*
Convenience header for Nlohmann::Json that sets the default options. Also
 adds support for `std::optional` and `std::variant`.
*/

#ifndef QLEVER_JSON_H
#define QLEVER_JSON_H

// Disallow implicit conversions from nlohmann::json to other types,
// most notably to std::string.
#define JSON_USE_IMPLICIT_CONVERSIONS 0

#include <concepts>
#include <nlohmann/json.hpp>
#include <optional>
#include <utility>
#include <variant>

#include "util/DisableWarningsClang13.h"
#include "util/Exception.h"

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

  static void from_json(const nlohmann::json& j, std::monostate&) {
    /*
    Monostate holds no values, so the given monostate is already identical to
    the serialized one.
    However, because of this, there also must be an error/mistake, if anybody
    tries to interpret an actual value as a `std::monostate`.
    */
    if (!j.is_null()){
      // We have no ID (the `-1`), because this is custom. We also do not know
      // the actual position, where the error happend, so I just put `1` as a
      // placeholder.
      throw nlohmann::json::parse_error::create(-1, 1,
      "error while parsing value - any value other than nullptr can't"
      " be interpreted as a valid std::monostate.");
    }
  }
};
}  // namespace nlohmann

/*
 * @brief A compile time for loop, which passes the loop index to the
 *  given loop body.
 *
 * @tparam Function The loop body should be a templated function, with one
 *  size_t template argument and no more. It also shouldn't take any function
 *  arguments. Should be passed per deduction.
 * @tparam ForLoopIndexes The indexes, that the for loop goes over. Should be
 *  passed per deduction.
 *
 * @param loopBody The body of the for loop.
 */
template <typename Function, size_t... ForLoopIndexes>
void ConstExprForLoop(const std::index_sequence<ForLoopIndexes...>&,
                      const Function& loopBody) {
  ((loopBody.template operator()<ForLoopIndexes>()), ...);
}

/*
 * @brief 'Converts' a run time value of `size_t` to a compile time value and
 * then calls `function.template operator()<value>()`. `value < MaxValue` must
 * be true, else an exception is thrown. *
 *
 * @tparam MaxValue The maximal value, that the function parameter value could
 *  take.
 * @tparam Function The given function be a templated function, with one
 *  size_t template argument and no more. It also shouldn't take any function
 *  arguments. This parameter should be passed per deduction.
 *
 * @param value Value that you need as a compile time value.
 * @param function The templated function, which you wish to execute. Must be
 *  a function object (for example a lambda expression) that has an
 *  `operator()` which is templated on a single `size_t`.
 */
template <size_t MaxValue, typename Function>
void RuntimeValueToCompileTimeValue(const size_t& value,
                                    const Function& function) {
  AD_CONTRACT_CHECK(value <= MaxValue); // Is the value valid?
  ConstExprForLoop(std::make_index_sequence<MaxValue>{},
                   [&function, &value]<size_t Index>() {
                     if (Index == value) {
                       function.template operator()<Index>();
                     }
                   });
}

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
    std::visit([&j](const auto& value){j["value"] = value;}, var);
  }

  static void from_json(const nlohmann::json& j, std::variant<Types...>& var) {
    // Which of the `sizeof...(Types)` possible value types, was the
    // serialized std::variant using?
    size_t index = j["index"].get<size_t>();

    // Quick check, if the index is even a possible value.
    if (index >= sizeof...(Types)){
      throw nlohmann::json::out_of_range::create(401,
      "The given index for a std::variant was out of range.");
    }

    // Interpreting the value based on its type.
    DISABLE_WARNINGS_CLANG_13
    RuntimeValueToCompileTimeValue<sizeof...(
        Types) - 1>(index, [&j, &var]<size_t Index>() {
      ENABLE_WARNINGS_CLANG_13
      var =
          j["value"]
              .get<std::variant_alternative_t<Index, std::variant<Types...>>>();
    });
  }
};
}  // namespace nlohmann

#endif  // QLEVER_JSON_H
