//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

// Convenience header for Nlohmann::Json that sets the default options. Also
// adds support for std::optional.

#ifndef QLEVER_JSON_H
#define QLEVER_JSON_H

// Disallow implicit conversions from nlohmann::json to other types,
// most notably to std::string.
#define JSON_USE_IMPLICIT_CONVERSIONS 0

#include <nlohmann/json.hpp>
#include <optional>
#include <variant>
#include <utility>
#include <concepts>

#include "util/DisableWarningsClang13.h"

// Added support for serializing `std::optional` using `nlohmann::json`.
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
          if (j.is_null()){
            opt = std::nullopt;
          } else {
            opt = j.get<T>();
          }
        }
    };
}

// Added support for serializing `std::monostate` using `nlohmann::json`.
namespace nlohmann {
    template<>
    struct adl_serializer<std::monostate>{
        static void to_json(nlohmann::json& j,
            const std::monostate&) {
          // Monostate is just an empty placeholder value.
          j = nullptr;
        }

        static void from_json(const nlohmann::json&,
            std::monostate&) {
          // Monostate holds no values, so the given monostate is already
          // identical to the serialized one.
          // TODO Add an exception here, if the j doesn't contain a nullptr.
          // I mean, there is literally no sense in trying to convert any actual
          // value to a std::monostate, so there must be a bug, if somebody tries
          // to!
        }
    };
}

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
    const Function& loopBody){
  ((loopBody.template operator()<ForLoopIndexes>()), ...);
}

/*
 * @brief 'Converts' a run time value of size_t to a compile time value, as
 *  long as the maximal value of this value is known at compile time, and
 *  passes it to a given function. However, it doesn't really convert anything,
 *  but simply creates all templated version in `[0, MaxValue]` and only
 *  executes the one, where the template parameter and given run time value are
 *  the same.
 *
 * @tparam MaxValue The maximal value, that the function parameter value could
 *  take.
 * @tparam Function The given function be a templated function, with one
 *  size_t template argument and no more. It also shouldn't take any function
 *  arguments. This parameter should be passed per deduction.
 *
 * @param value Value that you need as a compile time value.
 * @param functionBody The templated function, which you wish to execute.
 */
template <size_t MaxValue, typename Function>
void RuntimeValueToCompileTimeValue(const size_t& value, const Function& functionBody){
  ConstExprForLoop(std::make_index_sequence<MaxValue>{}, [&functionBody, &value]
      <size_t Index>(){
        if (Index == value) {functionBody.template operator()<Index>();}
      }
  );
}

// Added support for serializing `std::variant` using `nlohmann::json`.
namespace nlohmann {
    template <typename... Types>
    struct adl_serializer<std::variant<Types...>> {
        static void to_json(nlohmann::json& j,
            const std::variant<Types...>& var) {
          // We need to save, which of the types the std::variant actually
          // uses.
          size_t index = var.index();
          j["index"] = index;

          // 'Translate' the runtime variable index to a compile time value,
          // so that we can get the currently used value. This is needed,
          // because `var` is only known at compile time. And by extension, so
          // is `var.index()`.
          DISABLE_WARNINGS_CLANG_13
          RuntimeValueToCompileTimeValue<sizeof...(Types)>(index, [&j, &var]
              <size_t Index>(){
                ENABLE_WARNINGS_CLANG_13
                j["value"] = std::get<Index>(var);
              }
          );
        }

        static void from_json(const nlohmann::json& j,
            std::variant<Types...>& var) {
          // Which of the `sizeof...(Types)` possible value types, was the
          // serialized std::variant using?
          size_t index = j["index"].get<size_t>();

          // Interpreting the value based on its type.
          DISABLE_WARNINGS_CLANG_13
          RuntimeValueToCompileTimeValue<sizeof...(Types)>(index, [&j, &var]
              <size_t Index>(){
                ENABLE_WARNINGS_CLANG_13
                var = j["value"].get<std::variant_alternative_t<Index,
                  std::variant<Types...>>>();
              }
          );
        }
    };
}

#endif  // QLEVER_JSON_H
