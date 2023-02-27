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

// Added support for serializing `std::variant` using `nlohmann::json`.
namespace nlohmann {
    template <typename... Types>
    struct adl_serializer<std::optional<Types...>> {
        static void to_json(nlohmann::json& j,
            const std::variant<Types...>& var) {
          // We need to save, which of the types the std::variant actually
          // uses.
          j["index"] = var.index();
          j["value"] = std::get<var.index()>(var);
        }

        static void from_json(const nlohmann::json& j,
            std::variant<Types...>& var) {
          size_t index = j["index"].get<size_t>();

          auto setValueBasedOnJson = [&j, &var]<size_t Index>(const size_t& i){
            if (Index == i) {
              var = j["value"].get<std::variant_alternative_t<Index,
                  std::variant<Types...>>>();
            }
          };

          auto tryAllValues = [&j, &var,
               &setValueBasedOnJson]<typename T, T... ints>(const size_t& i,
                   const std::integer_sequence<T, ints...>&){
            ((setValueBasedOnJson<ints>(i)), ...);
          };

          tryAllValues(index, std::index_sequence_for<Types...>{});
        }
    };
}

#endif  // QLEVER_JSON_H
