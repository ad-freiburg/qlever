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

// Adding third party type support.
namespace nlohmann {
    template <typename T>
    struct adl_serializer<std::optional<T>> {
        static void to_json(nlohmann::json& j, const std::optional<T>& opt) {
          j = nlohmann::json{{"has_value", opt.has_value()}};
          // Only add, if there is value.
          if (opt.has_value()) {j["value"] = opt.value();}
        }

        static void from_json(const nlohmann::json& j, std::optional<T>& opt) {
          if (j.is_null() || !j["has_value"].get<bool>()){
            opt = std::nullopt;
          } else {
            opt = j["value"].get<T>();
          }
        }
    };
}

#endif  // QLEVER_JSON_H
