// Copyright 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ENUMWITHSTRINGS_H
#define QLEVER_SRC_UTIL_ENUMWITHSTRINGS_H

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <array>
#include <stdexcept>
#include <string_view>

#include "backports/three_way_comparison.h"
#include "util/Random.h"
#include "util/json.h"

namespace ad_utility {

// A CRTP base class for lightweight enum wrapper classes that need string
// conversion, JSON serialization, and related utilities.
//
// Derived classes must provide:
// - `enum struct Enum { ... };`
// - `static constexpr size_t numValues_`
// - `static constexpr std::array<Enum, numValues_> all_`
// - `static constexpr std::array<std::string_view, numValues_> descriptions_`
// - `static constexpr std::string_view typeName()` (for error messages)
// - `using EnumWithStrings::EnumWithStrings;` (to inherit constructors)
// Derived classes must define `enum struct Enum` as a public member before
// inheriting from this base.  Because CRTP instantiates the base class before
// the derived class body is complete, we cannot resolve `Derived::Enum` inside
// the class definition.  Instead every method that needs the enum type uses
// `typename Derived::Enum` only in deferred (non-eagerly-instantiated) context.
template <typename Derived>
class EnumWithStrings {
 protected:
  // We store the value as the underlying integer type so that the base class
  // does not depend on `Derived::Enum` during class-template instantiation.
  // The concrete enum type is only resolved when member functions are called.
  int value_{};

 public:
  // Constructors.
  EnumWithStrings() = default;
  // The constructor is a template so that `Derived::Enum` is not resolved
  // during base class instantiation (when `Derived` is still incomplete).
  // The `requires` clause prevents this from matching non-enum types (e.g.
  // `nlohmann::json`), which would break JSON deserialization.
  template <typename E>
  requires std::is_enum_v<E>
  explicit EnumWithStrings(E value) : value_{static_cast<int>(value)} {}

  // Return the actual enum value.  Using `auto` defers the return type
  // deduction to the point of use.
  auto value() const { return static_cast<typename Derived::Enum>(value_); }

  // Return all the possible enum values.
  static constexpr const auto& all() { return Derived::all_; }

  // Convert the enum to the corresponding string.
  std::string_view toString() const {
    return Derived::descriptions_.at(static_cast<size_t>(value_));
  }

  // Create from a string. Throws if the string doesn't match any description.
  static Derived fromString(std::string_view description) {
    auto it = ql::ranges::find(Derived::descriptions_, description);
    if (it == Derived::descriptions_.end()) {
      throw std::runtime_error{absl::StrCat(
          "\"", description, "\" is not a valid ", Derived::typeName(),
          ". The currently supported values are ", getListOfSupportedValues())};
    }
    return Derived{Derived::all_.at(
        static_cast<size_t>(it - Derived::descriptions_.begin()))};
  }

  // Return all the possible enum values as a comma-separated single string.
  static std::string getListOfSupportedValues() {
    return absl::StrJoin(Derived::descriptions_, ", ");
  }

  // Get a random value, useful for fuzz testing.
  static Derived random() {
    static thread_local ad_utility::FastRandomIntGenerator<size_t> r;
    return Derived{
        static_cast<typename Derived::Enum>(r() % Derived::numValues_)};
  }

  // JSON serialization.
  friend void to_json(nlohmann::json& j, const Derived& e) { j = e.toString(); }

  friend void from_json(const nlohmann::json& j, Derived& e) {
    e = Derived::fromString(static_cast<std::string>(j));
  }

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(EnumWithStrings, value_)
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_ENUMWITHSTRINGS_H
