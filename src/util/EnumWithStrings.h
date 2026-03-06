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
#include <ostream>
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
// - `static constexpr std::array<std::pair<Enum, std::string_view>, numValues>
// descriptions_`;
//   Note: `descriptions[i][0] == Enum{i}` must always hold. this is technically
//   redundant, but improves the safety, as we can detect invalid usages.
// - `static constexpr std::string_view typeName()` (for error messages)
// - `using EnumWithStrings::EnumWithStrings;` (to inherit constructors)
// - using enum Enum (not required, but makes conversion easier).

//  Note: for convenient definition of constants, in the derived classes you can
//  either put in a
// `using enum` statement, or manually define static constants of the derived
// type. The latter have the advantage, that you directly get constants of the
// derived type, and not of the underlying enum. For example usages, see
// `CompressionAlgorithm.h`  and `VocabularyType.h`.

// a generic base class, used to detect instantiations of `EnumWithStrings` at
// compile time
struct EnumWithStringsBaseTag {};

CPP_template(typename Derived,
             typename Enum)(requires std::is_enum_v<Enum>) class EnumWithStrings
    : public EnumWithStringsBaseTag {
 public:
 protected:
  Enum value_{};

 public:
  constexpr static bool checkIndices() {
    for (size_t i = 0; i < Derived::descriptions_.size(); ++i) {
      if (static_cast<size_t>(Derived::descriptions_[i].first) != i) {
        return false;
      }
    }
    return true;
  }

  // Check properties of the enum type.
  constexpr static void check() {
    using D = decltype(Derived::descriptions_);
    static_assert(ql::ranges::random_access_range<D>);
    static_assert(ql::concepts::same_as<ql::ranges::range_value_t<D>,
                                        std::pair<Enum, std::string_view>>);
    static_assert(checkIndices());
  }

  // Constructors.
  EnumWithStrings() noexcept { check(); };
  // Deliberately implicit, s.t. we can directly construct from the underlying
  // enum.
  explicit(false) EnumWithStrings(Enum value) : value_{value} { check(); }
  explicit(false) operator Enum() const { return value_; }

  // Access to the underlying enum, e.g. for switch-case statements.
  constexpr Enum value() const { return value_; }

  // Return all the possible enum values.
  static constexpr auto all() {
    return Derived::descriptions_ | ql::views::keys;
  }
  static constexpr auto descriptions() {
    return Derived::descriptions_ | ql::views::values;
  }
  static constexpr size_t numValues() { return Derived::descriptions_.size(); }

  // Convert the enum to the corresponding string.
  constexpr std::string_view toString() const {
    return descriptions()[static_cast<size_t>(value_)];
  }

  friend std::ostream& operator<<(std::ostream& stream, const Derived& d) {
    return stream << d.toString();
  }

  // Create from a string. Throws if the string doesn't match any description.
  static Derived fromString(std::string_view description) {
    auto descs = descriptions();
    auto it = ql::ranges::find(descs, description);
    if (it == descs.end()) {
      throw std::runtime_error{absl::StrCat(
          "\"", description, "\" is not a valid ", Derived::typeName(),
          ". The currently supported values are ", getListOfSupportedValues())};
    }
    return std::get<0>(
        Derived::descriptions_.at(static_cast<size_t>(it - descs.begin())));
  }

  // Return all the possible enum values as a comma-separated single string.
  static std::string getListOfSupportedValues() {
    return absl::StrJoin(descriptions(), ", ");
  }

  // Get a random value, useful for fuzz testing.
  static Derived random() {
    thread_local ad_utility::FastRandomIntGenerator<size_t> r;
    return all()[r() % numValues()];
  }

  // JSON serialization.
  friend void to_json(nlohmann::json& j, const Derived& e) { j = e.toString(); }

  friend void from_json(const nlohmann::json& j, Derived& e) {
    e = Derived::fromString(static_cast<std::string>(j));
  }

  // Hashing
  CPP_template_2(typename H, typename D)(
      requires ql::concepts::same_as<D, Derived>) friend H
      AbslHashValue(H h, const D& derived) {
    return H::combine(std::move(h), static_cast<Enum>(derived));
  }

  // Note: We don't need to and also cannot define an equality operator but
  // equality and other comparisons are handled by the implicit conversion to
  // the underlying enum.
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_ENUMWITHSTRINGS_H
