// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_DEFAULTEDHASH_H
#define QLEVER_SRC_UTIL_DEFAULTEDHASH_H

#include <type_traits>
#include <utility>

#include "backports/reflection.h"

// This file defines the macro `QL_DEFINE_DEFAULTED_HASH(Class, members...)`,
// which makes a class hashable by `absl::Hash` (and therefore usable as a key
// in `ad_utility::HashMap` and `ad_utility::HashSet`) by combining the hashes
// of the listed data members. It is used inside the class, for example:
//
// struct Point {
//   int x_;
//   int y_;
//   QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(Point, x_, y_)
//   QL_DEFINE_DEFAULTED_HASH(Point, x_, y_)
// };
//
// The macro has to list ALL the data members of the class in declaration order
// (so at least one), and the class must not have base classes. That way, the
// hash is consistent with the defaulted comparison of the class (see
// `backports/three_way_comparison.h`), which compares exactly the same members.
// In particular, a member that is added to the class later cannot be forgotten
// in the hash. With C++26 reflection (see `backports/reflection.h`) these
// conditions are checked at compile time. A class for which the hash
// deliberately considers only some of the members (or a function of them) has
// to define the `AbslHashValue` function manually.
//
// NOTE: The class is only hashable as itself, but not via an implicit
// conversion from another type (this includes classes that are derived from
// it).

#ifdef QLEVER_HAS_REFLECTION

#include <string>
#include <string_view>

namespace ad_utility::defaultedHash {
// Return the names of the non-static data members of the class `T` (including
// the private ones) in declaration order, separated by `", "`.
//
// NOTE: This is deliberately a template. GCC 16 (20260322) wrongly evaluates
// an equivalent non-template `consteval` function that takes the class as a
// `std::meta::info` argument when it is called from a member function of that
// class (the loop then sees no members at all).
template <typename T>
consteval std::string namesOfDataMembers() {
  std::string result;
  for (auto member : std::meta::nonstatic_data_members_of(
           ^^T, std::meta::access_context::unchecked())) {
    if (!result.empty()) {
      result += ", ";
    }
    result += std::meta::identifier_of(member);
  }
  return result;
}

// Return `text` with all whitespace removed. Used to compare the result of the
// stringification (`#__VA_ARGS__`) of a list of identifiers in a macro with the
// result of `namesOfDataMembers` independent of the formatting.
consteval std::string removeWhitespace(std::string_view text) {
  std::string result;
  for (char c : text) {
    if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
      result += c;
    }
  }
  return result;
}

// Return the empty string iff the stringified list of members of
// `QL_DEFINE_DEFAULTED_HASH` (`listedMembers`) contains exactly the data
// members of `T` in declaration order, and `T` has no base classes. Otherwise
// return an error message.
template <typename T>
consteval std::string_view checkHashedMembers(std::string_view listedMembers) {
  const std::string className{std::meta::display_string_of(^^T)};
  auto error = [&className](std::string_view message) {
    return std::string_view{std::define_static_string(
        "The `QL_DEFINE_DEFAULTED_HASH` (see `util/DefaultedHash.h`) of `" +
        className + "` is invalid: " + message)};
  };
  if (!std::meta::bases_of(^^T, std::meta::access_context::unchecked())
           .empty()) {
    return error(
        "The class has base classes, which is not supported. Define the "
        "`AbslHashValue` function manually");
  }
  auto actualMembers = namesOfDataMembers<T>();
  if (removeWhitespace(listedMembers) != removeWhitespace(actualMembers)) {
    return error("The listed members `" + std::string{listedMembers} +
                 "` are not exactly the data members of the class in "
                 "declaration order, which are `" +
                 actualMembers + "`");
  }
  return {};
}
}  // namespace ad_utility::defaultedHash

// The check is implemented in a static member function that is not a template,
// such that for a class that is not a template it is evaluated when the class
// is defined, even if the hash is never used. For a class template, the
// function is instantiated (and thus the check is evaluated) by the call in the
// hash function.
#define QL_IMPL_DEFINE_CHECK_OF_HASHED_MEMBERS(T, ...)                     \
  static constexpr void qlImplCheckHashedMembers() {                       \
    static_assert(                                                         \
        ::ad_utility::defaultedHash::checkHashedMembers<T>(#__VA_ARGS__)   \
            .empty(),                                                      \
        ::ad_utility::defaultedHash::checkHashedMembers<T>(#__VA_ARGS__)); \
  }
#define QL_IMPL_CALL_CHECK_OF_HASHED_MEMBERS qlImplCheckHashedMembers();
#else
#define QL_IMPL_DEFINE_CHECK_OF_HASHED_MEMBERS(T, ...)
#define QL_IMPL_CALL_CHECK_OF_HASHED_MEMBERS
#endif

// See the documentation at the beginning of this file. The actual hashing is
// implemented in a member function, because inside of it the members can be
// named without a prefix.
#define QL_DEFINE_DEFAULTED_HASH(T, ...)                                \
  QL_IMPL_DEFINE_CHECK_OF_HASHED_MEMBERS(T, __VA_ARGS__)                \
  template <typename QlHashState>                                       \
  QlHashState qlImplCombineHashOfMembers(QlHashState hashState) const { \
    QL_IMPL_CALL_CHECK_OF_HASHED_MEMBERS                                \
    return QlHashState::combine(std::move(hashState), __VA_ARGS__);     \
  }                                                                     \
  template <typename QlHashState, typename QlHashedType,                \
            std::enable_if_t<std::is_same_v<QlHashedType, T>, int> = 0> \
  friend QlHashState AbslHashValue(QlHashState hashState,               \
                                   const QlHashedType& value) {         \
    return value.qlImplCombineHashOfMembers(std::move(hashState));      \
  }

#endif  // QLEVER_SRC_UTIL_DEFAULTEDHASH_H
