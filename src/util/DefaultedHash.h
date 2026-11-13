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
// conditions are checked at compile time.
//
// A class for which the hash deliberately considers only some of the members
// uses `QL_DEFINE_DEFAULTED_HASH_OF_SUBSET` instead, which additionally lists
// the members that are NOT hashed, for example:
//
// struct Literal {
//   std::string content_;
//   size_t beginOfSuffix_;
//   QL_DEFINE_DEFAULTED_HASH_OF_SUBSET(Literal, QL_HASHED(content_),
//                                      QL_NOT_HASHED(beginOfSuffix_))
// };
//
// Each of the two lists has to be in declaration order, and together they have
// to contain each data member of the class exactly once (which is also checked
// with reflection). That way, it is an explicit decision for each member
// (in particular for one that is added later) whether it is hashed. Note that
// not hashing a member is always correct, but leads to hash collisions between
// objects that only differ in that member. A class for which the hash is not
// simply a combination of data members (for example because it hashes a
// function of them or a base class) has to define the `AbslHashValue` function
// manually.
//
// NOTE: The class is only hashable as itself, but not via an implicit
// conversion from another type (this includes classes that are derived from
// it).

#ifdef QLEVER_HAS_REFLECTION

#include <string>
#include <string_view>

namespace ql::reflection {
// Return the empty string iff the stringified lists of the members that are
// hashed (`hashedMembers`) and of the members that are deliberately not hashed
// (`notHashedMembers`) are each in the declaration order of the class `T`, and
// together contain each data member of `T` exactly once, and `T` has no base
// classes. Otherwise return an error message.
//
// NOTE: This is deliberately a template. GCC 16 (20260322) wrongly evaluates
// an equivalent non-template `consteval` function that takes the class as a
// `std::meta::info` argument when it is called from a member function of that
// class (the loop then sees no members at all).
template <typename T>
consteval std::string_view checkHashedMembers(
    std::string_view hashedMembers, std::string_view notHashedMembers) {
  auto error = [](std::string_view message) {
    return makeErrorMessage<T>(
        "`QL_DEFINE_DEFAULTED_HASH...` (see `util/DefaultedHash.h`)", message);
  };
  if (!std::meta::bases_of(^^T, std::meta::access_context::unchecked())
           .empty()) {
    return error(
        "The class has base classes, which is not supported. Define the "
        "`AbslHashValue` function manually");
  }
  auto hashed = splitListOfIdentifiers(hashedMembers);
  auto notHashed = splitListOfIdentifiers(notHashedMembers);
  // As the names of the members are unique, the two lists are valid iff each
  // member of the class is the next element of exactly one of them.
  size_t numHashed = 0;
  size_t numNotHashed = 0;
  bool isValid = true;
  auto actualMembers = namesOfDataMembers<T>();
  for (const auto& name : actualMembers) {
    if (numHashed < hashed.size() && hashed[numHashed] == name) {
      ++numHashed;
    } else if (numNotHashed < notHashed.size() &&
               notHashed[numNotHashed] == name) {
      ++numNotHashed;
    } else {
      isValid = false;
    }
  }
  if (isValid && numHashed == hashed.size() &&
      numNotHashed == notHashed.size()) {
    return {};
  }
  // The message for `QL_DEFINE_DEFAULTED_HASH_OF_SUBSET`.
  if (!notHashedMembers.empty()) {
    return error("The hashed members `" + std::string{hashedMembers} +
                 "` and the not hashed members `" +
                 std::string{notHashedMembers} +
                 "` have to be in declaration order each, and together have "
                 "to be exactly the data members of the class, which are `" +
                 joinNames(actualMembers) + "`");
  }
  return error("The listed members `" + std::string{hashedMembers} +
               "` are not exactly the data members of the class in "
               "declaration order, which are `" +
               joinNames(actualMembers) + "`");
}
}  // namespace ql::reflection

// The check is implemented in a static member function that is not a template,
// such that for a class that is not a template it is evaluated when the class
// is defined, even if the hash is never used. For a class template, the
// function is instantiated (and thus the check is evaluated) by the call in the
// hash function. `HASHED` and `NOT_HASHED` are string literals.
#define QL_IMPL_DEFINE_CHECK_OF_HASHED_MEMBERS(T, HASHED, NOT_HASHED) \
  static constexpr void qlImplCheckHashedMembers() {                  \
    QL_STATIC_ASSERT_NO_ERROR_MESSAGE(                                \
        ::ql::reflection::checkHashedMembers<T>(HASHED, NOT_HASHED))  \
  }
#define QL_IMPL_CALL_CHECK_OF_HASHED_MEMBERS qlImplCheckHashedMembers();
#else
#define QL_IMPL_DEFINE_CHECK_OF_HASHED_MEMBERS(T, HASHED, NOT_HASHED)
#define QL_IMPL_CALL_CHECK_OF_HASHED_MEMBERS
#endif

// The implementation of the two public macros below. `HASHED` and `NOT_HASHED`
// are the stringified lists of the respective members, and the variadic
// arguments are the members that are hashed. The actual hashing is implemented
// in a member function, because inside of it the members can be named without
// a prefix.
#define QL_IMPL_DEFAULTED_HASH(T, HASHED, NOT_HASHED, ...)              \
  QL_IMPL_DEFINE_CHECK_OF_HASHED_MEMBERS(T, HASHED, NOT_HASHED)         \
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

// See the documentation at the beginning of this file.
#define QL_DEFINE_DEFAULTED_HASH(T, ...) \
  QL_IMPL_DEFAULTED_HASH(T, #__VA_ARGS__, "", __VA_ARGS__)

// The two lists of members of `QL_DEFINE_DEFAULTED_HASH_OF_SUBSET`. They expand
// to a parenthesized list, which the following helper macros turn into a plain
// list of members (`QL_IMPL_UNWRAP LIST`) or into a string literal.
#define QL_HASHED(...) (__VA_ARGS__)
#define QL_NOT_HASHED(...) (__VA_ARGS__)
#define QL_IMPL_UNWRAP(...) __VA_ARGS__
#define QL_IMPL_STRINGIFY(...) #__VA_ARGS__
#define QL_IMPL_STRINGIFY_LIST(LIST) QL_IMPL_STRINGIFY LIST

// See the documentation at the beginning of this file. `HASHED` has to be a
// `QL_HASHED(members...)` and `NOT_HASHED` a `QL_NOT_HASHED(members...)`.
#define QL_DEFINE_DEFAULTED_HASH_OF_SUBSET(T, HASHED, NOT_HASHED) \
  QL_IMPL_DEFAULTED_HASH(T, QL_IMPL_STRINGIFY_LIST(HASHED),       \
                         QL_IMPL_STRINGIFY_LIST(NOT_HASHED),      \
                         QL_IMPL_UNWRAP HASHED)

#endif  // QLEVER_SRC_UTIL_DEFAULTEDHASH_H
