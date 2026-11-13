// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_BACKPORTS_REFLECTION_H
#define QLEVER_SRC_BACKPORTS_REFLECTION_H

// Detect support for C++26 static reflection (P2996) together with the
// expansion statements (`template for`, P1306), and define the macro
// `QLEVER_HAS_REFLECTION` if both are available. QLever doesn't depend on
// reflection, but uses it (guarded by `#ifdef QLEVER_HAS_REFLECTION`) for
// additional compile-time checks of invariants that the C++17/C++20 code has to
// state redundantly. Currently, only GCC 16 with `-std=c++26 -freflection`
// supports reflection, which CMake passes automatically for that compiler (see
// the `QLEVER_USE_REFLECTION` option).

// NOTE: The `<version>` header only exists since C++20 (e.g. it is missing in
// the standard library of GCC 8). Without it, there is no reflection either.
#if __has_include(<version>)
#include <version>
#endif

#if defined(__cpp_impl_reflection) && defined(__cpp_lib_reflection) && \
    defined(__cpp_expansion_statements)
#define QLEVER_HAS_REFLECTION
#endif

#ifdef QLEVER_HAS_REFLECTION
#include <meta>
#include <string>
#include <string_view>
#include <vector>

// Helpers for the compile-time checks that compare a list of data members that
// is passed to a macro with the actual data members of a class.
namespace ql::reflection {
// Return the names of the non-static data members of the class `T` (including
// the private ones) in declaration order.
//
// NOTE: This is deliberately a template. GCC 16 (20260322) wrongly evaluates
// an equivalent non-template `consteval` function that takes the class as a
// `std::meta::info` argument when it is called from a member function of that
// class (the loop then sees no members at all).
template <typename T>
consteval std::vector<std::string> namesOfDataMembers() {
  std::vector<std::string> result;
  auto members = std::meta::nonstatic_data_members_of(
      ^^T, std::meta::access_context::unchecked());
  for (auto member : members) {
    result.emplace_back(std::meta::identifier_of(member));
  }
  return result;
}

// Return the `names`, separated by `", "`, for example for an error message.
consteval std::string joinNames(const std::vector<std::string>& names) {
  std::string result;
  for (const auto& name : names) {
    if (!result.empty()) {
      result += ", ";
    }
    result += name;
  }
  return result;
}

// Return `text` with all whitespace removed.
consteval std::string removeWhitespace(std::string_view text) {
  std::string result;
  for (char c : text) {
    if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
      result += c;
    }
  }
  return result;
}

// Split the stringification (`#__VA_ARGS__`) of a list of identifiers in a
// macro into the single identifiers, such that it can be compared with the
// result of `namesOfDataMembers` independent of the formatting. Whitespace is
// ignored, and an empty `list` yields an empty result.
consteval std::vector<std::string> splitListOfIdentifiers(
    std::string_view list) {
  std::vector<std::string> result;
  std::string current;
  for (char c : removeWhitespace(list)) {
    if (c == ',') {
      result.push_back(std::move(current));
      current.clear();
    } else {
      current += c;
    }
  }
  if (!current.empty()) {
    result.push_back(std::move(current));
  }
  return result;
}

// Return the error message "The `checkedEntity` of `T` is invalid: `message`"
// as a string with static storage duration, such that it can be the result of
// a `consteval` function and the message of a `static_assert`.
template <typename T>
consteval std::string_view makeErrorMessage(std::string_view checkedEntity,
                                            std::string_view message) {
  return std::define_static_string(
      "The " + std::string{checkedEntity} + " of `" +
      std::string{std::meta::display_string_of(^^T)} +
      "` is invalid: " + std::string{message});
}
}  // namespace ql::reflection

// Statically assert that the `consteval` expression (the variadic arguments, as
// it may contain commas), which returns a `std::string_view`, returns the empty
// string, and use its result as the error message otherwise. The expression is
// evaluated only once. This has to be used inside a function body.
#define QL_STATIC_ASSERT_NO_ERROR_MESSAGE(...)                     \
  {                                                                \
    constexpr std::string_view qlImplErrorMessage = __VA_ARGS__;   \
    static_assert(qlImplErrorMessage.empty(), qlImplErrorMessage); \
  }
#endif

#endif  // QLEVER_SRC_BACKPORTS_REFLECTION_H
