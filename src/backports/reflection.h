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
// `QL_HAS_REFLECTION` if both are available. QLever doesn't depend on
// reflection, but uses it (guarded by `#ifdef QL_HAS_REFLECTION`) for
// additional compile-time checks of invariants that the C++17/C++20 code has to
// state redundantly. Currently, only GCC 16 with `-std=c++26 -freflection`
// supports reflection, which CMake passes automatically for that compiler (see
// the `QLEVER_USE_REFLECTION` option).

#include <version>

#if defined(__cpp_impl_reflection) && defined(__cpp_lib_reflection) && \
    defined(__cpp_expansion_statements)
#define QL_HAS_REFLECTION
#endif

#ifdef QL_HAS_REFLECTION
#include <meta>
#include <string>
#include <string_view>

namespace ql::reflection {

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

}  // namespace ql::reflection

#endif

#endif  // QLEVER_SRC_BACKPORTS_REFLECTION_H
