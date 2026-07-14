// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/EncodedIriManager.h"

#include "util/CtreHelpers.h"

namespace {
// Patterns for `detail::matchDigitsPrefix` below. These have to be variables
// with linkage (not function-local), because a `const auto&` non-type
// template parameter can only bind to an object with static storage
// duration and linkage, not to a function-local variable (unlike in C++20,
// which allows literal-class-type template arguments directly).
constexpr auto digitsPrefixRegex = ctll::fixed_string{"(?<digits>[0-9]+)>"};
constexpr ctll::fixed_string digitsCaptureGroup = "digits";
}  // namespace

// ____________________________________________________________________________
std::optional<std::string_view> detail::matchDigitsPrefix(
    std::string_view repr) {
  auto match = ctre::match<digitsPrefixRegex>(repr);
  if (!match) {
    return std::nullopt;
  }
  return match.template get<digitsCaptureGroup>().to_view();
}
