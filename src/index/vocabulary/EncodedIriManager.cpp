// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/EncodedIriManager.h"

#include "util/CtreHelpers.h"

// The regex that matches the digits at the end of an encodable IRI, and the
// name of its capture group. NOTE: These must live at namespace scope and must
// not be local to the function below. They are used as non-type template
// arguments, which in C++17 requires them to have linkage; a block-scope
// variable has none. `static` gives them internal linkage, which is sufficient.
static constexpr auto digitsRegex = ctll::fixed_string{"(?<digits>[0-9]+)>"};
static constexpr auto digitsCaptureGroup = ctll::fixed_string{"digits"};

// ____________________________________________________________________________
std::optional<std::string_view> detail::matchDigitsPrefix(
    std::string_view repr) {
  auto match = ctre::match<digitsRegex>(repr);
  if (!match) {
    return std::nullopt;
  }
  return match.template get<digitsCaptureGroup>().to_view();
}
