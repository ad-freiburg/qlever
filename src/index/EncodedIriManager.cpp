// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/EncodedIriManager.h"

#include "util/CtreHelpers.h"

// ____________________________________________________________________________
std::optional<std::string_view> detail::matchDigitsPrefix(
    std::string_view repr) {
  static constexpr auto regex = ctll::fixed_string{"(?<digits>[0-9]+)>"};
  auto match = ctre::match<regex>(repr);
  if (!match) {
    return std::nullopt;
  }
  constexpr ctll::fixed_string digitsCaptureGroup = "digits";
  return match.template get<digitsCaptureGroup>().to_view();
}
