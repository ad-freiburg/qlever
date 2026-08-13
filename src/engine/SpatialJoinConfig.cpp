// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/SpatialJoinConfig.h"

#include <algorithm>
#include <ctre-unicode.hpp>

namespace {
constexpr ctll::fixed_string de9imFilterRegex = "[0-2TtFf*]{9}";
}  // namespace

// ____________________________________________________________________________
std::optional<De9imFilterString> parseDe9imFilterString(
    std::string_view filter) {
  if (!ctre::match<de9imFilterRegex>(filter)) {
    return std::nullopt;
  }
  De9imFilterString result{};
  std::ranges::copy(filter, result.begin());
  return result;
}

// ____________________________________________________________________________
bool de9imFilterCanMatchDisjoint(const De9imFilterString& filter) {
  auto admitsF = [](char c) { return c == '*' || c == 'F' || c == 'f'; };
  return admitsF(filter[0]) && admitsF(filter[1]) && admitsF(filter[3]) &&
         admitsF(filter[4]);
}

// ____________________________________________________________________________
std::optional<De9imFilterString> validateDe9imFilterString(
    std::string_view filter) {
  auto result = parseDe9imFilterString(filter);
  if (!result.has_value() || de9imFilterCanMatchDisjoint(result.value())) {
    return std::nullopt;
  }
  return result;
}
