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

namespace {
// Match `repr` against the pattern `([0-9]+)>` and return the digit
// substring as a `string_view` into `repr` on success, or `std::nullopt` if
// the pattern does not match.
std::optional<std::string_view> matchDigitsPrefix(std::string_view repr) {
  auto match = ctre::match<digitsRegex>(repr);
  if (!match) {
    return std::nullopt;
  }
  return match.template get<digitsCaptureGroup>().to_view();
}
}  // namespace

// ____________________________________________________________________________
std::optional<std::pair<size_t, std::string_view>> detail::matchPrefixAndDigits(
    const std::vector<std::string>& prefixes, std::string_view repr,
    size_t maxNumDigits) {
  // Find the matching prefix.
  auto it = ql::ranges::find_if(prefixes, [&repr](std::string_view prefix) {
    return ql::starts_with(repr, prefix);
  });
  if (it == prefixes.end()) {
    return std::nullopt;
  }

  // Check that after the prefix, the string contains only digits and the
  // trailing '>'.
  repr.remove_prefix(it->size());
  auto numStringOpt = matchDigitsPrefix(repr);
  if (!numStringOpt.has_value()) {
    return std::nullopt;
  }
  std::string_view numString = numStringOpt.value();
  if (numString.size() > maxNumDigits) {
    return std::nullopt;
  }
  return std::pair{static_cast<size_t>(it - prefixes.begin()), numString};
}

// ____________________________________________________________________________
std::vector<std::string> detail::sortAndCheckPrefixes(
    std::vector<std::string> prefixes, size_t maxNumPrefixes) {
  if (prefixes.empty()) {
    return {};
  }
  // Sort the prefixes lexicographically to make the ordering deterministic
  // (provided that the prefixes do not end with digits).
  ql::ranges::sort(prefixes);

  // Remove duplicates.
  //
  // NOTE: `ql::ranges::unique` does not work because of a discrepancy in the
  // return types between `std::ranges` and `range-v3`.
  prefixes.erase(::ranges::unique(prefixes), prefixes.end());

  if (prefixes.size() > maxNumPrefixes) {
    throw std::runtime_error(
        absl::StrCat("Number of prefixes specified with `--encode-as-id` is ",
                     prefixes.size(), ", which is too many; ",
                     "the maximum is ", maxNumPrefixes));
  }

  // TODO<C++23> use `std::views::adjacent`.
  for (size_t i = 0; i < prefixes.size() - 1; ++i) {
    const auto& a = prefixes.at(i);
    const auto& b = prefixes.at(i + 1);
    if (ql::starts_with(b, a)) {
      throw std::runtime_error(absl::StrCat(
          "None of the prefixes specified with `--encode-as-id` "
          "may be a prefix of another; here is a violating pair: \"",
          a, "\" and \"", b, "\"."));
    }
  }
  std::vector<std::string> result;
  result.reserve(prefixes.size());
  for (const auto& prefix : prefixes) {
    if (ql::starts_with(prefix, '<')) {
      throw std::runtime_error(absl::StrCat(
          "The prefixes specified with `--encode-as-id` must not "
          "be enclosed in angle brackets; here is a violating prefix: \"",
          prefix, "\""));
    }
    result.push_back(absl::StrCat("<", prefix));
  }
  return result;
}
