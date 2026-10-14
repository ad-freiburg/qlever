// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/EncodedIriManager.h"

#include <absl/strings/str_cat.h>

#include <range/v3/view/enumerate.hpp>
#include <stdexcept>

#include "util/Algorithm.h"

namespace {
// The JSON keys, see `detail::patternsToJson`.
constexpr std::string_view jsonKeyPrefixes =
    "prefixes-with-leading-angle-brackets";
constexpr std::string_view jsonKeyPatterns = "patterns";

// Throw if the `prefix` (which the `origin` describes for the error message)
// starts with a `<`, which the manager adds itself.
void checkNoLeadingAngleBracket(std::string_view prefix,
                                std::string_view origin) {
  if (ql::starts_with(prefix, '<')) {
    throw std::runtime_error(absl::StrCat(
        "The prefixes ", origin,
        " must not be enclosed in angle brackets; here is a violating "
        "prefix: \"",
        prefix, "\""));
  }
}

// Throw if `numPatterns` patterns don't fit into the bits that are reserved
// for the tag.
void checkNumberOfPatterns(size_t numPatterns, size_t maxNumPatterns) {
  if (numPatterns > maxNumPatterns) {
    throw std::runtime_error(absl::StrCat(
        "The number of prefixes and patterns for IRIs that are encoded "
        "directly in an ID is ",
        numPatterns, ", which is too many; the maximum is ", maxNumPatterns));
  }
}
}  // namespace

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
    checkNoLeadingAngleBracket(prefix, "specified with `--encode-as-id`");
    result.push_back(absl::StrCat("<", prefix));
  }
  return result;
}

// ____________________________________________________________________________
std::vector<encodedIri::Pattern> detail::makePatterns(
    std::vector<std::string> prefixes,
    std::vector<encodedIri::Pattern> patterns, size_t numBitsEncoding,
    size_t maxNumPatterns) {
  std::vector<encodedIri::Pattern> result;
  result.reserve(prefixes.size() + patterns.size());
  for (auto& prefix :
       sortAndCheckPrefixes(std::move(prefixes), maxNumPatterns)) {
    result.push_back(
        encodedIri::plainPrefixPattern(std::move(prefix), numBitsEncoding));
  }
  for (auto& pattern : patterns) {
    checkNoLeadingAngleBracket(pattern.prefix_,
                               "of the patterns for encoded IRIs");
    encodedIri::validatePattern(pattern, numBitsEncoding);
    pattern.prefix_.insert(0, 1, '<');
    result.push_back(std::move(pattern));
  }
  checkNumberOfPatterns(result.size(), maxNumPatterns);
  return result;
}

// ____________________________________________________________________________
std::optional<std::pair<size_t, uint64_t>> detail::matchPatterns(
    const std::vector<encodedIri::Pattern>& patterns, std::string_view repr) {
  for (const auto& [tag, pattern] : ::ranges::views::enumerate(patterns)) {
    if (!ql::starts_with(repr, pattern.prefix_)) {
      continue;
    }
    auto payload =
        encodedIri::encodePayload(pattern, repr.substr(pattern.prefix_.size()));
    if (payload.has_value()) {
      return std::pair{static_cast<size_t>(tag), payload.value()};
    }
  }
  return std::nullopt;
}

// ____________________________________________________________________________
void detail::patternsToJson(nlohmann::json& j,
                            const std::vector<encodedIri::Pattern>& patterns,
                            size_t numBitsEncoding) {
  auto isPlain = [numBitsEncoding](const encodedIri::Pattern& pattern) {
    return encodedIri::isPlainPrefixPattern(pattern, numBitsEncoding);
  };
  if (ql::ranges::all_of(patterns, isPlain)) {
    j[jsonKeyPrefixes] =
        ad_utility::transform(patterns, &encodedIri::Pattern::prefix_);
  } else {
    j[jsonKeyPatterns] = patterns;
  }
}

// ____________________________________________________________________________
std::vector<encodedIri::Pattern> detail::patternsFromJson(
    const nlohmann::json& j, size_t numBitsEncoding, size_t maxNumPatterns) {
  std::vector<encodedIri::Pattern> patterns;
  if (j.contains(jsonKeyPatterns)) {
    patterns = j.at(jsonKeyPatterns).get<std::vector<encodedIri::Pattern>>();
    // The patterns come from the index metadata, which might have been
    // manipulated, so they have to be validated again.
    for (const auto& pattern : patterns) {
      encodedIri::validatePattern(pattern, numBitsEncoding);
    }
  } else {
    for (auto& prefix : j.at(jsonKeyPrefixes).get<std::vector<std::string>>()) {
      patterns.push_back(
          encodedIri::plainPrefixPattern(std::move(prefix), numBitsEncoding));
    }
  }
  checkNumberOfPatterns(patterns.size(), maxNumPatterns);
  return patterns;
}
