// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/encodedIris/EncodedIriManager.h"

#include <absl/strings/str_cat.h>

#include <range/v3/view/enumerate.hpp>
#include <stdexcept>

namespace {
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
}  // namespace

// _____________________________________________________________________________
EncodedIriManagerBase::EncodedIriManagerBase(size_t numBitsEncoding,
                                             size_t maxNumPatterns)
    : numBitsEncoding_{numBitsEncoding}, maxNumPatterns_{maxNumPatterns} {
  // The tag is stored by shifting it by `numBitsEncoding_`, which requires
  // `numBitsEncoding_` to be smaller than 64.
  AD_CONTRACT_CHECK(numBitsEncoding_ < 64);
}

// _____________________________________________________________________________
std::optional<uint64_t> EncodedIriManagerBase::encodeValue(
    std::string_view repr) const {
  for (const auto& [tag, pattern] : ::ranges::views::enumerate(patterns_)) {
    if (!ql::starts_with(repr, pattern.prefix_)) {
      continue;
    }
    auto payload = encodePayload(pattern, repr.substr(pattern.prefix_.size()));
    if (payload.has_value()) {
      return payload.value() | (static_cast<uint64_t>(tag) << numBitsEncoding_);
    }
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::string EncodedIriManagerBase::decodeValue(uint64_t encodedValue) const {
  // The tag is stored above the payload bits.
  uint64_t tag = encodedValue >> numBitsEncoding_;
  uint64_t payload =
      encodedValue & ad_utility::bitMaskForLowerBits(numBitsEncoding_);
  const auto& pattern = patterns_.at(tag);
  std::string result;
  // A decimal number needs at most 20 characters; the separators are
  // typically short.
  result.reserve(pattern.prefix_.size() + pattern.parts_.size() * 24 + 1);
  result = pattern.prefix_;
  // The first part is stored in the most significant bits of the payload.
  size_t shift = pattern.numBitsStored();
  for (const auto& part : pattern.parts_) {
    size_t numBits = part.numBitsStored();
    shift -= numBits;
    uint64_t stored =
        (payload >> shift) & ad_utility::bitMaskForLowerBits(numBits);
    if (part.encoding_ == encodedIri::NumberEncoding::Nibbles) {
      encodedIri::decodeNibblesToDigits(result, stored, part.numBits_);
    } else {
      encodedIri::decompressNumber(result, part, stored);
    }
    result.append(part.separator_);
  }
  result.push_back('>');
  return result;
}

// _____________________________________________________________________________
std::optional<uint64_t> EncodedIriManagerBase::getIndexOfPrefix(
    std::string_view prefixWithoutAngleBrackets) const {
  auto prefix = absl::StrCat("<", prefixWithoutAngleBrackets);
  auto it = ql::ranges::find_if(patterns_,
                                [&prefix](const encodedIri::Pattern& pattern) {
                                  return pattern.prefix_ == prefix;
                                });
  if (it == patterns_.end()) {
    return std::nullopt;
  }
  return static_cast<size_t>(it - patterns_.begin());
}

// _____________________________________________________________________________
void EncodedIriManagerBase::toJson(nlohmann::json& j) const {
  auto isPlain = [this](const encodedIri::Pattern& pattern) {
    return encodedIri::isPlainPrefixPattern(pattern, numBitsEncoding_);
  };
  if (ql::ranges::all_of(patterns_, isPlain)) {
    std::vector<std::string> prefixes;
    prefixes.reserve(patterns_.size());
    for (const auto& pattern : patterns_) {
      prefixes.push_back(pattern.prefix_);
    }
    j[jsonKey_] = std::move(prefixes);
  } else {
    j[jsonKeyPatterns_] = patterns_;
  }
}

// _____________________________________________________________________________
void EncodedIriManagerBase::fromJson(const nlohmann::json& j) {
  patterns_.clear();
  if (j.contains(jsonKeyPatterns_)) {
    patterns_ = j.at(jsonKeyPatterns_).get<std::vector<encodedIri::Pattern>>();
    // The patterns come from the index metadata, which might have been
    // manipulated, so they have to be validated again.
    for (const auto& pattern : patterns_) {
      encodedIri::validatePattern(pattern, numBitsEncoding_);
    }
  } else {
    for (auto& prefix : j.at(jsonKey_).get<std::vector<std::string>>()) {
      patterns_.push_back(
          encodedIri::plainPrefixPattern(std::move(prefix), numBitsEncoding_));
    }
  }
  checkNumberOfPatterns();
}

// _____________________________________________________________________________
void EncodedIriManagerBase::addPlainPrefixes(
    std::vector<std::string> prefixes) {
  if (prefixes.empty()) {
    return;
  }
  // Sort the prefixes lexicographically to make the ordering deterministic
  // (provided that the prefixes do not end with digits).
  ql::ranges::sort(prefixes);

  // Remove duplicates.
  //
  // NOTE: `ql::ranges::unique` does not work because of a discrepancy in the
  // return types between `std::ranges` and `range-v3`.
  prefixes.erase(::ranges::unique(prefixes), prefixes.end());

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
  patterns_.reserve(prefixes.size());
  for (auto& prefix : prefixes) {
    checkNoLeadingAngleBracket(prefix, "specified with `--encode-as-id`");
    patterns_.push_back(encodedIri::plainPrefixPattern(
        absl::StrCat("<", prefix), numBitsEncoding_));
  }
}

// _____________________________________________________________________________
void EncodedIriManagerBase::addPatterns(
    std::vector<encodedIri::Pattern> patterns) {
  for (auto& pattern : patterns) {
    checkNoLeadingAngleBracket(pattern.prefix_,
                               "of the patterns for encoded IRIs");
    encodedIri::validatePattern(pattern, numBitsEncoding_);
    pattern.prefix_.insert(0, 1, '<');
    patterns_.push_back(std::move(pattern));
  }
}

// _____________________________________________________________________________
void EncodedIriManagerBase::checkNumberOfPatterns() const {
  if (patterns_.size() > maxNumPatterns_) {
    throw std::runtime_error(absl::StrCat(
        "The number of prefixes and patterns for IRIs that are encoded "
        "directly in an ID is ",
        patterns_.size(), ", which is too many; the maximum is ",
        maxNumPatterns_));
  }
}

// _____________________________________________________________________________
std::optional<uint64_t> EncodedIriManagerBase::encodePayload(
    const encodedIri::Pattern& pattern, std::string_view suffix) {
  uint64_t payload = 0;
  for (const auto& part : pattern.parts_) {
    auto digits = encodedIri::leadingDigits(suffix);
    if (digits.empty()) {
      return std::nullopt;
    }
    suffix.remove_prefix(digits.size());
    if (!ql::starts_with(suffix, part.separator_)) {
      return std::nullopt;
    }
    suffix.remove_prefix(part.separator_.size());
    std::optional<uint64_t> stored;
    if (part.encoding_ == encodedIri::NumberEncoding::Nibbles) {
      if (digits.size() * encodedIri::NibbleSize > part.numBits_) {
        return std::nullopt;
      }
      stored = encodedIri::encodeDigitsAsNibbles(digits, part.numBits_);
    } else {
      auto value = encodedIri::parseDecimal(digits);
      if (!value.has_value()) {
        return std::nullopt;
      }
      stored = encodedIri::compressNumber(part, value.value());
    }
    if (!stored.has_value()) {
      return std::nullopt;
    }
    payload = (payload << part.numBitsStored()) | stored.value();
  }
  if (suffix != ">") {
    return std::nullopt;
  }
  return payload;
}
