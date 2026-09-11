// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "blobConverter/LegacyEncodedIriManager.h"

#include <absl/strings/str_cat.h>

#include <array>
#include <limits>
#include <stdexcept>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "blobConverter/LegacyDatatype.h"
#include "util/BitUtils.h"
#include "util/Exception.h"

namespace qlever::blobConverter {

namespace {
using encodedIri::FixedBitRange;

// The legacy decimal parser: parse `digits` (which must consist of decimal
// digits only) as a `uint64_t`. Leading zeros are accepted (and lost). Return
// `std::nullopt` on overflow.
std::optional<uint64_t> parseLegacyDecimal(std::string_view digits) {
  uint64_t value = 0;
  for (char c : digits) {
    uint64_t digit = static_cast<uint64_t>(c - '0');
    if (value > (std::numeric_limits<uint64_t>::max() - digit) / 10) {
      return std::nullopt;
    }
    value = value * 10 + digit;
  }
  return value;
}

// Match the `suffix` against the shape `<digits><separators[0]><digits>...
// <separators[n-1]>`, where the last separator includes the closing `>`, and
// return the parsed numbers. Return `std::nullopt` if the `suffix` does not
// have this shape, or if one of the numbers overflows.
std::optional<std::vector<uint64_t>> matchNumbers(
    std::string_view suffix, const std::vector<std::string_view>& separators) {
  std::vector<uint64_t> numbers;
  for (const auto& separator : separators) {
    auto digits = encodedIri::leadingDigits(suffix);
    if (digits.empty()) {
      return std::nullopt;
    }
    suffix.remove_prefix(digits.size());
    if (!ql::starts_with(suffix, separator)) {
      return std::nullopt;
    }
    suffix.remove_prefix(separator.size());
    auto number = parseLegacyDecimal(digits);
    if (!number.has_value()) {
      return std::nullopt;
    }
    numbers.push_back(number.value());
  }
  if (!suffix.empty()) {
    return std::nullopt;
  }
  return numbers;
}

// Return true if `num1` fulfills the constraints of the first number of the
// `RangePattern` and `ValRangePattern` schemes: it fits into 32 bits, its bits
// 30 and 31 are zero, and its bit 29 is one.
bool isValid32BitRangeNumber(uint64_t num1) {
  return num1 < (1ULL << 32) && (num1 >> 30) == 0 && ((num1 >> 29) & 1) == 1;
}

// Return true if `num1` fulfills the constraints of the first number of the
// `LaneRef`-like schemes (and the 64-bit variant of `StopLoc`): its bits 62 and
// 63 are zero, its bit 61 is one, and its bits `[17, 32)` are zero.
bool isValid64BitRefNumber(uint64_t num1) {
  if ((num1 >> 62) != 0 || ((num1 >> 61) & 1) != 1) {
    return false;
  }
  return ((num1 >> 17) & ad_utility::bitMaskForLowerBits(15)) == 0;
}

// Compress a `LaneRef`-like first number: keep its bits `[0, 17)` and
// `[32, 61)` and pack them together with `num2` into `[middle:29][lower:17]
// [num2:4]`.
uint64_t packRefNumbers(uint64_t num1, uint64_t num2) {
  uint64_t lowerBits = num1 & ad_utility::bitMaskForLowerBits(17);
  uint64_t middleBits = (num1 >> 32) & ad_utility::bitMaskForLowerBits(29);
  return (middleBits << 21) | (lowerBits << 4) | num2;
}

// The inverse of `packRefNumbers`.
std::pair<uint64_t, uint64_t> unpackRefNumbers(uint64_t encoded) {
  uint64_t num2 = encoded & 0xF;
  uint64_t lowerBits = (encoded >> 4) & ad_utility::bitMaskForLowerBits(17);
  uint64_t middleBits = (encoded >> 21) & ad_utility::bitMaskForLowerBits(29);
  uint64_t num1 = (1ULL << 61) | (middleBits << 32) | lowerBits;
  return {num1, num2};
}

// Remove the `constraints` (sorted, non-overlapping bit ranges) from the
// `value` and concatenate the remaining bits. This is the legacy
// `compressMultiConstraintValue`.
uint64_t compressMultiConstraintValue(
    uint64_t value, const std::vector<FixedBitRange>& constraints) {
  uint64_t result = 0;
  size_t outputBitPos = 0;
  size_t inputBitPos = 0;
  for (const auto& constraint : constraints) {
    size_t numBitsToCopy = constraint.begin_ - inputBitPos;
    if (numBitsToCopy > 0) {
      uint64_t mask = ad_utility::bitMaskForLowerBits(numBitsToCopy);
      result |= ((value >> inputBitPos) & mask) << outputBitPos;
      outputBitPos += numBitsToCopy;
    }
    inputBitPos = constraint.end_;
  }
  if (inputBitPos < 64) {
    result |= (value >> inputBitPos) << outputBitPos;
  }
  return result;
}

// The inverse of `compressMultiConstraintValue`: reinsert the `constraints`
// with their fixed values. This is the legacy `decompressMultiConstraintValue`.
uint64_t decompressMultiConstraintValue(
    uint64_t compressed, const std::vector<FixedBitRange>& constraints) {
  uint64_t result = 0;
  size_t inputBitPos = 0;
  size_t outputBitPos = 0;
  for (const auto& constraint : constraints) {
    size_t numBitsToCopy = constraint.begin_ - outputBitPos;
    if (numBitsToCopy > 0) {
      uint64_t mask = ad_utility::bitMaskForLowerBits(numBitsToCopy);
      result |= ((compressed >> inputBitPos) & mask) << outputBitPos;
      inputBitPos += numBitsToCopy;
    }
    result |= constraint.value_ << constraint.begin_;
    outputBitPos = constraint.end_;
  }
  if (inputBitPos < 64) {
    result |= (compressed >> inputBitPos) << outputBitPos;
  }
  return result;
}

// Return the payload of `num1`, `num2`, `num3` for the `RangePattern` and
// `ValRangePattern` schemes, or `std::nullopt` if the constraints are violated.
// The two schemes differ in the number of bits of `num2` and `num3`
// (`numBitsSmall`) and in how many bits `num1` is shifted (`shiftNum1`); `num2`
// is shifted by 11 bits in both.
std::optional<uint64_t> encodeRangeLike(const std::vector<uint64_t>& numbers,
                                        size_t numBitsSmall, size_t shiftNum1) {
  AD_CORRECTNESS_CHECK(numbers.size() == 3);
  auto [num1, num2, num3] = std::tie(numbers[0], numbers[1], numbers[2]);
  if (!isValid32BitRangeNumber(num1)) {
    return std::nullopt;
  }
  if (num2 >= (1ULL << numBitsSmall) || num3 >= (1ULL << numBitsSmall)) {
    return std::nullopt;
  }
  uint64_t num1Compressed = num1 & ad_utility::bitMaskForLowerBits(29);
  return (num1Compressed << shiftNum1) | (num2 << 11) | num3;
}

// Append `num1_num2_num3<suffixChar>` to `result`.
void appendThreeNumbers(std::string& result, uint64_t num1, uint64_t num2,
                        uint64_t num3, char suffixChar) {
  absl::StrAppend(&result, num1, "_", num2, "_", num3);
  result.push_back(suffixChar);
}

// Return `std::nullopt` if the `payload` does not fit into `NumBitsEncoding`
// bits, which the legacy encoder checked for every scheme.
std::optional<uint64_t> checkPayloadFits(std::optional<uint64_t> payload) {
  if (payload.has_value() &&
      payload.value() >= (1ULL << LegacyEncodedIriManager::NumBitsEncoding)) {
    return std::nullopt;
  }
  return payload;
}

// The legacy encoder required the compressed value of a single 64-bit number
// (the number without its fixed bit ranges) to fit into `NumBitsEncoding` bits,
// which limits the value of the highest unconstrained bits to zero if fewer
// than `64 - NumBitsEncoding` bits are fixed. The current `encodedIri::Part`
// expresses this limit as additional fixed bit ranges with the value zero, so
// add them to the `constraints` (which must be sorted and non-overlapping) if
// necessary.
std::vector<FixedBitRange> withExcessBitsFixed(
    std::vector<FixedBitRange> constraints) {
  std::array<bool, 64> isConstrained{};
  size_t numConstrained = 0;
  for (const auto& constraint : constraints) {
    for (size_t bit = constraint.begin_; bit < constraint.end_; ++bit) {
      isConstrained[bit] = true;
    }
    numConstrained += constraint.numBits();
  }
  size_t numRemaining = 64 - numConstrained;
  if (numRemaining <= LegacyEncodedIriManager::NumBitsEncoding) {
    return constraints;
  }
  size_t numExcess = numRemaining - LegacyEncodedIriManager::NumBitsEncoding;
  // Fix the highest `numExcess` unconstrained bits to zero, grouped into
  // maximal ranges.
  std::optional<FixedBitRange> current;
  for (size_t bit = 64; bit > 0 && numExcess > 0; --bit) {
    size_t position = bit - 1;
    if (isConstrained[position]) {
      if (current.has_value()) {
        constraints.push_back(current.value());
        current.reset();
      }
      continue;
    }
    --numExcess;
    if (current.has_value()) {
      current->begin_ = position;
    } else {
      current = FixedBitRange{position, position + 1, 0};
    }
  }
  if (current.has_value()) {
    constraints.push_back(current.value());
  }
  ql::ranges::sort(constraints, {}, &FixedBitRange::begin_);
  return constraints;
}

// Parse the bit range constraints of a `"prefix-configs"` item.
std::vector<FixedBitRange> parseBitRangeConstraints(
    const nlohmann::json& json) {
  std::vector<FixedBitRange> constraints;
  for (const auto& c : json) {
    FixedBitRange range{static_cast<size_t>(c.at("bitStart")),
                        static_cast<size_t>(c.at("bitEnd")),
                        static_cast<uint64_t>(c.at("value"))};
    if (range.begin_ >= range.end_ || range.end_ > 64 ||
        range.value_ > ad_utility::bitMaskForLowerBits(range.numBits())) {
      throw std::runtime_error{absl::StrCat(
          "Invalid bit range constraint in the legacy encoded-IRI "
          "configuration: bitStart = ",
          range.begin_, ", bitEnd = ", range.end_, ", value = ", range.value_)};
    }
    constraints.push_back(range);
  }
  ql::ranges::sort(constraints, {}, &FixedBitRange::begin_);
  for (size_t i = 1; i < constraints.size(); ++i) {
    if (constraints[i - 1].end_ > constraints[i].begin_) {
      throw std::runtime_error{
          "Overlapping bit range constraints in the legacy encoded-IRI "
          "configuration"};
    }
  }
  return constraints;
}
}  // namespace

// _____________________________________________________________________________
LegacyEncodedIriManager::LegacyEncodedIriManager(
    std::vector<LegacyPrefixConfig> configs)
    : configs_{std::move(configs)} {
  if (configs_.size() > (1ULL << NumBitsTags)) {
    throw std::runtime_error{absl::StrCat(
        "The legacy encoded-IRI configuration has ", configs_.size(),
        " prefixes, but at most ", 1ULL << NumBitsTags, " are possible")};
  }
  for (const auto& config : configs_) {
    if (!ql::starts_with(config.prefix_, '<')) {
      throw std::runtime_error{absl::StrCat(
          "The prefix \"", config.prefix_,
          "\" of the legacy encoded-IRI configuration does not start with "
          "`<`")};
    }
  }
}

// _____________________________________________________________________________
LegacyEncodedIriManager LegacyEncodedIriManager::fromJson(
    const nlohmann::json& json) {
  static constexpr std::string_view keyConfigs = "prefix-configs";
  static constexpr std::string_view keyPlain =
      "prefixes-with-leading-angle-brackets";
  std::vector<LegacyPrefixConfig> configs;
  if (json.contains(keyConfigs)) {
    for (const auto& item : json.at(keyConfigs)) {
      LegacyPrefixConfig config;
      config.prefix_ = static_cast<std::string>(item.at("prefix"));
      if (item.contains("bitRangeConstraints")) {
        config.bitRangeConstraints_ =
            parseBitRangeConstraints(item.at("bitRangeConstraints"));
        // NOTE: The legacy reader silently treated an empty list as the plain
        // mode; it is rejected here because the legacy writer never wrote the
        // key for an empty list, so this indicates a corrupt configuration.
        if (config.bitRangeConstraints_.empty()) {
          throw std::runtime_error{absl::StrCat(
              "Empty list of bit range constraints for the prefix \"",
              config.prefix_, "\" in the legacy encoded-IRI configuration")};
        }
      } else if (item.contains("zeroBitStart") && item.contains("zeroBitEnd")) {
        size_t begin = static_cast<size_t>(item.at("zeroBitStart"));
        size_t end = static_cast<size_t>(item.at("zeroBitEnd"));
        // NOTE: A range that ends at bit 64 is rejected, because the legacy
        // encoder did not handle it either (it shifted by `end`, which is
        // undefined for 64).
        if (begin >= end || end >= 64) {
          throw std::runtime_error{absl::StrCat(
              "Invalid zero bit range [", begin, ", ", end,
              ") for the prefix \"", config.prefix_,
              "\" in the legacy encoded-IRI configuration (the end has to be "
              "smaller than 64)")};
        }
        config.zeroBitRange_ = std::make_pair(begin, end);
      } else if (item.contains("specialEncoding")) {
        // NOTE: The legacy reader treated the value `0` as `None`; it is
        // rejected here because the legacy writer only wrote the key for a
        // special encoding, so `0` indicates a corrupt configuration.
        int encoding = static_cast<int>(item.at("specialEncoding"));
        if (encoding < static_cast<int>(LegacySpecialEncoding::RangePattern) ||
            encoding > static_cast<int>(LegacySpecialEncoding::StopLoc32)) {
          throw std::runtime_error{absl::StrCat(
              "Unknown special encoding ", encoding, " for the prefix \"",
              config.prefix_, "\" in the legacy encoded-IRI configuration")};
        }
        config.specialEncoding_ = static_cast<LegacySpecialEncoding>(encoding);
      }
      configs.push_back(std::move(config));
    }
  } else if (json.contains(keyPlain)) {
    for (const auto& prefix : json.at(keyPlain)) {
      LegacyPrefixConfig config;
      config.prefix_ = static_cast<std::string>(prefix);
      configs.push_back(std::move(config));
    }
  } else {
    throw std::runtime_error{absl::StrCat(
        "The legacy encoded-IRI configuration has neither the key \"",
        keyConfigs, "\" nor the key \"", keyPlain, "\"")};
  }
  return LegacyEncodedIriManager{std::move(configs)};
}

// _____________________________________________________________________________
std::string LegacyEncodedIriManager::toString(uint64_t encodedVal) const {
  uint64_t tag = encodedVal >> NumBitsEncoding;
  uint64_t payload =
      encodedVal & ad_utility::bitMaskForLowerBits(NumBitsEncoding);
  if (tag >= configs_.size()) {
    throw std::runtime_error{absl::StrCat("The legacy encoded IRI has the tag ",
                                          tag, ", but only ", configs_.size(),
                                          " prefixes are configured")};
  }
  const auto& config = configs_[tag];
  std::string result = config.prefix_;
  decodePayload(result, config, payload);
  result.push_back('>');
  return result;
}

// _____________________________________________________________________________
std::string LegacyEncodedIriManager::toStringFromLegacyBits(
    uint64_t legacyBits) const {
  AD_CONTRACT_CHECK(
      legacyDatatypeBits(legacyBits) ==
          static_cast<uint64_t>(LegacyDatatype::EncodedVal),
      "The legacy datatype of the `Id` must be `EncodedVal` to decode it as "
      "an encoded IRI");
  return toString(legacyDataBits(legacyBits));
}

// _____________________________________________________________________________
void LegacyEncodedIriManager::decodePayload(std::string& result,
                                            const LegacyPrefixConfig& config,
                                            uint64_t payload) {
  using ad_utility::bitMaskForLowerBits;
  switch (config.specialEncoding_) {
    case LegacySpecialEncoding::RangePattern: {
      uint64_t num3 = payload & bitMaskForLowerBits(11);
      uint64_t num2 = (payload >> 11) & bitMaskForLowerBits(10);
      uint64_t num1 =
          (1ULL << 29) | ((payload >> 21) & bitMaskForLowerBits(29));
      appendThreeNumbers(result, num1, num2, num3, 'P');
      return;
    }
    case LegacySpecialEncoding::ValRangePattern: {
      uint64_t num3 = payload & bitMaskForLowerBits(11);
      uint64_t num2 = (payload >> 11) & bitMaskForLowerBits(11);
      uint64_t num1 =
          (1ULL << 29) | ((payload >> 22) & bitMaskForLowerBits(29));
      appendThreeNumbers(result, num1, num2, num3, 'M');
      return;
    }
    case LegacySpecialEncoding::LaneRef:
    case LegacySpecialEncoding::RoadRef:
    case LegacySpecialEncoding::SpeedProfile: {
      auto [num1, num2] = unpackRefNumbers(payload);
      absl::StrAppend(&result, num1, "_", num2);
      return;
    }
    case LegacySpecialEncoding::StopLoc:
    case LegacySpecialEncoding::StopLoc32: {
      bool is32BitVariant = (payload >> 50) & 1;
      if (is32BitVariant) {
        uint64_t num2 = payload & bitMaskForLowerBits(18);
        uint64_t num1 = (payload >> 18) & bitMaskForLowerBits(32);
        absl::StrAppend(&result, num1, "_", num2);
      } else {
        auto [num1, num2] = unpackRefNumbers(payload);
        absl::StrAppend(&result, num1, "_", num2);
      }
      return;
    }
    case LegacySpecialEncoding::None:
      break;
  }
  if (config.isMultiConstraintMode()) {
    absl::StrAppend(&result, decompressMultiConstraintValue(
                                 payload, config.bitRangeConstraints_));
  } else if (config.zeroBitRange_.has_value()) {
    auto [begin, end] = config.zeroBitRange_.value();
    uint64_t lowerBits = payload & bitMaskForLowerBits(begin);
    uint64_t upperBits = payload >> begin;
    absl::StrAppend(&result, (upperBits << end) | lowerBits);
  } else {
    encodedIri::decodeDigits(result, payload, NumBitsEncoding);
  }
}

// _____________________________________________________________________________
std::optional<uint64_t> LegacyEncodedIriManager::encode(
    std::string_view iri) const {
  auto it = ql::ranges::find_if(configs_, [&iri](const LegacyPrefixConfig& c) {
    return ql::starts_with(iri, c.prefix_);
  });
  if (it == configs_.end()) {
    return std::nullopt;
  }
  uint64_t tag = static_cast<uint64_t>(it - configs_.begin());
  auto payload = encodePayload(*it, iri.substr(it->prefix_.size()));
  if (!payload.has_value()) {
    return std::nullopt;
  }
  return payload.value() | (tag << NumBitsEncoding);
}

// _____________________________________________________________________________
std::optional<uint64_t> LegacyEncodedIriManager::encodePayload(
    const LegacyPrefixConfig& config, std::string_view suffix) {
  switch (config.specialEncoding_) {
    case LegacySpecialEncoding::RangePattern: {
      auto numbers = matchNumbers(suffix, {"_", "_", "P>"});
      if (!numbers.has_value()) {
        return std::nullopt;
      }
      return checkPayloadFits(encodeRangeLike(numbers.value(), 8, 21));
    }
    case LegacySpecialEncoding::ValRangePattern: {
      auto numbers = matchNumbers(suffix, {"_", "_", "M>"});
      if (!numbers.has_value()) {
        return std::nullopt;
      }
      return checkPayloadFits(encodeRangeLike(numbers.value(), 11, 22));
    }
    case LegacySpecialEncoding::LaneRef:
    case LegacySpecialEncoding::RoadRef:
    case LegacySpecialEncoding::SpeedProfile: {
      auto numbers = matchNumbers(suffix, {"_", ">"});
      if (!numbers.has_value()) {
        return std::nullopt;
      }
      auto [num1, num2] = std::tie(numbers.value()[0], numbers.value()[1]);
      if (num2 >= 16 || !isValid64BitRefNumber(num1)) {
        return std::nullopt;
      }
      return checkPayloadFits(packRefNumbers(num1, num2));
    }
    case LegacySpecialEncoding::StopLoc:
    case LegacySpecialEncoding::StopLoc32: {
      auto numbers = matchNumbers(suffix, {"_", ">"});
      if (!numbers.has_value()) {
        return std::nullopt;
      }
      auto [num1, num2] = std::tie(numbers.value()[0], numbers.value()[1]);
      // The 64-bit variant is tried first, because it is more restrictive.
      if (num2 < 16 && isValid64BitRefNumber(num1)) {
        auto payload = checkPayloadFits(packRefNumbers(num1, num2));
        if (payload.has_value()) {
          return payload;
        }
      }
      if (num1 < (1ULL << 32) && num2 < (1ULL << 18)) {
        return checkPayloadFits((1ULL << 50) | (num1 << 18) | num2);
      }
      return std::nullopt;
    }
    case LegacySpecialEncoding::None:
      break;
  }

  // The remaining modes all consist of a single number followed by the closing
  // `>`.
  auto digits = encodedIri::leadingDigits(suffix);
  if (digits.empty() || suffix.substr(digits.size()) != ">") {
    return std::nullopt;
  }
  if (config.isMultiConstraintMode()) {
    auto value = parseLegacyDecimal(digits);
    if (!value.has_value()) {
      return std::nullopt;
    }
    for (const auto& constraint : config.bitRangeConstraints_) {
      uint64_t mask = ad_utility::bitMaskForLowerBits(constraint.numBits());
      if (((value.value() >> constraint.begin_) & mask) != constraint.value_) {
        return std::nullopt;
      }
    }
    return checkPayloadFits(compressMultiConstraintValue(
        value.value(), config.bitRangeConstraints_));
  }
  if (config.zeroBitRange_.has_value()) {
    auto value = parseLegacyDecimal(digits);
    if (!value.has_value()) {
      return std::nullopt;
    }
    auto [begin, end] = config.zeroBitRange_.value();
    for (size_t bit = begin; bit < end; ++bit) {
      if ((value.value() >> bit) & 1) {
        return std::nullopt;
      }
    }
    uint64_t lowerBits = value.value() & ad_utility::bitMaskForLowerBits(begin);
    uint64_t upperBits = value.value() >> end;
    return checkPayloadFits((upperBits << begin) | lowerBits);
  }
  if (digits.size() > NumDigits) {
    return std::nullopt;
  }
  return encodedIri::encodeDigits(digits, NumBitsEncoding);
}

// _____________________________________________________________________________
std::vector<encodedIri::Pattern> LegacyEncodedIriManager::toPatterns() const {
  using encodedIri::Part;
  using encodedIri::Pattern;
  std::vector<Pattern> patterns;
  // The first number of the `LaneRef`-like schemes and of the 64-bit variant
  // of `StopLoc`.
  const Part refNumber{64, {{17, 32, 0}, {61, 64, 1}}, "_"};
  // The first number of the `RangePattern` and `ValRangePattern` schemes.
  const Part rangeNumber{32, {{29, 32, 1}}, "_"};
  for (const auto& config : configs_) {
    // The current `EncodedIriManager` adds the leading `<` itself.
    std::string prefix = config.prefix_.substr(1);
    switch (config.specialEncoding_) {
      case LegacySpecialEncoding::RangePattern:
        patterns.push_back(
            Pattern{prefix, {rangeNumber, Part{8, {}, "_"}, Part{8, {}, "P"}}});
        continue;
      case LegacySpecialEncoding::ValRangePattern:
        patterns.push_back(Pattern{
            prefix, {rangeNumber, Part{11, {}, "_"}, Part{11, {}, "M"}}});
        continue;
      case LegacySpecialEncoding::LaneRef:
      case LegacySpecialEncoding::RoadRef:
      case LegacySpecialEncoding::SpeedProfile:
        patterns.push_back(Pattern{prefix, {refNumber, Part{4, {}, ""}}});
        continue;
      case LegacySpecialEncoding::StopLoc:
      case LegacySpecialEncoding::StopLoc32:
        patterns.push_back(Pattern{prefix, {refNumber, Part{4, {}, ""}}});
        patterns.push_back(
            Pattern{prefix, {Part{32, {}, "_"}, Part{18, {}, ""}}});
        continue;
      case LegacySpecialEncoding::None:
        break;
    }
    if (config.isMultiConstraintMode()) {
      patterns.push_back(Pattern{
          prefix,
          {Part{64, withExcessBitsFixed(config.bitRangeConstraints_), ""}}});
    } else if (config.zeroBitRange_.has_value()) {
      auto [begin, end] = config.zeroBitRange_.value();
      patterns.push_back(Pattern{
          prefix, {Part{64, withExcessBitsFixed({{begin, end, 0}}), ""}}});
    } else {
      patterns.push_back(
          encodedIri::plainPrefixPattern(prefix, NumBitsEncoding));
    }
  }
  return patterns;
}

// _____________________________________________________________________________
EncodedIriManager LegacyEncodedIriManager::makeCurrentManager() const {
  return EncodedIriManager{{}, toPatterns()};
}

}  // namespace qlever::blobConverter
