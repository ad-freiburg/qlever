// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/EncodedIriPattern.h"

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>

#include <charconv>
#include <stdexcept>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/span.h"

namespace encodedIri {

namespace {
// The JSON keys of the members of the structs in this file.
constexpr std::string_view beginKey = "begin";
constexpr std::string_view endKey = "end";
constexpr std::string_view valueKey = "value";
constexpr std::string_view numBitsKey = "num-bits";
constexpr std::string_view fixedBitRangesKey = "fixed-bit-ranges";
constexpr std::string_view suffixKey = "suffix";
constexpr std::string_view encodingKey = "encoding";
constexpr std::string_view prefixKey = "prefix-with-leading-angle-bracket";
constexpr std::string_view partsKey = "parts";

// The JSON representations of the `NumberEncoding` enum.
constexpr std::string_view binaryEncodingName = "binary";
constexpr std::string_view nibblesEncodingName = "nibbles";

// The string `[begin, end)` for the error messages about a bit range.
std::string rangeToString(uint64_t begin, uint64_t end) {
  return absl::StrCat("[", begin, ", ", end, ")");
}

// Throw a `std::runtime_error` that reports an invalid `pattern`.
[[noreturn]] void throwInvalidPattern(const Pattern& pattern,
                                      std::string_view message) {
  throw std::runtime_error(
      absl::StrCat("The pattern for encoded IRIs with the prefix \"",
                   pattern.prefix_, "\" is invalid: ", message));
}
}  // namespace

// _____________________________________________________________________________
FixedBitRange::FixedBitRange(uint64_t begin, uint64_t end, uint64_t value)
    : begin_{static_cast<uint8_t>(begin)},
      end_{static_cast<uint8_t>(end)},
      value_{value} {
  auto throwInvalid = [begin, end](std::string_view message) {
    throw std::runtime_error(absl::StrCat(
        "The fixed bit range ", rangeToString(begin, end),
        " of a number of a pattern for encoded IRIs is invalid: ", message));
  };
  if (begin >= end || end > 64) {
    throwInvalid("it is empty or not contained in the 64 bits of a number");
  }
  if (value > ad_utility::bitMaskForLowerBits(end - begin)) {
    throwInvalid(absl::StrCat("the value ", value, " doesn't fit into it"));
  }
}

// _____________________________________________________________________________
Part::Part(uint64_t numBits, std::vector<FixedBitRange> fixedBitRanges,
           std::string suffix, NumberEncoding encoding)
    : numBits_{static_cast<uint8_t>(numBits)},
      fixedBitRanges_{std::move(fixedBitRanges)},
      suffix_{std::move(suffix)},
      encoding_{encoding} {
  auto throwInvalid = [numBits, this](std::string_view message) {
    throw std::runtime_error(absl::StrCat(
        "The number with ", numBits, " bits and the suffix \"", suffix_,
        "\" of a pattern for encoded IRIs is invalid: ", message));
  };
  if (numBits == 0 || numBits > 64) {
    throwInvalid("only 1 to 64 bits are supported");
  }
  // The ranges themselves are valid (see the constructor of `FixedBitRange`),
  // so only their relation to each other and to `numBits` has to be checked.
  size_t lastEnd = 0;
  for (const auto& range : fixedBitRanges_) {
    if (range.end_ > numBits) {
      throwInvalid(absl::StrCat(
          "the fixed bit range ", rangeToString(range.begin_, range.end_),
          " is not contained in the ", rangeToString(0, numBits), " bits"));
    }
    if (range.begin_ < lastEnd) {
      throwInvalid(
          "the fixed bit ranges have to be sorted and must not overlap");
    }
    lastEnd = range.end_;
  }
  if (encoding_ == NumberEncoding::Nibbles &&
      (numBits % NibbleSize != 0 || !fixedBitRanges_.empty())) {
    throwInvalid(
        "the nibble encoding requires a multiple of four bits and no fixed bit "
        "ranges");
  }
  if (suffix_.find_first_of("<>") != std::string::npos) {
    throwInvalid("the suffix must not contain an angle bracket");
  }
  if (!suffix_.empty() &&
      absl::ascii_isdigit(static_cast<unsigned char>(suffix_[0]))) {
    throwInvalid(
        "the suffix must not start with a digit, because the digits of the "
        "number are matched greedily");
  }
}

// _____________________________________________________________________________
Pattern::Pattern(std::string prefix, std::vector<Part> parts)
    : prefix_{std::move(prefix)}, parts_{std::move(parts)} {
  if (prefix_.find('>') != std::string::npos) {
    throwInvalidPattern(*this, "the prefix must not contain a `>`");
  }
  if (parts_.empty()) {
    throwInvalidPattern(*this, "it must contain at least one number");
  }
  // All suffixes but the last one have to be non-empty, else two consecutive
  // numbers could not be told apart.
  for (const auto& part : ql::span{parts_}.first(parts_.size() - 1)) {
    if (part.suffix_.empty()) {
      throwInvalidPattern(*this,
                          "only the last number of a pattern may be followed "
                          "by an empty suffix");
    }
  }
}

// _____________________________________________________________________________
Pattern plainPrefixPattern(std::string prefix, size_t numBits) {
  return Pattern{std::move(prefix),
                 {Part{numBits, {}, "", NumberEncoding::Nibbles}}};
}

// _____________________________________________________________________________
bool isPlainPrefixPattern(const Pattern& pattern, size_t numBits) {
  if (pattern.parts_.size() != 1) {
    return false;
  }
  const auto& part = pattern.parts_.at(0);
  return part.encoding_ == NumberEncoding::Nibbles &&
         part.numBits_ == numBits && part.suffix_.empty() &&
         part.fixedBitRanges_.empty();
}

// _____________________________________________________________________________
void validatePattern(const Pattern& pattern, size_t numBitsAvailable) {
  // The payload is shifted by the number of bits of a pattern, which is
  // undefined behavior for a shift by 64 or more.
  AD_CONTRACT_CHECK(numBitsAvailable < 64);
  if (pattern.numBitsStored() > numBitsAvailable) {
    throwInvalidPattern(pattern,
                        absl::StrCat("it requires ", pattern.numBitsStored(),
                                     " bits, but only ", numBitsAvailable,
                                     " bits are available"));
  }
}

// _____________________________________________________________________________
std::optional<uint64_t> compressNumber(const Part& part, uint64_t value) {
  if (value > ad_utility::bitMaskForLowerBits(part.numBits_)) {
    return std::nullopt;
  }
  uint64_t result = 0;
  // The next bit of `value` that hasn't been looked at yet, and the next bit of
  // `result` that hasn't been written yet.
  size_t inputPos = 0;
  size_t outputPos = 0;
  // Copy the bits `[inputPos, end)` of `value` (which are not part of a fixed
  // bit range) to the next free bits of `result`, and advance both positions.
  auto copyBits = [&result, &inputPos, &outputPos, value](size_t end) {
    size_t numBits = end - inputPos;
    if (numBits > 0) {
      result |= ((value >> inputPos) & ad_utility::bitMaskForLowerBits(numBits))
                << outputPos;
      outputPos += numBits;
      inputPos = end;
    }
  };
  for (const auto& range : part.fixedBitRanges_) {
    // Copy the variable bits before the fixed range.
    copyBits(range.begin_);
    // The bits of `value` in the fixed range must have the fixed value, else
    // the `value` doesn't match the `part` and cannot be encoded.
    if (((value >> range.begin_) &
         ad_utility::bitMaskForLowerBits(range.numBits())) != range.value_) {
      return std::nullopt;
    }
    // Skip the fixed range, it is not stored.
    inputPos = range.end_;
  }
  // Copy the variable bits after the last fixed range.
  copyBits(part.numBits_);
  return result;
}

// _____________________________________________________________________________
uint64_t decompressNumber(const Part& part, uint64_t compressedValue) {
  uint64_t result = 0;
  // The next bit of `compressedValue` that hasn't been looked at yet, and the
  // next bit of `result` that hasn't been written yet.
  size_t inputPos = 0;
  size_t outputPos = 0;
  // Fill the bits `[outputPos, end)` of `result` (which are not part of a fixed
  // bit range) with the next bits of `compressedValue`, and advance both
  // positions.
  auto copyBits = [&result, &inputPos, &outputPos,
                   compressedValue](size_t end) {
    size_t numBits = end - outputPos;
    if (numBits > 0) {
      result |= ((compressedValue >> inputPos) &
                 ad_utility::bitMaskForLowerBits(numBits))
                << outputPos;
      inputPos += numBits;
      outputPos = end;
    }
  };
  for (const auto& range : part.fixedBitRanges_) {
    // Fill the variable bits before the fixed range.
    copyBits(range.begin_);
    // Reinsert the fixed value, which was not stored.
    result |= range.value_ << range.begin_;
    outputPos = range.end_;
  }
  // Fill the variable bits after the last fixed range.
  copyBits(part.numBits_);
  return result;
}

// _____________________________________________________________________________
void decompressNumber(std::string& result, const Part& part,
                      uint64_t compressedValue) {
  absl::StrAppend(&result, decompressNumber(part, compressedValue));
}

// _____________________________________________________________________________
std::string_view leadingDigits(std::string_view input) {
  auto end = ql::ranges::find_if_not(input, absl::ascii_isdigit);
  return input.substr(0, end - input.begin());
}

// _____________________________________________________________________________
std::optional<uint64_t> parseDecimal(std::string_view input) {
  if (input.empty() || (input.size() > 1 && input[0] == '0')) {
    return std::nullopt;
  }
  uint64_t value = 0;
  const char* end = input.data() + input.size();
  auto [ptr, ec] = std::from_chars(input.data(), end, value);
  // The `ec` is `result_out_of_range` if the number doesn't fit into a
  // `uint64_t`; `ptr != end` means that the `input` contained a non-digit.
  if (ec != std::errc{} || ptr != end) {
    return std::nullopt;
  }
  return value;
}

// _____________________________________________________________________________
std::optional<uint64_t> encodePayload(const Pattern& pattern,
                                      std::string_view rest) {
  // The payload is shifted by the number of stored bits, see the header.
  AD_CONTRACT_CHECK(pattern.numBitsStored() < 64);
  uint64_t payload = 0;
  for (const auto& part : pattern.parts_) {
    auto digits = leadingDigits(rest);
    if (digits.empty()) {
      return std::nullopt;
    }
    rest.remove_prefix(digits.size());
    if (!ql::starts_with(rest, part.suffix_)) {
      return std::nullopt;
    }
    rest.remove_prefix(part.suffix_.size());
    std::optional<uint64_t> stored;
    if (part.encoding_ == NumberEncoding::Nibbles) {
      if (digits.size() * NibbleSize > part.numBits_) {
        return std::nullopt;
      }
      stored = encodeDigitsAsNibbles(digits, part.numBits_);
    } else {
      auto value = parseDecimal(digits);
      if (!value.has_value()) {
        return std::nullopt;
      }
      stored = compressNumber(part, value.value());
    }
    if (!stored.has_value()) {
      return std::nullopt;
    }
    payload = (payload << part.numBitsStored()) | stored.value();
  }
  if (rest != ">") {
    return std::nullopt;
  }
  return payload;
}

// _____________________________________________________________________________
std::string decodeToIri(const Pattern& pattern, uint64_t payload) {
  std::string result;
  // A decimal number needs at most 20 characters; the suffixes are
  // typically short.
  result.reserve(pattern.prefix_.size() + pattern.parts_.size() * 24 + 1);
  result = pattern.prefix_;
  // The first part is stored in the most significant bits of the payload,
  // which is shifted by the number of stored bits, see the header.
  size_t shift = pattern.numBitsStored();
  AD_CONTRACT_CHECK(shift < 64);
  for (const auto& part : pattern.parts_) {
    size_t numBits = part.numBitsStored();
    shift -= numBits;
    uint64_t stored =
        (payload >> shift) & ad_utility::bitMaskForLowerBits(numBits);
    if (part.encoding_ == NumberEncoding::Nibbles) {
      decodeNibblesToDigits(result, stored, part.numBits_);
    } else {
      decompressNumber(result, part, stored);
    }
    result.append(part.suffix_);
  }
  result.push_back('>');
  return result;
}

}  // namespace encodedIri

// _____________________________________________________________________________
void nlohmann::adl_serializer<encodedIri::FixedBitRange>::to_json(
    json& j, const encodedIri::FixedBitRange& range) {
  j[encodedIri::beginKey] = range.begin_;
  j[encodedIri::endKey] = range.end_;
  j[encodedIri::valueKey] = range.value_;
}

// _____________________________________________________________________________
encodedIri::FixedBitRange
nlohmann::adl_serializer<encodedIri::FixedBitRange>::from_json(const json& j) {
  using namespace encodedIri;
  return FixedBitRange{j.at(beginKey).get<uint64_t>(),
                       j.at(endKey).get<uint64_t>(),
                       j.at(valueKey).get<uint64_t>()};
}

// _____________________________________________________________________________
void nlohmann::adl_serializer<encodedIri::Part>::to_json(
    json& j, const encodedIri::Part& part) {
  using namespace encodedIri;
  j[numBitsKey] = part.numBits_;
  j[fixedBitRangesKey] = part.fixedBitRanges_;
  j[suffixKey] = part.suffix_;
  j[encodingKey] = part.encoding_ == NumberEncoding::Nibbles
                       ? nibblesEncodingName
                       : binaryEncodingName;
}

// _____________________________________________________________________________
encodedIri::Part nlohmann::adl_serializer<encodedIri::Part>::from_json(
    const json& j) {
  using namespace encodedIri;
  auto encodingName = j.at(encodingKey).get<std::string>();
  NumberEncoding encoding;
  if (encodingName == nibblesEncodingName) {
    encoding = NumberEncoding::Nibbles;
  } else if (encodingName == binaryEncodingName) {
    encoding = NumberEncoding::Binary;
  } else {
    throw std::runtime_error(absl::StrCat(
        "Unknown encoding \"", encodingName,
        "\" for a number of an encoded IRI, expected \"", binaryEncodingName,
        "\" or \"", nibblesEncodingName, "\""));
  }
  return Part{j.at(numBitsKey).get<uint64_t>(),
              j.at(fixedBitRangesKey).get<std::vector<FixedBitRange>>(),
              j.at(suffixKey).get<std::string>(), encoding};
}

// _____________________________________________________________________________
void nlohmann::adl_serializer<encodedIri::Pattern>::to_json(
    json& j, const encodedIri::Pattern& pattern) {
  j[encodedIri::prefixKey] = pattern.prefix_;
  j[encodedIri::partsKey] = pattern.parts_;
}

// _____________________________________________________________________________
encodedIri::Pattern nlohmann::adl_serializer<encodedIri::Pattern>::from_json(
    const json& j) {
  using namespace encodedIri;
  return Pattern{j.at(prefixKey).get<std::string>(),
                 j.at(partsKey).get<std::vector<Part>>()};
}
