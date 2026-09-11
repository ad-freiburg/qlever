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

#include <limits>
#include <stdexcept>

namespace encodedIri {

namespace {
// The JSON keys of the members of the structs in this file.
constexpr const char* beginKey = "begin";
constexpr const char* endKey = "end";
constexpr const char* valueKey = "value";
constexpr const char* numBitsKey = "num-bits";
constexpr const char* fixedBitRangesKey = "fixed-bit-ranges";
constexpr const char* separatorKey = "separator";
constexpr const char* encodingKey = "encoding";
constexpr const char* prefixKey = "prefix-with-leading-angle-bracket";
constexpr const char* partsKey = "parts";

// The JSON representations of the `NumberEncoding` enum.
constexpr const char* binaryEncodingName = "binary";
constexpr const char* digitsEncodingName = "digits";

// Throw a `std::runtime_error` that reports an invalid `pattern`.
[[noreturn]] void throwInvalidPattern(const Pattern& pattern,
                                      std::string_view message) {
  throw std::runtime_error(
      absl::StrCat("The pattern for encoded IRIs with the prefix \"",
                   pattern.prefix_, "\" is invalid: ", message));
}
}  // namespace

// _____________________________________________________________________________
Pattern plainPrefixPattern(std::string prefix, size_t numBits) {
  return Pattern{std::move(prefix),
                 {Part{numBits, {}, "", NumberEncoding::Digits}}};
}

// _____________________________________________________________________________
bool isPlainPrefixPattern(const Pattern& pattern, size_t numBits) {
  if (pattern.parts_.size() != 1) {
    return false;
  }
  const auto& part = pattern.parts_.at(0);
  return part.encoding_ == NumberEncoding::Digits && part.numBits_ == numBits &&
         part.separator_.empty() && part.fixedBitRanges_.empty();
}

// _____________________________________________________________________________
void validatePattern(const Pattern& pattern, size_t numBitsAvailable) {
  // The payload is shifted by the number of bits of a pattern, which is
  // undefined behavior for a shift by 64 or more.
  AD_CONTRACT_CHECK(numBitsAvailable < 64);
  if (pattern.prefix_.find('>') != std::string::npos) {
    throwInvalidPattern(pattern, "the prefix must not contain a `>`");
  }
  if (pattern.parts_.empty()) {
    throwInvalidPattern(pattern, "it must contain at least one number");
  }
  for (const auto& part : pattern.parts_) {
    if (part.numBits_ == 0 || part.numBits_ > 64) {
      throwInvalidPattern(
          pattern, absl::StrCat("a number has ", part.numBits_,
                                " bits, but only 1 to 64 bits are supported"));
    }
    size_t lastEnd = 0;
    for (const auto& range : part.fixedBitRanges_) {
      if (range.begin_ >= range.end_ || range.end_ > part.numBits_) {
        throwInvalidPattern(
            pattern, absl::StrCat("the fixed bit range [", range.begin_, ", ",
                                  range.end_,
                                  ") is empty or not contained in "
                                  "the [0, ",
                                  part.numBits_, ") bits of its number"));
      }
      if (range.begin_ < lastEnd) {
        throwInvalidPattern(pattern,
                            "the fixed bit ranges of a number have to be "
                            "sorted and must not overlap");
      }
      if (range.value_ > ad_utility::bitMaskForLowerBits(range.numBits())) {
        throwInvalidPattern(
            pattern, absl::StrCat("the value ", range.value_,
                                  " doesn't fit into the fixed bit range [",
                                  range.begin_, ", ", range.end_, ")"));
      }
      lastEnd = range.end_;
    }
    if (part.encoding_ == NumberEncoding::Digits &&
        (part.numBits_ % NibbleSize != 0 || !part.fixedBitRanges_.empty())) {
      throwInvalidPattern(pattern,
                          "a number that is encoded digit by digit must have a "
                          "multiple of four bits and no fixed bit ranges");
    }
    if (part.separator_.find_first_of("<>") != std::string::npos) {
      throwInvalidPattern(pattern,
                          "a separator must not contain an angle bracket");
    }
    if (!part.separator_.empty() &&
        absl::ascii_isdigit(static_cast<unsigned char>(part.separator_[0]))) {
      throwInvalidPattern(pattern,
                          "a separator must not start with a digit, because "
                          "the digits of the preceding number are matched "
                          "greedily");
    }
  }
  // All separators but the last one have to be non-empty, else two consecutive
  // numbers could not be told apart.
  for (size_t i = 0; i + 1 < pattern.parts_.size(); ++i) {
    if (pattern.parts_.at(i).separator_.empty()) {
      throwInvalidPattern(pattern,
                          "only the last number of a pattern may be followed "
                          "by an empty separator");
    }
  }
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
    copyBits(range.begin_);
    if (((value >> range.begin_) &
         ad_utility::bitMaskForLowerBits(range.numBits())) != range.value_) {
      return std::nullopt;
    }
    inputPos = range.end_;
  }
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
    copyBits(range.begin_);
    result |= range.value_ << range.begin_;
    outputPos = range.end_;
  }
  copyBits(part.numBits_);
  return result;
}

// _____________________________________________________________________________
std::string_view leadingDigits(std::string_view input) {
  size_t end = input.find_first_not_of("0123456789");
  return input.substr(0, end);
}

// _____________________________________________________________________________
std::optional<uint64_t> parseDecimal(std::string_view input) {
  if (input.empty() || (input.size() > 1 && input[0] == '0')) {
    return std::nullopt;
  }
  uint64_t value = 0;
  for (char c : input) {
    uint64_t digit = static_cast<uint64_t>(c - '0');
    if (value > (std::numeric_limits<uint64_t>::max() - digit) / 10) {
      return std::nullopt;
    }
    value = value * 10 + digit;
  }
  return value;
}

// _____________________________________________________________________________
void to_json(nlohmann::json& j, const FixedBitRange& range) {
  j[beginKey] = range.begin_;
  j[endKey] = range.end_;
  j[valueKey] = range.value_;
}

// _____________________________________________________________________________
void from_json(const nlohmann::json& j, FixedBitRange& range) {
  range.begin_ = j.at(beginKey).get<size_t>();
  range.end_ = j.at(endKey).get<size_t>();
  range.value_ = j.at(valueKey).get<uint64_t>();
}

// _____________________________________________________________________________
void to_json(nlohmann::json& j, const Part& part) {
  j[numBitsKey] = part.numBits_;
  j[fixedBitRangesKey] = part.fixedBitRanges_;
  j[separatorKey] = part.separator_;
  j[encodingKey] = part.encoding_ == NumberEncoding::Digits
                       ? digitsEncodingName
                       : binaryEncodingName;
}

// _____________________________________________________________________________
void from_json(const nlohmann::json& j, Part& part) {
  part.numBits_ = j.at(numBitsKey).get<size_t>();
  part.fixedBitRanges_ =
      j.at(fixedBitRangesKey).get<std::vector<FixedBitRange>>();
  part.separator_ = j.at(separatorKey).get<std::string>();
  auto encoding = j.at(encodingKey).get<std::string>();
  if (encoding == digitsEncodingName) {
    part.encoding_ = NumberEncoding::Digits;
  } else if (encoding == binaryEncodingName) {
    part.encoding_ = NumberEncoding::Binary;
  } else {
    throw std::runtime_error(
        absl::StrCat("Unknown encoding \"", encoding,
                     "\" for a number of an encoded IRI, expected \"",
                     binaryEncodingName, "\" or \"", digitsEncodingName, "\""));
  }
}

// _____________________________________________________________________________
void to_json(nlohmann::json& j, const Pattern& pattern) {
  j[prefixKey] = pattern.prefix_;
  j[partsKey] = pattern.parts_;
}

// _____________________________________________________________________________
void from_json(const nlohmann::json& j, Pattern& pattern) {
  pattern.prefix_ = j.at(prefixKey).get<std::string>();
  pattern.parts_ = j.at(partsKey).get<std::vector<Part>>();
}

}  // namespace encodedIri
