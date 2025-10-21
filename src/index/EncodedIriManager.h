// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_ENCODEDVALUES_H
#define QLEVER_SRC_INDEX_ENCODEDVALUES_H

#include <limits>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "backports/three_way_comparison.h"
#include "global/Id.h"
#include "util/BitUtils.h"
#include "util/CtreHelpers.h"
#include "util/Log.h"
#include "util/json.h"

namespace detail {
// CTRE named capture group identifiers for C++17 compatibility
constexpr ctll::fixed_string digitsCaptureGroup = "digits";

// Configuration for a prefix encoding. Either plain (just a prefix and digits)
// or with bit pattern constraints (prefix + numeric value where certain bits
// must be zero).
struct PrefixConfig {
  // The IRI prefix (with leading angle bracket, e.g., "<http://example.org/")
  std::string prefix;

  // For bit pattern mode: the range of bits that must be zero in the numeric
  // value [bitStart, bitEnd) (half-open interval, bitEnd is exclusive).
  // If not set, plain digit encoding is used.
  std::optional<std::pair<size_t, size_t>> zeroBitRange;

  // Default constructor for plain prefix mode
  explicit PrefixConfig(std::string p) : prefix(std::move(p)) {}

  // Constructor for bit pattern mode. The bit range is [bitStart, bitEnd)
  // where bitEnd is exclusive.
  PrefixConfig(std::string p, size_t bitStart, size_t bitEnd)
      : prefix(std::move(p)), zeroBitRange(std::make_pair(bitStart, bitEnd)) {
    AD_CONTRACT_CHECK(bitStart < bitEnd);
    AD_CONTRACT_CHECK(bitEnd <= 64);
  }

  // Check if this is a bit pattern encoding
  bool isBitPatternMode() const { return zeroBitRange.has_value(); }

  // Get the bit range (throws if not in bit pattern mode)
  std::pair<size_t, size_t> getBitRange() const {
    AD_CONTRACT_CHECK(isBitPatternMode());
    return zeroBitRange.value();
  }

  // Equality for testing
  bool operator==(const PrefixConfig& other) const {
    return prefix == other.prefix && zeroBitRange == other.zeroBitRange;
  }
};
}  // namespace detail

// This class allows the encoding of IRIs that start with a fixed prefix
// followed by a sequence of decimal digits directly into an `Id`. For
// example, <http://example.org/12345> with digit sequence `12345` and
// prefix `http://example.org/`.
//
// Two encoding modes are supported:
//
// 1. PLAIN DIGIT MODE (original behavior):
// The digits are encoded in a non-standard way which ensures the order of
// encoded values corresponds to the lexical order of the original IRIs.
// Each decimal digit is encoded as a 4-bit nibble, where digit `i` is
// encoded as `i+1` and converted to a hexadecimal number. The nibbles are
// stored left-aligned (not right-aligned) and filled on the right with zeroes.
//
// 2. BIT PATTERN MODE (new):
// For IRIs with numeric values where certain bits are always zero, this mode
// compresses the value by removing those zero bits. The prefix configuration
// specifies a bit range [bitStart, bitEnd) (half-open interval) that must be
// all zeros. The value is then stored with those bits removed, allowing more
// efficient encoding.
//
// An `Id` has 64 bits, of which the `NumBitsTotal` rightmost bits are
// used for the encoding. The `64 - NumBitsTotal` leftmost bits are ignored when
// decoding and can be used for other purposes. The next `NumBitsTags` bits
// encode the IRI prefix; that is, at most `2 ** NumBitsTags` different prefixes
// can be used. The remaining `NumBitsTotal - NumBitsTags` bits are used to
// encode the digits that follow the prefix.
//
// For example, here are a few example encodings, with `NumBitsTotal = 40` and
// `NumBitsTags = 8`. The prefix is `http://example.org/` and encoded in 8
// bits as `ff`. Note that the IRIs on the left are in lexical order, and so are
// the encodings on the right.
//
// <http://example.org/1>    ->  00 00 00 ff 20 00 00 00
// <http://example.org/10>   ->  00 00 00 ff 21 00 00 00
// <http://example.org/100>  ->  00 00 00 ff 21 10 00 00
// <http://example.org/2>    ->  00 00 00 ff 30 00 00 00
// <http://example.org/20>   ->  00 00 00 ff 31 00 00 00
//
// NOTE: Only IRIs that fulfill these constraints can be encoded. For example,
// if 4 times the number of digits is larger than `NumBitsTotal - NumBitsTags`,
// the IRI will not be encoded (but stored as a regular IRI). See the bottom of
// the file for the default values of `NumBitsTotal` and `NumBitsTags`.
//
template <size_t NumBitsTotal, size_t NumBitsTags>
class EncodedIriManagerImpl {
 public:
  static constexpr size_t NumBitsEncoding = NumBitsTotal - NumBitsTags;

  // We use 4-bit nibbles per digit in the encoding.
  static constexpr size_t NibbleSize = 4;
  static constexpr size_t NumDigits = NumBitsEncoding / NibbleSize;
  static_assert(NumBitsEncoding % NibbleSize == 0);

  static_assert(NumBitsTotal <= 64);
  static_assert(NumBitsTags <= 64);
  static_assert(NumDigits > 0);

  // The prefixes of the IRIs that will be encoded.
  std::vector<detail::PrefixConfig> prefixes_;

  // By default, `prefixes_` is empty, so no IRI will be encoded.
  EncodedIriManagerImpl() = default;

  // Construct from the list of prefixes. The prefixes have to be specified
  // without any brackes, so e.g. "http://example.org/" if IRIs of the form
  // `<http://example.org/1234>` should be encoded.
  explicit EncodedIriManagerImpl(
      std::vector<std::string> prefixesWithoutAngleBrackets) {
    if (prefixesWithoutAngleBrackets.empty()) {
      return;
    }
    // Sort the prefixes lexicographically to make the ordering deterministic
    // (provided that the prefixes do not end with digits).
    ql::ranges::sort(prefixesWithoutAngleBrackets);

    // Remove duplicates.
    //
    // NOTE: `ql::ranges::unique` does not work because of a discrepancy in the
    // return types between `std::ranges` and `range-v3`.
    prefixesWithoutAngleBrackets.erase(
        ::ranges::unique(prefixesWithoutAngleBrackets),
        prefixesWithoutAngleBrackets.end());
    static constexpr auto maxNumPrefixes = 1ULL << NumBitsTags;

    if (prefixesWithoutAngleBrackets.size() > maxNumPrefixes) {
      throw std::runtime_error(absl::StrCat(
          "Number of prefixes specified with `--encode-as-id` is ",
          prefixesWithoutAngleBrackets.size(), ", which is too many; ",
          "the maximum is ", maxNumPrefixes));
    }

    // TODO<C++23> use `std::views::adjacent`.
    for (size_t i = 0; i < prefixesWithoutAngleBrackets.size() - 1; ++i) {
      const auto& a = prefixesWithoutAngleBrackets.at(i);
      const auto& b = prefixesWithoutAngleBrackets.at(i + 1);
      if (ql::starts_with(b, a)) {
        throw std::runtime_error(absl::StrCat(
            "None of the prefixes specified with `--encode-as-id` "
            "may be a prefix of another; here is a violating pair: \"",
            a, "\" and \"", b, "\"."));
      }
    }
    prefixes_.reserve(prefixesWithoutAngleBrackets.size());
    for (const auto& prefix : prefixesWithoutAngleBrackets) {
      if (ql::starts_with(prefix, '<')) {
        throw std::runtime_error(absl::StrCat(
            "The prefixes specified with `--encode-as-id` must not "
            "be enclosed in angle brackets; here is a violating prefix: \"",
            prefix, "\""));
      }
      prefixes_.emplace_back(absl::StrCat("<", prefix));
    }
  }

  // Try to encode the given string as an `Id`. If the encoding fails, return
  // `std::nullopt`. This happens in one of the following cases:
  //
  // 1. The string is not an `<iriref-in-angle-brackets>`
  // 2. The string does not start with any of the `prefixes_`
  // 3. After the matching prefix, there are characters other than `[0-9]`
  // 4. There are more digits than fit into `NumBitsEncoding` (4 bits / digit)
  std::optional<Id> encode(std::string_view repr) const {
    // Find the matching prefix.
    auto it =
        ql::ranges::find_if(prefixes_, [&repr](const detail::PrefixConfig& cfg) {
          return ql::starts_with(repr, cfg.prefix);
        });
    if (it == prefixes_.end()) {
      return std::nullopt;
    }

    // Check that after the prefix, the string contains only digits and the
    // trailing '>'.
    repr.remove_prefix(it->prefix.size());
    static constexpr auto regex = ctll::fixed_string{"(?<digits>[0-9]+)>"};
    auto match = ctre::match<regex>(repr);
    if (!match) {
      return std::nullopt;
    }

    // Extract the substring with the digits.
    const auto& numString =
        match.template get<detail::digitsCaptureGroup>().to_view();

    // Get the index of the used prefix.
    auto prefixIndex = static_cast<size_t>(it - prefixes_.begin());

    // Handle bit pattern mode
    if (it->isBitPatternMode()) {
      return encodeBitPattern(numString, prefixIndex, it->getBitRange());
    }

    // Handle plain digit mode
    if (numString.size() > NumDigits) {
      return std::nullopt;
    }
    return Id::makeFromEncodedVal(encodeDecimalToNBit(numString) |
                                  (prefixIndex << NumBitsEncoding));
  }

  // Convert an `Id` that was encoded using this encoder back to a string.
  // Throw an exception if the `Id` has a datatype different from `EncodedVal`.
  std::string toString(Id id) const {
    AD_CORRECTNESS_CHECK(id.getDatatype() == Datatype::EncodedVal);
    // Get only the rightmost bits that represent the digits.
    static constexpr auto mask =
        ad_utility::bitMaskForLowerBits(NumBitsEncoding);
    auto digitEncoding = id.getEncodedVal() & mask;
    // Get the index of the prefix.
    auto prefixIdx = id.getEncodedVal() >> NumBitsEncoding;
    std::string result;
    const auto& prefixConfig = prefixes_.at(prefixIdx);
    result.reserve(prefixConfig.prefix.size() + NumDigits + 1);
    result = prefixConfig.prefix;

    // Handle bit pattern mode
    if (prefixConfig.isBitPatternMode()) {
      decodeBitPattern(result, digitEncoding, prefixConfig.getBitRange());
    } else {
      // Handle plain digit mode
      decodeDecimalFrom64Bit(result, digitEncoding);
    }
    result.push_back('>');
    return result;
  }

  // Conversion to and from JSON.
  static constexpr const char* jsonKey_ =
      "prefixes-with-leading-angle-brackets";
  static constexpr const char* jsonKeyExtended_ = "prefix-configs";

  friend void to_json(nlohmann::json& j,
                      const EncodedIriManagerImpl& encodedIriManager) {
    // Use new format that supports both plain and bit pattern modes
    nlohmann::json configs = nlohmann::json::array();
    for (const auto& cfg : encodedIriManager.prefixes_) {
      nlohmann::json item;
      item["prefix"] = cfg.prefix;
      if (cfg.isBitPatternMode()) {
        auto [bitStart, bitEnd] = cfg.getBitRange();
        item["zeroBitStart"] = bitStart;
        item["zeroBitEnd"] = bitEnd;
      }
      configs.push_back(item);
    }
    j[jsonKeyExtended_] = configs;
  }

  friend void from_json(const nlohmann::json& j,
                        EncodedIriManagerImpl& encodedIriManager) {
    encodedIriManager.prefixes_.clear();

    // Try new format first
    if (j.contains(jsonKeyExtended_)) {
      const auto& configs = j[jsonKeyExtended_];
      for (const auto& item : configs) {
        std::string prefix = item["prefix"];
        if (item.contains("zeroBitStart") && item.contains("zeroBitEnd")) {
          size_t bitStart = item["zeroBitStart"];
          size_t bitEnd = item["zeroBitEnd"];
          encodedIriManager.prefixes_.emplace_back(std::move(prefix), bitStart,
                                                   bitEnd);
        } else {
          encodedIriManager.prefixes_.emplace_back(std::move(prefix));
        }
      }
    } else if (j.contains(jsonKey_)) {
      // Fall back to old format for backward compatibility
      std::vector<std::string> oldPrefixes = j[jsonKey_];
      for (auto& prefix : oldPrefixes) {
        encodedIriManager.prefixes_.emplace_back(std::move(prefix));
      }
    }
  }

  // Hash support for use in `TestIndexConfig`.
  template <typename H>
  friend H AbslHashValue(H h, const EncodedIriManagerImpl& manager) {
    return H::combine(std::move(h), manager.prefixes_);
  }

  // Equality operator for use in `TestIndexConfig`.
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(EncodedIriManagerImpl, prefixes_)

 private:
  // Encode a number with bit pattern constraints. The numString must parse to
  // a valid uint64_t, and the bits in the range [bitStart, bitEnd) must all be
  // zero. Returns nullopt if constraints are not met or if the number is too
  // large to fit after removing the zero bits.
  std::optional<Id> encodeBitPattern(
      std::string_view numString, size_t prefixIndex,
      std::pair<size_t, size_t> bitRange) const {
    auto [bitStart, bitEnd] = bitRange;

    // Parse the numeric string to uint64_t
    uint64_t value = 0;
    for (char c : numString) {
      // Check for overflow
      if (value > (std::numeric_limits<uint64_t>::max() - (c - '0')) / 10) {
        return std::nullopt;
      }
      value = value * 10 + (c - '0');
    }

    // Check that all bits in the range [bitStart, bitEnd) are zero
    for (size_t bit = bitStart; bit < bitEnd; ++bit) {
      if ((value >> bit) & 1) {
        return std::nullopt;
      }
    }

    // Remove the zero bits from the value
    size_t numZeroBits = bitEnd - bitStart;
    uint64_t lowerBits = value & ad_utility::bitMaskForLowerBits(bitStart);
    uint64_t upperBits = value >> bitEnd;
    uint64_t compressedValue = (upperBits << bitStart) | lowerBits;

    // Check if the compressed value fits in the available encoding bits
    // We need to store the compressed value, so it must fit in NumBitsEncoding
    if (compressedValue >= (1ULL << NumBitsEncoding)) {
      return std::nullopt;
    }

    return Id::makeFromEncodedVal(compressedValue |
                                  (prefixIndex << NumBitsEncoding));
  }

  // Decode a bit pattern encoded value back to the original uint64_t and
  // append it to the result string. The bit range [bitStart, bitEnd) specifies
  // which bits were removed during encoding.
  static void decodeBitPattern(std::string& result, uint64_t encoded,
                                std::pair<size_t, size_t> bitRange) {
    auto [bitStart, bitEnd] = bitRange;

    // Reconstruct the original value by reinserting the zero bits
    uint64_t lowerBits = encoded & ad_utility::bitMaskForLowerBits(bitStart);
    uint64_t upperBits = encoded >> bitStart;
    uint64_t originalValue = (upperBits << bitEnd) | lowerBits;

    // Convert to decimal string
    result.append(std::to_string(originalValue));
  }

  // Encode the `numberStr` (which may only consist of digits) into a 64-bit
  // number.
  static constexpr uint64_t encodeDecimalToNBit(std::string_view numberStr) {
    auto len = numberStr.size();
    AD_CORRECTNESS_CHECK(len <= NumDigits);

    uint64_t result = 0;

    // Compute the starting shift (for the first digit).
    uint64_t shift = NumBitsEncoding - NibbleSize;

    for (const char digitChar : numberStr) {
      // Deliberately encode [0, ..., 9] as [1, ..., A], so that the padding
      // nibble `0`is smaller than any valid digit encoding.
      uint8_t digit = (digitChar - '0') + 1;
      result |= static_cast<uint64_t>(digit) << shift;
      shift -= NibbleSize;
    }
    return result;
  }

  // The inverse of `encodeDecimalToNBit`. The result is appended to the
  // `result` string.
  static void decodeDecimalFrom64Bit(std::string& result, uint64_t encoded) {
    size_t shift = NumBitsEncoding - NibbleSize;
    auto numTrailingZeros = std::countr_zero(encoded);
    size_t numTrailingZeroNibbles = numTrailingZeros / NibbleSize;
    size_t len = NumDigits - numTrailingZeroNibbles;
    for (size_t i = 0; i < len; ++i) {
      result.push_back(static_cast<char>(((encoded >> shift) & 0xF) + '0' - 1));
      shift -= NibbleSize;
    }
  }
};

// The default encoder for IRIs in QLever: 60 bits are used for the complete
// encodingr, 8 bits are used for the prefixes (which allows up to 256
// prefixes). This leaves 52 bits for the digits, so up to 13 digits can be
// encoded.
using EncodedIriManager = EncodedIriManagerImpl<Id::numDataBits, 8>;

#endif  // QLEVER_SRC_INDEX_ENCODEDVALUES_H
