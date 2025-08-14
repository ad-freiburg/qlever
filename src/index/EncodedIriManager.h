// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_ENCODEDVALUES_H
#define QLEVER_SRC_INDEX_ENCODEDVALUES_H

#include "backports/algorithm.h"
#include "global/Id.h"
#include "util/BitUtils.h"
#include "util/CtreHelpers.h"
#include "util/Log.h"
#include "util/json.h"

// This class allows the encoding of IRIs that start with a fixed prefix
// followed by a sequence of digits directly into an ID .`NumBitsTotal` is the
// total number of bits used for the encoding. The leading `64 - NumBitsTotal`
// bits of an encoded ID are set to zero when encoding an ID and can be used for
// other purposes. `NumBitsTags` specifies the number of different IRI prefixes
// that can be used, which is `2 ** NumBitsTags`. The greater the number of
// tags, the smaller the positive integers that can stand after the prefix.
//
// The encoding is as follows:
// 1. The first `64 - NumBitsTags` bits are set to 0 when encoding and ignored
// when decoding.
// 2. The next `NumBitsTags` bits are used to encode the index of the prefix.
// 3. The remaining bits are used to encode the digits that follow after the
// prefix
//    Each decimal digit is encoded as a 4-bit nibble, where the digit `i` is
//    encoded as `i+1` and converted to a hexadecimal number. The nibbles are
//    stored left-aligned and filled at then end with zero. This makes sure that
//    when comparing the encodings of IRIs with the same prefix as an unsigned
//    integer, then the result is the same as lexically comparing the original
//    IRIs.
template <size_t NumBitsTotal, size_t NumBitsTags>
class EncodedIriManagerImpl {
 public:
  static constexpr size_t NumBitsEncoding = NumBitsTotal - NumBitsTags;

  // 4-bit nibbles per digit in the encoding.
  static constexpr size_t NibbleSize = 4;
  static constexpr size_t NumDigits = NumBitsEncoding / NibbleSize;
  static_assert(NumBitsEncoding % NibbleSize == 0);

  static_assert(NumBitsTotal <= 64);
  static_assert(NumBitsTags <= 64);
  static_assert(NumDigits > 0);

  // The prefixes of the IRIs that will be encoded.
  std::vector<std::string> prefixes_;

  // Default constructor: Empty prefixes, no IRIs will be encoded.
  EncodedIriManagerImpl() = default;

  // Construct from the list of prefixes. The prefixes have to be specified
  // without any brackes, so e.g. "http://example.org/" if IRIs of the form
  // `<http://example.org/123>` should be encoded.
  explicit EncodedIriManagerImpl(
      std::vector<std::string> prefixesWithoutAngleBrackets) {
    if (prefixesWithoutAngleBrackets.empty()) {
      return;
    }
    // Sort the prefixes lexicographically to make the ordering more
    // deterministic.
    ql::ranges::sort(prefixesWithoutAngleBrackets);

    // Remove duplicates.
    prefixesWithoutAngleBrackets.erase(
        ::ranges::unique(prefixesWithoutAngleBrackets),
        prefixesWithoutAngleBrackets.end());
    for (size_t i = 0; i < prefixesWithoutAngleBrackets.size() - 1; ++i) {
      const auto& a = prefixesWithoutAngleBrackets.at(i);
      const auto& b = prefixesWithoutAngleBrackets.at(i + 1);
      if (b.starts_with(a)) {
        throw std::runtime_error(absl::StrCat(
            "None of the  prefixes for the encoding of numeric IRIs in IDs "
            "may be a prefix of another prefix. Violated for \"",
            a, "\" and \"", b, "\"."));
      }
    }
    static constexpr auto maxNumPrefixes = 1ull << NumBitsTags;
    if (prefixesWithoutAngleBrackets.size() > maxNumPrefixes) {
      throw std::runtime_error(absl::StrCat(
          "Too many prefixes specified for the numeric encoding of numeric "
          "IRIs into IDs. The maximum is ",
          maxNumPrefixes, " , but ", prefixesWithoutAngleBrackets.size(),
          " prefixes were specified"));
    }
    prefixes_.reserve(prefixesWithoutAngleBrackets.size());
    for (const auto& prefix : prefixesWithoutAngleBrackets) {
      if (prefix.starts_with('<')) {
        throw std::runtime_error(absl::StrCat(
            "The prefixes for the direct encoding of IRIs into IDs may *not* "
            "be enclosed in angle brackets <>. Violated for \"",
            prefix, "\""));
      }
      prefixes_.push_back(absl::StrCat("<", prefix));
    }
  }

  // Try to encode the given string as an ID. If the encoding fails, return
  // `nullopt`. This might happen in the following cases: 0. The string is not
  // an `<iriref-in-angle-brackets>`
  // 1. The string doesn't start with any of the `prefixes_`.
  // 2. After the matching prefix, the string contains characters other than the
  // digits `[0-9]`.
  // 3. The string contains too many digits to encode them given
  // `NumBitsEncoding`.
  std::optional<Id> encode(std::string_view repr) const {
    // Find the matching prefix.
    auto it = ql::ranges::find_if(prefixes_, [&repr](std::string_view prefix) {
      return repr.starts_with(prefix);
    });
    if (it == prefixes_.end()) {
      return std::nullopt;
    }

    // Check that after the prefix, the string contains only digits and the
    // trailing '>'.
    repr.remove_prefix(it->size());
    static constexpr auto regex = ctll::fixed_string{"(?<digits>[0-9]+)>"};
    auto match = ctre::match<regex>(repr);
    if (!match) {
      return std::nullopt;
    }

    // Extract the substring with the digits, and check that it is not too long.
    const auto& numString = match.template get<"digits">().to_view();
    if (numString.size() > NumDigits) {
      return std::nullopt;
    }

    // The the index of the used prefix, and run the actual encoding.
    auto prefixIndex = static_cast<size_t>(it - prefixes_.begin());
    return Id::makeFromEncodedVal(encodeDecimalToNBit(numString) |
                                  (prefixIndex << NumBitsEncoding));
  }

  // Convert an `Id` that was encoded using this encoder back to a string.
  // Throw an exception if the `Id` has a datatype different from `EncodedVal`.
  std::string toString(Id id) const {
    AD_CORRECTNESS_CHECK(id.getDatatype() == Datatype::EncodedVal);
    // Get only the lower bits that represent the digits.
    static constexpr auto mask =
        ad_utility::bitMaskForLowerBits(NumBitsEncoding);
    auto digitEncoding = id.getEncodedVal() & mask;
    // Get the index of the prefix.
    auto prefixIdx = id.getEncodedVal() >> NumBitsEncoding;
    std::string result;
    const auto& prefix = prefixes_.at(prefixIdx);
    result.reserve(prefix.size() + NumDigits + 1);
    result = prefix;
    decodeDecimalFrom64Bit(result, digitEncoding);
    result.push_back('>');
    return result;
  }

  // Conversion to and from JSON.
  static constexpr const char* jsonKey_ =
      "prefixes-with-leading-angle-brackets";
  friend void to_json(nlohmann::json& j,
                      const EncodedIriManagerImpl& encodedIriManager) {
    j[jsonKey_] = encodedIriManager.prefixes_;
  }
  friend void from_json(const nlohmann::json& j,
                        EncodedIriManagerImpl& encodedIriManager) {
    encodedIriManager.prefixes_ =
        static_cast<std::vector<std::string>>(j[jsonKey_]);
  }

  // Hash support for use in TestIndexConfig
  template <typename H>
  friend H AbslHashValue(H h, const EncodedIriManagerImpl& manager) {
    return H::combine(std::move(h), manager.prefixes_);
  }

  // Equality operator for use in TestIndexConfig
  bool operator==(const EncodedIriManagerImpl&) const = default;

 private:
  // Encode the `numberStr` (which may only consist of digits) into a 64-bit
  // number.
  static constexpr uint64_t encodeDecimalToNBit(std::string_view numberStr) {
    auto len = numberStr.size();
    AD_CORRECTNESS_CHECK(len <= NumDigits);

    uint64_t result = 0;

    // Compute the starting shift (for the first digit)
    uint64_t shift = NumBitsEncoding - NibbleSize;

    for (const char digitChar : numberStr) {
      // Deliberately encode [0, ..., 9] as [1, ..., A] to make the
      // padding nibble `0`.
      uint8_t digit = (digitChar - '0') + 1;
      result |= static_cast<uint64_t>(digit) << shift;
      shift -= 4;
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

// The default encoder for IRIs in QLever. 60 bits are used for the complete
// encoding, 8 bits for the prefixes (allows for 256) prefixes, leaving 52
// bits for the actual encoding (allowing for 13-digit numbers).
using EncodedIriManager = EncodedIriManagerImpl<Id::numDataBits, 8>;

#endif  // QLEVER_SRC_INDEX_ENCODEDVALUES_H
