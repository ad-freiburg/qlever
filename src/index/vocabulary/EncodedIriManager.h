// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_VOCABULARY_ENCODEDIRIMANAGER_H
#define QLEVER_SRC_INDEX_VOCABULARY_ENCODEDIRIMANAGER_H

#include <absl/numeric/bits.h>

#include <charconv>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "backports/three_way_comparison.h"
#include "global/Id.h"
#include "util/BitUtils.h"
#include "util/Log.h"
#include "util/json.h"

namespace detail {
// Match `repr` against the pattern `([0-9]+)>` and return the digit
// substring as a `string_view` into `repr` on success, or `std::nullopt` if
// the pattern does not match.
std::optional<std::string_view> matchDigitsPrefix(std::string_view repr);
}  // namespace detail

// This class allows the encoding of IRIs that start with a fixed prefix
// followed by a sequence of decimal digits directly into an `Id`. For
// example, <http://example.org/12345> with digit sequence `12345` and
// prefix `http://example.org/`. This is implemented as follows:
//
// An `Id` has 64 bits, of which the `NumBitsTotal` rightmost bits are
// used for the encoding. The `64 - NumBitsTotal` leftmost bits are ignored when
// decoding and can be used for other purposes. The highest of the
// `NumBitsTotal` bits is the *layout bit*, which selects between two layouts:
//
// Layout bit `0` (the *narrow* layout): The next `NumBitsTags - 1` bits encode
// the IRI prefix; that is, at most `2 ** (NumBitsTags - 1)` different narrow
// prefixes can be used. The remaining `NumBitsTotal - NumBitsTags` bits encode
// the digits that follow the prefix, in the following non-standard way, which
// makes sure that the order of the encoded values corresponds to the lexical
// order of the original IRIs. Each decimal digit is encoded as a 4-bit nibble,
// where digit `i` is encoded as `i+1` and converted to a hexadecimal number.
// The nibbles are stored left-aligned (not right-aligned) and filled on the
// right with zeroes.
//
// For example, here are a few example encodings, with `NumBitsTotal = 40` and
// `NumBitsTags = 8`. The prefix is `http://example.org/` and encoded in 8
// bits as `7f`. Note that the IRIs on the left are in lexical order, and so are
// the encodings on the right.
//
// <http://example.org/1>    ->  00 00 00 7f 20 00 00 00
// <http://example.org/10>   ->  00 00 00 7f 21 00 00 00
// <http://example.org/100>  ->  00 00 00 7f 21 10 00 00
// <http://example.org/2>    ->  00 00 00 7f 30 00 00 00
// <http://example.org/20>   ->  00 00 00 7f 31 00 00 00
//
// Layout bit `1` (the *wide* layout): The next `NumBitsWideTags` bits encode
// the IRI prefix (so at most `2 ** NumBitsWideTags` wide prefixes), and the
// remaining `NumBitsTotal - 1 - NumBitsWideTags` bits store the digit sequence
// as a plain binary number. This fits considerably longer digit sequences (17
// digits vs 13 with the default settings), at the price of a much smaller
// prefix budget and of the bitwise order being the *numeric* order of the
// digit sequences, not the lexical order. Digit sequences with leading zeros
// are not encoded in the wide layout (the binary encoding could not
// distinguish `07` from `7`).
//
// NOTE: Only IRIs that fulfill these constraints can be encoded. For example,
// if 4 times the number of digits is larger than `NumBitsTotal - NumBitsTags`,
// the IRI will not be encoded in the narrow layout (but stored as a regular
// IRI). See the bottom of the file for the default values of `NumBitsTotal`
// and `NumBitsTags`.
struct NoHardcodedPrefixes {
  // The fixed prefixes have to be wrapped into a struct because
  // `std::array<std::string_view>` cannot be passed as a template parameter
  // before C++20.
  static constexpr std::array<std::string_view, 0> value = {};
};

template <size_t NumBitsTotal, size_t NumBitsTags,
          typename HardcodedPrefixesT = NoHardcodedPrefixes>
class EncodedIriManagerImpl {
  static constexpr const auto& HardcodedPrefixes = HardcodedPrefixesT::value;

 public:
  static constexpr size_t NumBitsEncoding = NumBitsTotal - NumBitsTags;

  // We use 4-bit nibbles per digit in the encoding.
  static constexpr size_t NibbleSize = 4;
  static constexpr size_t NumDigits = NumBitsEncoding / NibbleSize;
  static_assert(NumBitsEncoding % NibbleSize == 0);

  static_assert(NumBitsTotal <= 64);
  static_assert(NumBitsTags >= 2);
  static_assert(NumBitsTags <= 64);
  static_assert(NumDigits > 0);

  // The layout bit is the highest of the `NumBitsTotal` bits. It is `0` for
  // the narrow layout and `1` for the wide layout. In the narrow layout it
  // coincides with the highest bit of the `NumBitsTags` tag bits, which is
  // why at most `2 ** (NumBitsTags - 1)` narrow prefixes are allowed.
  static constexpr uint64_t layoutBitMask_ = 1ULL << (NumBitsTotal - 1);

  // The number of tag and payload bits of the wide layout.
  static constexpr size_t NumBitsWideTags = 2;
  static constexpr size_t NumBitsWidePayload =
      NumBitsTotal - 1 - NumBitsWideTags;
  static_assert(NumBitsWidePayload > NumBitsEncoding);
  static_assert(NumBitsWidePayload <= 62);

  // The largest number of decimal digits `d` such that *every* `d`-digit
  // number fits into `NumBitsWidePayload` bits, i.e. the largest `d` with
  // `10^d <= 2^NumBitsWidePayload`.
  static constexpr size_t NumDigitsWide = []() {
    size_t numDigits = 0;
    uint64_t powerOfTen = 10;
    while (powerOfTen <= (1ULL << NumBitsWidePayload)) {
      ++numDigits;
      powerOfTen *= 10;
    }
    return numDigits;
  }();
  static_assert(NumDigitsWide > NumDigits);

  // The prefixes of the IRIs that will be encoded, for both layouts.
  std::vector<std::string> prefixes_;
  std::vector<std::string> widePrefixes_;

  static constexpr auto maxNumPrefixes_ = 1ULL << (NumBitsTags - 1);
  static constexpr auto maxNumWidePrefixes_ = 1ULL << NumBitsWideTags;

  // By default, `prefixes_` is empty, so no IRI will be encoded.
  // NOTE: When loading an existing index, in particular one from an older
  // QLever version with different hardcoded prefixes, it is crucial to use the
  // deserialization from JSON to initialize the EncodedIriManager. See the
  // note in `from_json`.
  EncodedIriManagerImpl() : EncodedIriManagerImpl(std::vector<std::string>{}) {}

  // Construct from the list of prefixes (for the narrow layout) and the
  // optional list of wide prefixes (for the wide layout). The prefixes have to
  // be specified without any brackets, so e.g. "http://example.org/" if IRIs
  // of the form `<http://example.org/1234>` should be encoded.
  // NOTE: When loading an existing index, in particular one from an older
  // QLever version with different hardcoded prefixes, it is crucial to use the
  // deserialization from JSON to initialize the EncodedIriManager. See the
  // note in `from_json`.
  explicit EncodedIriManagerImpl(
      std::vector<std::string> prefixesWithoutAngleBrackets,
      std::vector<std::string> widePrefixesWithoutAngleBrackets = {}) {
    // Add hardcoded prefixes.
    for (const auto& prefix : HardcodedPrefixes) {
      // Adding a hardcoded prefix a second time in the constructor is an error.
      AD_CONTRACT_CHECK(
          !ad_utility::contains(prefixesWithoutAngleBrackets, prefix));
      prefixesWithoutAngleBrackets.emplace_back(prefix);
    }
    if (prefixesWithoutAngleBrackets.empty() &&
        widePrefixesWithoutAngleBrackets.empty()) {
      return;
    }

    // Sort the prefixes lexicographically to make the ordering deterministic
    // (provided that the prefixes do not end with digits), remove duplicates,
    // and check the size limit.
    auto normalize = [](std::vector<std::string>& prefixes, size_t maxNum,
                        std::string_view layoutName) {
      ql::ranges::sort(prefixes);

      // NOTE: `ql::ranges::unique` does not work because of a discrepancy in
      // the return types between `std::ranges` and `range-v3`.
      prefixes.erase(::ranges::unique(prefixes), prefixes.end());

      if (prefixes.size() > maxNum) {
        throw std::runtime_error(absl::StrCat(
            "Number of ", layoutName,
            " prefixes specified with `--encode-as-id` is ", prefixes.size(),
            ", which is too many; ", "the maximum is ", maxNum));
      }
    };
    normalize(prefixesWithoutAngleBrackets, maxNumPrefixes_, "narrow");
    normalize(widePrefixesWithoutAngleBrackets, maxNumWidePrefixes_, "wide");

    // No prefix (narrow or wide) may be a prefix of another one, so we check
    // adjacent entries of the sorted union of both lists.
    // TODO<C++23> use `std::views::adjacent`.
    std::vector<std::string> allPrefixes = prefixesWithoutAngleBrackets;
    ql::ranges::copy(widePrefixesWithoutAngleBrackets,
                     std::back_inserter(allPrefixes));
    ql::ranges::sort(allPrefixes);
    for (size_t i = 0; i + 1 < allPrefixes.size(); ++i) {
      const auto& a = allPrefixes.at(i);
      const auto& b = allPrefixes.at(i + 1);
      if (ql::starts_with(b, a)) {
        throw std::runtime_error(absl::StrCat(
            "None of the prefixes specified with `--encode-as-id` "
            "may be a prefix of another; here is a violating pair: \"",
            a, "\" and \"", b, "\"."));
      }
    }

    auto storeWithLeadingBracket = [](const std::vector<std::string>& src,
                                      std::vector<std::string>& dst) {
      dst.reserve(src.size());
      for (const auto& prefix : src) {
        if (ql::starts_with(prefix, '<')) {
          throw std::runtime_error(absl::StrCat(
              "The prefixes specified with `--encode-as-id` must not "
              "be enclosed in angle brackets; here is a violating prefix: \"",
              prefix, "\""));
        }
        dst.push_back(absl::StrCat("<", prefix));
      }
    };
    storeWithLeadingBracket(prefixesWithoutAngleBrackets, prefixes_);
    storeWithLeadingBracket(widePrefixesWithoutAngleBrackets, widePrefixes_);
  }

  // Try to encode the given string as an `Id`. If the encoding fails, return
  // `std::nullopt`. This happens in one of the following cases:
  //
  // 1. The string is not an `<iriref-in-angle-brackets>`
  // 2. The string does not start with any of the `prefixes_`/`widePrefixes_`
  // 3. After the matching prefix, there are characters other than `[0-9]`
  // 4. There are more digits than fit into the respective layout
  std::optional<Id> encode(std::string_view repr) const {
    if (auto wideId = encodeWide(repr)) {
      return wideId;
    }
    // Find the matching prefix.
    auto it = ql::ranges::find_if(prefixes_, [&repr](std::string_view prefix) {
      return ql::starts_with(repr, prefix);
    });
    if (it == prefixes_.end()) {
      return std::nullopt;
    }

    // Check that after the prefix, the string contains only digits and the
    // trailing '>'.
    repr.remove_prefix(it->size());
    auto numStringOpt = detail::matchDigitsPrefix(repr);
    if (!numStringOpt.has_value()) {
      return std::nullopt;
    }
    std::string_view numString = numStringOpt.value();
    if (numString.size() > NumDigits) {
      return std::nullopt;
    }

    // Get the index of the used prefix, and run the actual encoding.
    auto prefixIndex = static_cast<size_t>(it - prefixes_.begin());
    return makeIdFromPrefixIdxAndPayload(prefixIndex,
                                         encodeDecimalToNBit(numString));
  }

  // The wide-layout part of `encode` above. In addition to the failure cases
  // listed there, digit sequences with a leading zero are not encoded (the
  // binary payload could not distinguish `07` from `7`).
  std::optional<Id> encodeWide(std::string_view repr) const {
    auto it =
        ql::ranges::find_if(widePrefixes_, [&repr](std::string_view prefix) {
          return ql::starts_with(repr, prefix);
        });
    if (it == widePrefixes_.end()) {
      return std::nullopt;
    }
    repr.remove_prefix(it->size());
    auto numStringOpt = detail::matchDigitsPrefix(repr);
    if (!numStringOpt.has_value()) {
      return std::nullopt;
    }
    std::string_view numString = numStringOpt.value();
    if (numString.size() > NumDigitsWide ||
        (numString.size() > 1 && numString.front() == '0')) {
      return std::nullopt;
    }

    // As `numString` consists of at most `NumDigitsWide` digits, the value
    // always fits into the payload bits and `from_chars` cannot fail.
    uint64_t value = 0;
    std::from_chars(numString.data(), numString.data() + numString.size(),
                    value);
    auto prefixIndex = static_cast<uint64_t>(it - widePrefixes_.begin());
    return Id::makeFromEncodedVal(layoutBitMask_ |
                                  (prefixIndex << NumBitsWidePayload) | value);
  }

  // combine the integer representation of the prefix and of the payload into a
  // single `Id` with datatype `EncodedValue`. Note: This is the narrow layout;
  // the highest tag bit (the layout bit) has to be `0`, which is guaranteed by
  // `prefixIdx < maxNumPrefixes_`.
  static Id makeIdFromPrefixIdxAndPayload(uint64_t prefixIdx,
                                          uint64_t payload) {
    return Id::makeFromEncodedVal(payload | (prefixIdx << NumBitsEncoding));
  }

  // Convert an `Id` that was encoded using this encoder back to a string.
  // Throw an exception if the `Id` has a datatype different from `EncodedVal`.
  std::string toString(Id id) const {
    AD_CORRECTNESS_CHECK(id.getDatatype() == Datatype::EncodedVal);
    auto bits = id.getEncodedVal();
    if (bits & layoutBitMask_) {
      // Wide layout: the payload is the number itself.
      auto prefixIdx = (bits ^ layoutBitMask_) >> NumBitsWidePayload;
      auto value = bits & ad_utility::bitMaskForLowerBits(NumBitsWidePayload);
      return absl::StrCat(widePrefixes_.at(prefixIdx), value, ">");
    }
    // Get only the rightmost bits that represent the digits.
    auto [prefixIdx, digitEncoding] = splitIntoPrefixIdxAndPayload(id);
    return toStringWithGivenPrefix(digitEncoding, prefixes_.at(prefixIdx));
  }

  // The second half of `toString` above: combine the integer encoding of the
  // payload and the prefix string into a result string that represents an IRI.
  // Note: This function expects, that the prefix starts with `<`.
  static std::string toStringWithGivenPrefix(uint64_t digitEncoding,
                                             std::string_view prefix) {
    AD_EXPENSIVE_CHECK(ql::starts_with(prefix, '<'));
    std::string result;
    result.reserve(prefix.size() + NumDigits + 1);
    result = prefix;
    decodeDecimalFrom64Bit(result, digitEncoding);
    result.push_back('>');
    return result;
  }

  // From the `Id` (which is expected to be of type `EncodedVal`, else an
  // `AD_CONTRACT_CHECK` fails), extract the integer encoding of the prefix and
  // of the payload. NOTE: This interprets the bits according to the narrow
  // layout. For an `Id` in the wide layout, the returned prefix index is the
  // raw tag field, which is `>= maxNumPrefixes_` (because the layout bit is
  // set), so comparisons against valid narrow prefix indices always fail, but
  // the returned payload is meaningless.
  static std::pair<uint64_t, uint64_t> splitIntoPrefixIdxAndPayload(Id id) {
    AD_CONTRACT_CHECK(
        id.getDatatype() == Datatype::EncodedVal,
        "datatype must be `EncodedVal` for `splitIntoPrefixIdxAndPayload`");
    static constexpr auto mask =
        ad_utility::bitMaskForLowerBits(NumBitsEncoding);
    auto digitEncoding = id.getEncodedVal() & mask;
    // Get the index of the prefix.
    auto prefixIdx = id.getEncodedVal() >> NumBitsEncoding;
    return std::make_pair(prefixIdx, digitEncoding);
  }

  // The same as `splitIntoPrefixIdxAndPayload` except that the payload is
  // returned decoded.
  static std::pair<uint64_t, uint64_t> splitIntoPrefixIdxAndDecodedPayload(
      Id id) {
    auto [prefix, payload] = splitIntoPrefixIdxAndPayload(id);
    return {prefix, decodeDecimalFrom64Bit(payload)};
  }

  // The index of a narrow prefix. This is the same prefix that is used for
  // `makeIdFromPrefixIdxAndPayload` and returned from
  // `splitIntoPrefixIdxAndPayload`.
  std::optional<uint64_t> getIndexOfPrefix(
      std::string_view prefixWithoutAngleBrackets) const {
    auto it = ql::ranges::find(prefixes_,
                               absl::StrCat("<", prefixWithoutAngleBrackets));
    if (it == prefixes_.end()) {
      return std::nullopt;
    }
    return static_cast<size_t>(it - prefixes_.begin());
  }

  // Conversion to and from JSON.
  static constexpr const char* jsonKey_ =
      "prefixes-with-leading-angle-brackets";
  static constexpr const char* jsonKeyWide_ =
      "wide-prefixes-with-leading-angle-brackets";
  friend void to_json(nlohmann::json& j,
                      const EncodedIriManagerImpl& encodedIriManager) {
    j[jsonKey_] = encodedIriManager.prefixes_;
    j[jsonKeyWide_] = encodedIriManager.widePrefixes_;
  }
  friend void from_json(const nlohmann::json& j,
                        EncodedIriManagerImpl& encodedIriManager) {
    // When loading an existing index, EncodedIriManagers must be de-serialized
    // from json through this method. This is required so that
    // 1. the user specified prefixes set for the index build are loaded and
    // 2. that exactly the hardcoded prefixes that the index was built with are
    // loaded.
    //
    // This keeps compatibility with already built indices. Newly built indices
    // go through the normal constructor and use the current hardcoded
    // prefixes. Indices built before the wide layout was introduced have no
    // wide prefixes (their narrow layout is bit-compatible, as they never had
    // more than `maxNumPrefixes_` prefixes and thus never set the layout bit).
    encodedIriManager.prefixes_ =
        static_cast<std::vector<std::string>>(j[jsonKey_]);
    encodedIriManager.widePrefixes_ =
        j.contains(jsonKeyWide_)
            ? static_cast<std::vector<std::string>>(j[jsonKeyWide_])
            : std::vector<std::string>{};
  }

  // Hash support for use in `TestIndexConfig`.
  template <typename H>
  friend H AbslHashValue(H h, const EncodedIriManagerImpl& manager) {
    return H::combine(std::move(h), manager.prefixes_, manager.widePrefixes_);
  }

  // Equality operator for use in `TestIndexConfig`.
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(EncodedIriManagerImpl, prefixes_,
                                              widePrefixes_)

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

  // Helper for decoding numbers. Calls `F` for every digit (from high to low)
  // in the decoded representation of `encoded`.
  template <typename F>
  static void decodeDecimalFrom64BitHelper(F processDigit, uint64_t encoded) {
    size_t shift = NumBitsEncoding - NibbleSize;
    auto numTrailingZeros = absl::countr_zero(encoded);
    size_t numTrailingZeroNibbles = numTrailingZeros / NibbleSize;
    size_t len = NumDigits - numTrailingZeroNibbles;
    for (size_t i = 0; i < len; ++i) {
      processDigit(((encoded >> shift) & 0xF) - 1);
      shift -= NibbleSize;
    }
  }

  // The inverse of `encodeDecimalToNBit`. The result is appended to the
  // `result` string.
  static void decodeDecimalFrom64Bit(std::string& result, uint64_t encoded) {
    decodeDecimalFrom64BitHelper(
        [&result](auto digit) {
          result.push_back(static_cast<char>(digit + '0'));
        },
        encoded);
  }

  // Overload of `decodeDecimalFrom64Bit` that returns the result as a
  // `uint64_t`.
  static uint64_t decodeDecimalFrom64Bit(uint64_t encoded) {
    uint64_t result = 0;
    decodeDecimalFrom64BitHelper(
        [&result](auto digit) {
          result *= 10;
          result += digit;
        },
        encoded);
    return result;
  }
};

// The default encoder for IRIs in QLever: 60 bits are used for the complete
// encoding. The highest bit selects the layout: `0` = 7 bits for the prefixes
// (up to 128 prefixes) and 52 bits for the digits (up to 13 digits); `1` = 2
// bits for the prefixes (up to 4 wide prefixes) and 57 bits for the digits as
// a binary number (up to 17 digits). Additionally the prefix for newly created
// graphs is always set.
struct AlwaysOnPrefixes {
  static constexpr std::array<std::string_view, 1> value = {
      QLEVER_NEW_GRAPH_PREFIX};
};
using EncodedIriManager =
    EncodedIriManagerImpl<Id::numDataBits, 8, AlwaysOnPrefixes>;

#endif  // QLEVER_SRC_INDEX_VOCABULARY_ENCODEDIRIMANAGER_H
