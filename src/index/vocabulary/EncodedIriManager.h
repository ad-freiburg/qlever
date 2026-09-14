// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_VOCABULARY_ENCODEDIRIMANAGER_H
#define QLEVER_SRC_INDEX_VOCABULARY_ENCODEDIRIMANAGER_H

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "backports/three_way_comparison.h"
#include "global/Id.h"
#include "index/vocabulary/NibbleEncoding.h"
#include "util/BitUtils.h"
#include "util/Log.h"
#include "util/json.h"

namespace detail {
// Find the first prefix in `prefixes` that `repr` starts with, and match the
// remainder of `repr` against the pattern `([0-9]+)>` with at most
// `maxNumDigits` digits. Return the index of the matching prefix together with
// the digits (as a `string_view` into `repr`), or `std::nullopt` if there is no
// such prefix or the remainder does not match.
std::optional<std::pair<size_t, std::string_view>> matchPrefixAndDigits(
    const std::vector<std::string>& prefixes, std::string_view repr,
    size_t maxNumDigits);

// Sort the `prefixes` (which have to be specified without the enclosing angle
// brackets) and remove duplicates. Throw if they are invalid, that is, if there
// are more than `maxNumPrefixes` of them, if one of them is a prefix of another
// one, or if one of them starts with `<`. Return the prefixes, each of them
// with a leading `<`.
std::vector<std::string> sortAndCheckPrefixes(std::vector<std::string> prefixes,
                                              size_t maxNumPrefixes);
}  // namespace detail

// This class allows the encoding of IRIs that start with a fixed prefix
// followed by a sequence of decimal digits directly into an `Id`. For
// example, <http://example.org/12345> with digit sequence `12345` and
// prefix `http://example.org/`. This is implemented as follows:
//
// An `Id` has 64 bits, of which the `NumBitsTotal` rightmost bits are
// used for the encoding. The `64 - NumBitsTotal` leftmost bits are ignored when
// decoding and can be used for other purposes. The next `NumBitsTags` bits
// encode the IRI prefix; that is, at most `2 ** NumBitsTags` different prefixes
// can be used. The remaining `NumBitsTotal - NumBitsTags` bits are used to
// encode the digits that follow the prefix.
//
// The digits are encoded with the nibble encoding (see `NibbleEncoding.h` for
// the details), which stores each decimal digit in four bits and makes sure
// that the order of the encoded values corresponds to the lexical order of the
// original IRIs.
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
  static constexpr size_t NibbleSize = encodedIri::NibbleSize;
  static constexpr size_t NumDigits = NumBitsEncoding / NibbleSize;
  static_assert(NumBitsEncoding % NibbleSize == 0);

  static_assert(NumBitsTotal <= 64);
  static_assert(NumBitsTags <= 64);
  static_assert(NumDigits > 0);

  // The prefixes of the IRIs that will be encoded.
  std::vector<std::string> prefixes_;

  static constexpr auto maxNumPrefixes_ = 1ULL << NumBitsTags;

  // By default, `prefixes_` is empty, so no IRI will be encoded.
  // NOTE: When loading an existing index, in particular one from an older
  // QLever version with different hardcoded prefixes, it is crucial to use the
  // deserialization from JSON to initialize the EncodedIriManager. See the
  // note in `from_json`.
  EncodedIriManagerImpl() : EncodedIriManagerImpl(std::vector<std::string>{}) {}

  // Construct from the list of prefixes. The prefixes have to be specified
  // without any brackets, so e.g. "http://example.org/" if IRIs of the form
  // `<http://example.org/1234>` should be encoded.
  // NOTE: When loading an existing index, in particular one from an older
  // QLever version with different hardcoded prefixes, it is crucial to use the
  // deserialization from JSON to initialize the EncodedIriManager. See the
  // note in `from_json`.
  explicit EncodedIriManagerImpl(
      std::vector<std::string> prefixesWithoutAngleBrackets) {
    // Add hardcoded prefixes.
    for (const auto& prefix : HardcodedPrefixes) {
      // Adding a hardcoded prefix a second time in the constructor is an error.
      AD_CONTRACT_CHECK(
          !ad_utility::contains(prefixesWithoutAngleBrackets, prefix));
      prefixesWithoutAngleBrackets.emplace_back(prefix);
    }
    prefixes_ = detail::sortAndCheckPrefixes(
        std::move(prefixesWithoutAngleBrackets), maxNumPrefixes_);
  }

  // Try to encode the given string as an `Id`. If the encoding fails, return
  // `std::nullopt`. This happens in one of the following cases:
  //
  // 1. The string is not an `<iriref-in-angle-brackets>`
  // 2. The string does not start with any of the `prefixes_`
  // 3. After the matching prefix, there are characters other than `[0-9]`
  // 4. There are more digits than fit into `NumBitsEncoding` (4 bits / digit)
  std::optional<Id> encode(std::string_view repr) const {
    auto match = detail::matchPrefixAndDigits(prefixes_, repr, NumDigits);
    if (!match.has_value()) {
      return std::nullopt;
    }
    const auto& [prefixIndex, numString] = match.value();
    return makeIdFromPrefixIdxAndPayload(prefixIndex,
                                         encodeDecimalToNBit(numString));
  }

  // combine the integer representation of the prefix and of the payload into a
  // single `Id` with datatype `EncodedValue`.
  static Id makeIdFromPrefixIdxAndPayload(uint64_t prefixIdx,
                                          uint64_t payload) {
    return Id::makeFromEncodedVal(payload | (prefixIdx << NumBitsEncoding));
  }

  // Convert an `Id` that was encoded using this encoder back to a string.
  // Throw an exception if the `Id` has a datatype different from `EncodedVal`.
  std::string toString(Id id) const {
    AD_CORRECTNESS_CHECK(id.getDatatype() == Datatype::EncodedVal);
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
  // of the payload.
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

  // The index of a prefix. This is the same prefix that is used for
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
  friend void to_json(nlohmann::json& j,
                      const EncodedIriManagerImpl& encodedIriManager) {
    j[jsonKey_] = encodedIriManager.prefixes_;
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
    // prefixes.
    encodedIriManager.prefixes_ =
        static_cast<std::vector<std::string>>(j[jsonKey_]);
  }

  // Hash support for use in `TestIndexConfig`.
  template <typename H>
  friend H AbslHashValue(H h, const EncodedIriManagerImpl& manager) {
    return H::combine(std::move(h), manager.prefixes_);
  }

  // Equality operator for use in `TestIndexConfig`.
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(EncodedIriManagerImpl, prefixes_)

  // Encode the `numberStr` (which may only consist of digits) into a 64-bit
  // number.
  static constexpr uint64_t encodeDecimalToNBit(std::string_view numberStr) {
    return encodedIri::encodeDigitsAsNibbles(numberStr, NumBitsEncoding);
  }

  // The inverse of `encodeDecimalToNBit`. The result is appended to the
  // `result` string.
  static void decodeDecimalFrom64Bit(std::string& result, uint64_t encoded) {
    encodedIri::decodeNibblesToDigits(result, encoded, NumBitsEncoding);
  }

  // Overload of `decodeDecimalFrom64Bit` that returns the result as a
  // `uint64_t`.
  static uint64_t decodeDecimalFrom64Bit(uint64_t encoded) {
    return encodedIri::decodeNibblesToNumber(encoded, NumBitsEncoding);
  }
};

// The default encoder for IRIs in QLever: 60 bits are used for the complete
// encoding, 8 bits are used for the prefixes (which allows up to 256
// prefixes). This leaves 52 bits for the digits, so up to 13 digits can be
// encoded. Additionally the prefix for newly created graphs is always set.
struct AlwaysOnPrefixes {
  static constexpr std::array<std::string_view, 1> value = {
      QLEVER_NEW_GRAPH_PREFIX};
};
using EncodedIriManager =
    EncodedIriManagerImpl<Id::numDataBits, 8, AlwaysOnPrefixes>;

#endif  // QLEVER_SRC_INDEX_VOCABULARY_ENCODEDIRIMANAGER_H
