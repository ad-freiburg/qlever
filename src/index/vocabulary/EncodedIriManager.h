// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_ENCODEDIRIMANAGER_H
#define QLEVER_SRC_INDEX_VOCABULARY_ENCODEDIRIMANAGER_H

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "backports/three_way_comparison.h"
#include "global/Id.h"
#include "index/vocabulary/EncodedIriPattern.h"
#include "index/vocabulary/NibbleEncoding.h"
#include "util/BitUtils.h"
#include "util/Log.h"
#include "util/json.h"

namespace detail {
// Sort the `prefixes` (which have to be specified without the enclosing angle
// brackets) and remove duplicates. Throw if they are invalid, that is, if there
// are more than `maxNumPrefixes` of them, if one of them is a prefix of another
// one, or if one of them starts with `<`. Return the prefixes, each of them
// with a leading `<`.
std::vector<std::string> sortAndCheckPrefixes(std::vector<std::string> prefixes,
                                              size_t maxNumPrefixes);

// Build the list of patterns of an `EncodedIriManagerImpl` from the plain
// `prefixes` (see `sortAndCheckPrefixes`, they become plain prefix patterns
// with `numBitsEncoding` bits) followed by the general `patterns` in the given
// order. The `prefix_` of each pattern has to be specified without the leading
// `<`, which is added here. Throw if one of the patterns needs more than
// `numBitsEncoding` bits (see `encodedIri::validatePattern`) or if there are
// more than `maxNumPatterns` patterns in total.
std::vector<encodedIri::Pattern> makePatterns(
    std::vector<std::string> prefixes,
    std::vector<encodedIri::Pattern> patterns, size_t numBitsEncoding,
    size_t maxNumPatterns);

// Find the first of the `patterns` that `repr` matches (see
// `encodedIri::encodePayload`). Return the index of that pattern together with
// the encoded payload, or `std::nullopt` if there is no such pattern.
std::optional<std::pair<size_t, uint64_t>> matchPatterns(
    const std::vector<encodedIri::Pattern>& patterns, std::string_view repr);

// Conversion of the `patterns` to and from JSON. As long as only plain prefixes
// are used (which is the default), the legacy format (a simple list of the
// prefixes with leading `<`) is written, such that the format of the index
// metadata doesn't change. `patternsFromJson` reads both formats and validates
// the patterns (see `makePatterns` for the arguments), because the index
// metadata might have been manipulated.
void patternsToJson(nlohmann::json& j,
                    const std::vector<encodedIri::Pattern>& patterns,
                    size_t numBitsEncoding);
std::vector<encodedIri::Pattern> patternsFromJson(const nlohmann::json& j,
                                                  size_t numBitsEncoding,
                                                  size_t maxNumPatterns);
}  // namespace detail

// This class allows the encoding of IRIs that follow a fixed pattern directly
// into an `Id`. In the simplest case (which is what the `--encode-as-id`
// option of the index builder configures), such a pattern is a fixed prefix
// that is followed by a sequence of decimal digits, for example
// <http://example.org/12345> with the prefix `http://example.org/` and the
// digit sequence `12345`. Arbitrary patterns of the form
// `<prefix><number><suffix><number>...>` can be configured via
// `encodedIri::Pattern` (see `EncodedIriPattern.h`). This is implemented as
// follows:
//
// An `Id` has 64 bits, of which the `NumBitsTotal` rightmost bits are
// used for the encoding. The `64 - NumBitsTotal` leftmost bits are ignored when
// decoding and can be used for other purposes. The next `NumBitsTags` bits
// encode the pattern of the IRI; that is, at most `2 ** NumBitsTags` different
// patterns can be used. The remaining `NumBitsTotal - NumBitsTags` bits (the
// payload) are used to encode the numbers of the IRI. The first number of a
// pattern is stored in the most significant bits of the payload, the last one
// in the least significant bits.
//
// A number can be encoded in one of two ways (see
// `encodedIri::NumberEncoding`). A plain prefix uses the `Nibbles` encoding
// (see `NibbleEncoding.h` for the details), which stores each decimal digit in
// four bits and makes sure that the order of the encoded values corresponds to
// the lexical order of the original IRIs.
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
// The other encoding, `Binary`, stores the value of a number and leaves out
// the bits that are known in advance. It is the default for the numbers of a
// general pattern, because it is much more compact, but it is only
// order-preserving with respect to the numeric and not with respect to the
// lexicographic order of the IRIs. This is not a correctness problem (`JOIN`,
// `GROUP BY`, `DISTINCT` etc. only require a consistent order), but `ORDER BY`
// and range filters on the affected IRIs follow the order of the encoding.
//
// NOTE: Only IRIs that fulfill the constraints of a pattern can be encoded. For
// example, if 4 times the number of digits is larger than
// `NumBitsTotal - NumBitsTags`, the IRI will not be encoded (but stored as a
// regular IRI). See the bottom of the file for the default values of
// `NumBitsTotal` and `NumBitsTags`.
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
  // The tag is stored by shifting it by `NumBitsEncoding`, which requires
  // `NumBitsEncoding` to be smaller than 64.
  static_assert(NumBitsEncoding < 64);

  // The patterns of the IRIs that will be encoded. The index of a pattern in
  // this vector is the tag that is stored in the `Id`. The `prefix_` of each of
  // them starts with `<`.
  std::vector<encodedIri::Pattern> patterns_;

  static constexpr auto maxNumPrefixes_ = 1ULL << NumBitsTags;

  // By default, `patterns_` is empty, so no IRI will be encoded.
  // NOTE: When loading an existing index, in particular one from an older
  // QLever version with different hardcoded prefixes, it is crucial to use the
  // deserialization from JSON to initialize the EncodedIriManager. See the
  // note in `from_json`.
  EncodedIriManagerImpl() : EncodedIriManagerImpl(std::vector<std::string>{}) {}

  // Construct from a list of plain prefixes and a list of general patterns. The
  // prefixes and the `prefix_` of the patterns have to be specified without any
  // brackets, so e.g. "http://example.org/" if IRIs of the form
  // `<http://example.org/1234>` should be encoded. The prefixes are sorted, and
  // the patterns are appended to them in the given order, so changing the order
  // of the patterns changes the `Id`s of the encoded IRIs.
  // NOTE: When loading an existing index, in particular one from an older
  // QLever version with different hardcoded prefixes, it is crucial to use the
  // deserialization from JSON to initialize the EncodedIriManager. See the
  // note in `from_json`.
  explicit EncodedIriManagerImpl(
      std::vector<std::string> prefixesWithoutAngleBrackets,
      std::vector<encodedIri::Pattern> patterns = {}) {
    // Add hardcoded prefixes.
    for (const auto& prefix : HardcodedPrefixes) {
      // Adding a hardcoded prefix a second time in the constructor is an error.
      AD_CONTRACT_CHECK(
          !ad_utility::contains(prefixesWithoutAngleBrackets, prefix));
      prefixesWithoutAngleBrackets.emplace_back(prefix);
    }
    patterns_ = detail::makePatterns(std::move(prefixesWithoutAngleBrackets),
                                     std::move(patterns), NumBitsEncoding,
                                     maxNumPrefixes_);
  }

  // Try to encode the given string as an `Id`. If the encoding fails, return
  // `std::nullopt`. This happens in one of the following cases:
  //
  // 1. The string is not an `<iriref-in-angle-brackets>`.
  // 2. The string doesn't match any of the `patterns_`.
  // 3. One of the numbers of the matching pattern violates its constraints,
  //    for example because it has too many digits.
  std::optional<Id> encode(std::string_view repr) const {
    auto match = detail::matchPatterns(patterns_, repr);
    if (!match.has_value()) {
      return std::nullopt;
    }
    const auto& [tag, payload] = match.value();
    return makeIdFromPrefixIdxAndPayload(tag, payload);
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
    auto [tag, payload] = splitIntoPrefixIdxAndPayload(id);
    return encodedIri::decodeToIri(patterns_.at(tag), payload);
  }

  // From the `Id` (which is expected to be of type `EncodedVal`, else an
  // `AD_CONTRACT_CHECK` fails), extract the integer encoding of the prefix and
  // of the payload.
  // NOTE: The payload is only a single nibble-encoded number (which
  // `decodeDecimalFrom64Bit` can decode) if the prefix is a plain prefix, so
  // callers have to check the prefix index before decoding the payload.
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

  // The index of a prefix. This is the same prefix that is used for
  // `makeIdFromPrefixIdxAndPayload` and returned from
  // `splitIntoPrefixIdxAndPayload`. If several patterns share the same prefix,
  // then the index of the first of them is returned.
  std::optional<uint64_t> getIndexOfPrefix(
      std::string_view prefixWithoutAngleBrackets) const {
    auto prefix = absl::StrCat("<", prefixWithoutAngleBrackets);
    auto it =
        ql::ranges::find(patterns_, prefix, &encodedIri::Pattern::prefix_);
    if (it == patterns_.end()) {
      return std::nullopt;
    }
    return static_cast<size_t>(it - patterns_.begin());
  }

  // Conversion to and from JSON, see `detail::patternsToJson` and
  // `detail::patternsFromJson`.
  friend void to_json(nlohmann::json& j,
                      const EncodedIriManagerImpl& encodedIriManager) {
    detail::patternsToJson(j, encodedIriManager.patterns_, NumBitsEncoding);
  }
  friend void from_json(const nlohmann::json& j,
                        EncodedIriManagerImpl& encodedIriManager) {
    // When loading an existing index, EncodedIriManagers must be de-serialized
    // from json through this method. This is required so that
    // 1. the user specified prefixes and patterns set for the index build are
    // loaded and
    // 2. that exactly the hardcoded prefixes that the index was built with are
    // loaded.
    //
    // This keeps compatibility with already built indices. Newly built indices
    // go through the normal constructor and use the current hardcoded
    // prefixes.
    encodedIriManager.patterns_ =
        detail::patternsFromJson(j, NumBitsEncoding, maxNumPrefixes_);
  }

  // Hash support for use in `TestIndexConfig`.
  template <typename H>
  friend H AbslHashValue(H h, const EncodedIriManagerImpl& manager) {
    return H::combine(std::move(h), manager.patterns_);
  }

  // Equality operator for use in `TestIndexConfig`.
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(EncodedIriManagerImpl, patterns_)

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
