// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_VOCABULARY_ENCODEDIRIMANAGER_H
#define QLEVER_SRC_INDEX_VOCABULARY_ENCODEDIRIMANAGER_H

#include <absl/strings/str_cat.h>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "backports/three_way_comparison.h"
#include "global/Id.h"
#include "index/vocabulary/EncodedIriPattern.h"
#include "util/BitUtils.h"
#include "util/Log.h"
#include "util/json.h"

// This class allows the encoding of IRIs that follow a fixed pattern directly
// into an `Id`. In the simplest case (which is what the `--encode-as-id`
// option of the index builder configures), such a pattern is a fixed prefix
// that is followed by a sequence of decimal digits, for example
// <http://example.org/12345> with the prefix `http://example.org/` and the
// digit sequence `12345`. Arbitrary patterns of the form
// `<prefix><number><separator><number>...>` can be configured via
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
// `encodedIri::NumberEncoding`). A plain prefix uses the `Digits` encoding,
// which makes sure that the order of the encoded values corresponds to the
// lexical order of the original IRIs. Each decimal digit is encoded as a 4-bit
// nibble, where digit `i` is encoded as `i+1` and converted to a hexadecimal
// number. The nibbles are stored left-aligned (not right-aligned) and filled on
// the right with zeroes.
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
    addPlainPrefixes(std::move(prefixesWithoutAngleBrackets));
    for (auto& pattern : patterns) {
      checkNoLeadingAngleBracket(pattern.prefix_,
                                 "of the patterns for encoded IRIs");
      encodedIri::validatePattern(pattern, NumBitsEncoding);
      pattern.prefix_.insert(0, 1, '<');
      patterns_.push_back(std::move(pattern));
    }
    checkNumberOfPatterns(patterns_.size());
  }

  // Try to encode the given string as an `Id`. If the encoding fails, return
  // `std::nullopt`. This happens in one of the following cases:
  //
  // 1. The string is not an `<iriref-in-angle-brackets>`.
  // 2. The string doesn't match any of the `patterns_`.
  // 3. One of the numbers of the matching pattern violates its constraints,
  //    for example because it has too many digits.
  std::optional<Id> encode(std::string_view repr) const {
    for (size_t tag = 0; tag < patterns_.size(); ++tag) {
      const auto& pattern = patterns_[tag];
      if (!ql::starts_with(repr, pattern.prefix_)) {
        continue;
      }
      auto payload =
          encodePayload(pattern, repr.substr(pattern.prefix_.size()));
      if (payload.has_value()) {
        return makeIdFromPrefixIdxAndPayload(tag, payload.value());
      }
    }
    return std::nullopt;
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
    // Get only the rightmost bits that represent the numbers.
    auto [prefixIdx, payload] = splitIntoPrefixIdxAndPayload(id);
    const auto& pattern = patterns_.at(prefixIdx);
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
      if (part.encoding_ == encodedIri::NumberEncoding::Digits) {
        encodedIri::decodeDigits(result, stored, part.numBits_);
      } else {
        absl::StrAppend(&result, encodedIri::decompressNumber(part, stored));
      }
      result.append(part.separator_);
    }
    result.push_back('>');
    return result;
  }

  // Combine the integer encoding of the digits and the prefix string into a
  // result string that represents an IRI. This is the special case of
  // `toString` for a plain prefix, for callers that have the prefix at hand
  // but not the manager.
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

  // The index of a prefix. This is the same prefix that is used for
  // `makeIdFromPrefixIdxAndPayload` and returned from
  // `splitIntoPrefixIdxAndPayload`. If several patterns share the same prefix,
  // then the index of the first of them is returned.
  std::optional<uint64_t> getIndexOfPrefix(
      std::string_view prefixWithoutAngleBrackets) const {
    auto prefix = absl::StrCat("<", prefixWithoutAngleBrackets);
    auto it = ql::ranges::find_if(
        patterns_, [&prefix](const encodedIri::Pattern& pattern) {
          return pattern.prefix_ == prefix;
        });
    if (it == patterns_.end()) {
      return std::nullopt;
    }
    return static_cast<size_t>(it - patterns_.begin());
  }

  // Conversion to and from JSON. As long as only plain prefixes are used (which
  // is the default), the legacy format (a simple list of the prefixes) is
  // written, such that the format of the index metadata doesn't change.
  static constexpr const char* jsonKey_ =
      "prefixes-with-leading-angle-brackets";
  static constexpr const char* jsonKeyPatterns_ = "patterns";
  friend void to_json(nlohmann::json& j,
                      const EncodedIriManagerImpl& encodedIriManager) {
    const auto& patterns = encodedIriManager.patterns_;
    auto isPlain = [](const encodedIri::Pattern& pattern) {
      return encodedIri::isPlainPrefixPattern(pattern, NumBitsEncoding);
    };
    if (ql::ranges::all_of(patterns, isPlain)) {
      std::vector<std::string> prefixes;
      prefixes.reserve(patterns.size());
      for (const auto& pattern : patterns) {
        prefixes.push_back(pattern.prefix_);
      }
      j[jsonKey_] = std::move(prefixes);
    } else {
      j[jsonKeyPatterns_] = patterns;
    }
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
    auto& patterns = encodedIriManager.patterns_;
    patterns.clear();
    if (j.contains(jsonKeyPatterns_)) {
      patterns = j.at(jsonKeyPatterns_).get<std::vector<encodedIri::Pattern>>();
      // The patterns come from the index metadata, which might have been
      // manipulated, so they have to be validated again.
      for (const auto& pattern : patterns) {
        encodedIri::validatePattern(pattern, NumBitsEncoding);
      }
    } else {
      for (auto& prefix : j.at(jsonKey_).get<std::vector<std::string>>()) {
        patterns.push_back(
            encodedIri::plainPrefixPattern(std::move(prefix), NumBitsEncoding));
      }
    }
    checkNumberOfPatterns(patterns.size());
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
  static uint64_t encodeDecimalToNBit(std::string_view numberStr) {
    return encodedIri::encodeDigits(numberStr, NumBitsEncoding);
  }

  // The inverse of `encodeDecimalToNBit`. The result is appended to the
  // `result` string.
  static void decodeDecimalFrom64Bit(std::string& result, uint64_t encoded) {
    encodedIri::decodeDigits(result, encoded, NumBitsEncoding);
  }

  // Overload of `decodeDecimalFrom64Bit` that returns the result as a
  // `uint64_t`.
  static uint64_t decodeDecimalFrom64Bit(uint64_t encoded) {
    return encodedIri::decodeDigitsToNumber(encoded, NumBitsEncoding);
  }

 private:
  // Throw if the `prefix` (which the `origin` describes for the error message)
  // starts with a `<`, which the manager adds itself.
  static void checkNoLeadingAngleBracket(std::string_view prefix,
                                         std::string_view origin) {
    if (ql::starts_with(prefix, '<')) {
      throw std::runtime_error(absl::StrCat(
          "The prefixes ", origin,
          " must not be enclosed in angle brackets; here is a violating "
          "prefix: \"",
          prefix, "\""));
    }
  }

  // Throw if `numPatterns` patterns don't fit into the `NumBitsTags` bits that
  // are reserved for the tag.
  static void checkNumberOfPatterns(size_t numPatterns) {
    if (numPatterns > maxNumPrefixes_) {
      throw std::runtime_error(absl::StrCat(
          "The number of prefixes and patterns for IRIs that are encoded "
          "directly in an ID is ",
          numPatterns, ", which is too many; the maximum is ",
          maxNumPrefixes_));
    }
  }

  // Sort and check the `prefixes` and add them to the `patterns_` as plain
  // prefix patterns.
  void addPlainPrefixes(std::vector<std::string> prefixes) {
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
          absl::StrCat("<", prefix), NumBitsEncoding));
    }
  }

  // Try to encode the `suffix` (the part of the IRI that follows the prefix of
  // the `pattern`, including the closing `>`) into the payload bits of an `Id`.
  static std::optional<uint64_t> encodePayload(
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
      if (part.encoding_ == encodedIri::NumberEncoding::Digits) {
        if (digits.size() * NibbleSize > part.numBits_) {
          return std::nullopt;
        }
        stored = encodedIri::encodeDigits(digits, part.numBits_);
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
