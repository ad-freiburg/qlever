// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_ENCODEDIRISCHEMETESTHELPERS_H
#define QLEVER_TEST_UTIL_ENCODEDIRISCHEMETESTHELPERS_H

#include <memory>
#include <string>
#include <vector>

#include "backports/StartsWithAndEndsWith.h"
#include "index/EncodedIriScheme.h"
#include "util/BitUtils.h"
#include "util/CtreHelpers.h"

namespace ad_utility::testing {

// A custom encoding scheme for IRIs that mixes constant strings with two
// numbers, namely `<somePrefix://num_123_anotherNum_24>` and
// `<somePrefix://num_123_otherNum_24>`, where only the `123` and the `24` are
// load-bearing. The two variants use one tag each. The part before the first
// number (`somePrefix://num_` in the example) is configurable, which is also
// used to test the serialization of the configuration of a scheme.
//
// NOTE: This scheme is not order-preserving, because the constant string
// between the two numbers starts with `_`, which is larger than any digit (see
// the documentation of `EncodedIriScheme`).
class TwoNumbersScheme : public qlever::EncodedIriScheme {
 public:
  // The number of bits that are used for each of the two numbers, so each of
  // them may have at most six digits.
  static constexpr size_t numBitsPerNumber = 24;
  // The JSON key for the configurable prefix.
  static constexpr const char* prefixKey_ = "prefix";

 private:
  // The prefix, without angle brackets, e.g. `somePrefix://num_`.
  std::string prefix_;
  // The same prefix with a leading `<`, to avoid a string concatenation for
  // every call to `encode`.
  std::string prefixWithAngleBracket_;

 public:
  explicit TwoNumbersScheme(std::string prefix = "somePrefix://num_")
      : prefix_{std::move(prefix)},
        prefixWithAngleBracket_{absl::StrCat("<", prefix_)} {}

  // The name, both as a static function (which is required by
  // `qlever::registerEncodedIriScheme`) and as the virtual function.
  static std::string schemeName() { return "test-two-numbers"; }
  std::string name() const override { return schemeName(); }

  std::vector<std::string> prefixes() const override { return {prefix_}; }

  size_t numTags() const override { return 2; }

  size_t numPayloadBits() const override { return 2 * numBitsPerNumber; }

  std::optional<TagAndPayload> encode(
      std::string_view iriWithAngleBrackets) const override {
    if (!ql::starts_with(iriWithAngleBrackets, prefixWithAngleBracket_)) {
      return std::nullopt;
    }
    iriWithAngleBrackets.remove_prefix(prefixWithAngleBracket_.size());
    static constexpr auto regex = ctll::fixed_string{
        "(?<first>[0-9]+)_(?<kind>another|other)Num_(?<second>[0-9]+)>"};
    auto match = ctre::match<regex>(iriWithAngleBrackets);
    if (!match) {
      return std::nullopt;
    }
    constexpr ctll::fixed_string firstGroup = "first";
    constexpr ctll::fixed_string kindGroup = "kind";
    constexpr ctll::fixed_string secondGroup = "second";
    auto first = match.template get<firstGroup>().to_view();
    auto second = match.template get<secondGroup>().to_view();
    using namespace qlever::encodedIri;
    if (!fitsIntoNibbles(first, numBitsPerNumber) ||
        !fitsIntoNibbles(second, numBitsPerNumber)) {
      return std::nullopt;
    }
    uint64_t payload =
        (encodeDecimalNibbles(first, numBitsPerNumber) << numBitsPerNumber) |
        encodeDecimalNibbles(second, numBitsPerNumber);
    size_t localTag =
        match.template get<kindGroup>().to_view() == "another" ? 0 : 1;
    return TagAndPayload{localTag, payload};
  }

  std::string decode(size_t localTag, uint64_t payload) const override {
    AD_CONTRACT_CHECK(localTag < numTags());
    std::string result = prefixWithAngleBracket_;
    qlever::encodedIri::decodeDecimalNibbles(
        result, payload >> numBitsPerNumber, numBitsPerNumber);
    result += localTag == 0 ? "_anotherNum_" : "_otherNum_";
    qlever::encodedIri::decodeDecimalNibbles(result, lowerNumber(payload),
                                             numBitsPerNumber);
    result.push_back('>');
    return result;
  }

  Numbers decodeNumbers(size_t localTag, uint64_t payload) const override {
    AD_CONTRACT_CHECK(localTag < numTags());
    using qlever::encodedIri::decodeDecimalNibblesToNumber;
    return {
        decodeDecimalNibblesToNumber(payload >> numBitsPerNumber,
                                     numBitsPerNumber),
        decodeDecimalNibblesToNumber(lowerNumber(payload), numBitsPerNumber)};
  }

  nlohmann::json toJson() const override {
    nlohmann::json j;
    j[prefixKey_] = prefix_;
    return j;
  }

  static qlever::EncodedIriSchemePtr fromJson(const nlohmann::json& j) {
    return std::make_shared<TwoNumbersScheme>(
        j.at(prefixKey_).get<std::string>());
  }

 private:
  // The encoding of the second number, which lives in the lower bits of the
  // `payload`.
  static uint64_t lowerNumber(uint64_t payload) {
    return payload & ad_utility::bitMaskForLowerBits(numBitsPerNumber);
  }
};
AD_REGISTER_ENCODED_IRI_SCHEME(TwoNumbersScheme);

// Helper to create a `shared_ptr` to a `TwoNumbersScheme`.
inline qlever::EncodedIriSchemePtr twoNumbersScheme(
    std::string prefix = "somePrefix://num_") {
  return std::make_shared<TwoNumbersScheme>(std::move(prefix));
}

}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_ENCODEDIRISCHEMETESTHELPERS_H
