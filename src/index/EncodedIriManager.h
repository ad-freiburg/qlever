// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_ENCODEDVALUES_H
#define QLEVER_SRC_INDEX_ENCODEDVALUES_H

#include <absl/numeric/bits.h>
#include <absl/strings/str_join.h>

#include <limits>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "backports/three_way_comparison.h"
#include "global/Id.h"
#include "index/EncodedIriScheme.h"
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
// decoding and can be used for other purposes. The next `NumBitsTags` bits
// encode the IRI prefix; that is, at most `2 ** NumBitsTags` different prefixes
// can be used. The remaining `NumBitsTotal - NumBitsTags` bits are used to
// encode the digits that follow the prefix.
//
// The digits are encoded in the following non-standard way, which makes sure
// that the order of the encoded values corresponds to the lexical order of the
// original IRIs. Each decimal digit is encoded as a 4-bit nibble, where digit
// `i` is encoded as `i+1` and converted to a hexadecimal number. The nibbles
// are stored left-aligned (not right-aligned) and filled on the right with
// zeroes.
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
// In addition to these plain prefixes, arbitrary user-defined encoding schemes
// can be plugged in, see `EncodedIriScheme.h`. Such a scheme reserves a number
// of tags and is free to choose the structure of the IRIs that it encodes, for
// example `<somePrefix://num_123_anotherNum_24>`, where only the `123` and the
// `24` are load-bearing.
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
  static constexpr size_t NibbleSize = qlever::encodedIri::NibbleSize;
  static constexpr size_t NumDigits = NumBitsEncoding / NibbleSize;
  static_assert(NumBitsEncoding % NibbleSize == 0);

  static_assert(NumBitsTotal <= 64);
  static_assert(NumBitsTags <= 64);
  static_assert(NumDigits > 0);

  static constexpr auto maxNumPrefixes_ = 1ULL << NumBitsTags;

  // The marker for a `Slot` that belongs to a plain prefix and not to an
  // `EncodedIriScheme`.
  static constexpr size_t noScheme = std::numeric_limits<size_t>::max();

  // A single tag. It either belongs to a plain prefix, or to one of the
  // `EncodedIriScheme`s (see `EncodedIriScheme.h`).
  struct Slot {
    // The prefix with a leading `<`. For a slot that belongs to a scheme, this
    // is the lexicographically smallest of the prefixes of that scheme; it is
    // only used for the ordering of the tags and for error messages.
    std::string prefix_;
    // The index of the scheme in `schemes_`, or `noScheme` for a plain prefix.
    size_t schemeIdx_ = noScheme;
    // The index of this slot within its scheme, `0` for a plain prefix.
    size_t localTag_ = 0;

    // Return true if and only if this slot belongs to an `EncodedIriScheme`.
    bool isScheme() const { return schemeIdx_ != noScheme; }

    QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(Slot, prefix_, schemeIdx_,
                                                localTag_)

    template <typename H>
    friend H AbslHashValue(H h, const Slot& slot) {
      return H::combine(std::move(h), slot.prefix_, slot.schemeIdx_,
                        slot.localTag_);
    }
  };

  // A scheme together with the information where its tags live in this
  // manager.
  struct SchemeInfo {
    qlever::EncodedIriSchemePtr scheme_;
    // The tag of the slot with the local tag `0` of this scheme. The tags of
    // the scheme are `[firstTag_, firstTag_ + numTags_)`.
    size_t firstTag_ = 0;
    size_t numTags_ = 1;
    size_t numPayloadBits_ = 0;
    // The payloads of the scheme are shifted to the left by this number of
    // bits, such that they are left-aligned within the payload bits of the
    // `Id` (which is required for the ordering of the encoded `Id`s).
    size_t payloadShift_ = 0;
  };

 private:
  // A single entry of the table that is used to dispatch an IRI to its plain
  // prefix or to its scheme in `encode`.
  struct DispatchEntry {
    // The prefix with a leading `<`.
    std::string prefix_;
    // The tag of the prefix (for a plain prefix), or the first tag of the
    // scheme (for a scheme).
    size_t tag_ = 0;
    // The index of the scheme in `schemes_`, or `noScheme` for a plain prefix.
    size_t schemeIdx_ = noScheme;
  };

  // The tags, one entry per tag.
  std::vector<Slot> slots_;
  // The user-defined encoding schemes.
  std::vector<SchemeInfo> schemes_;
  // The dispatch table for `encode`, see `DispatchEntry` above.
  std::vector<DispatchEntry> dispatch_;

  // Tag type for the private constructor that restores a manager from JSON.
  struct FromJson {};

 public:
  // By default, there are no prefixes and no schemes, so no IRI will be
  // encoded.
  // NOTE: When loading an existing index, in particular one from an older
  // QLever version with different hardcoded prefixes, it is crucial to use the
  // deserialization from JSON to initialize the EncodedIriManager. See the
  // note in `from_json`.
  EncodedIriManagerImpl() : EncodedIriManagerImpl(std::vector<std::string>{}) {}

  // Construct from the list of prefixes and the (optional) list of
  // user-defined encoding schemes. The prefixes have to be specified without
  // any brackets, so e.g. "http://example.org/" if IRIs of the form
  // `<http://example.org/1234>` should be encoded. The tags are assigned in
  // the lexicographic order of the prefixes (where a scheme is ordered by its
  // smallest prefix and occupies a contiguous range of tags).
  // NOTE: When loading an existing index, in particular one from an older
  // QLever version with different hardcoded prefixes, it is crucial to use the
  // deserialization from JSON to initialize the EncodedIriManager. See the
  // note in `from_json`.
  explicit EncodedIriManagerImpl(
      std::vector<std::string> prefixesWithoutAngleBrackets,
      std::vector<qlever::EncodedIriSchemePtr> schemes = {}) {
    // Add hardcoded prefixes.
    for (const auto& prefix : HardcodedPrefixes) {
      // Adding a hardcoded prefix a second time in the constructor is an error.
      AD_CONTRACT_CHECK(
          !ad_utility::contains(prefixesWithoutAngleBrackets, prefix));
      prefixesWithoutAngleBrackets.emplace_back(prefix);
    }
    if (prefixesWithoutAngleBrackets.empty() && schemes.empty()) {
      return;
    }

    for (const auto& prefix : prefixesWithoutAngleBrackets) {
      if (ql::starts_with(prefix, '<')) {
        throw std::runtime_error(absl::StrCat(
            "The prefixes specified with `--encode-as-id` must not "
            "be enclosed in angle brackets; here is a violating prefix: \"",
            prefix, "\""));
      }
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

    // From now on, all prefixes are stored with a leading `<`. Note that this
    // doesn't change their relative order.
    std::vector<std::string> prefixes;
    prefixes.reserve(prefixesWithoutAngleBrackets.size());
    for (const auto& prefix : prefixesWithoutAngleBrackets) {
      prefixes.push_back(absl::StrCat("<", prefix));
    }

    // Set up the schemes, and compute the sort key (the smallest prefix) of
    // each of them.
    std::vector<std::pair<std::string, size_t>> sortKeysOfSchemes;
    for (const auto& scheme : schemes) {
      checkSchemeIsValid(scheme);
      for (const auto& info : schemes_) {
        if (info.scheme_->name() == scheme->name()) {
          throw std::runtime_error(absl::StrCat(
              "Two different encoding schemes for IRIs with the same name \"",
              scheme->name(), "\" were specified"));
        }
      }
      sortKeysOfSchemes.emplace_back(smallestPrefixOfScheme(*scheme),
                                     schemes_.size());
      schemes_.push_back(makeSchemeInfo(scheme));
    }

    // Assign the tags in the lexicographic order of the prefixes, where the
    // tags of a scheme are contiguous and sorted by its smallest prefix.
    ql::ranges::sort(sortKeysOfSchemes);
    auto prefixIt = prefixes.begin();
    for (const auto& [sortKey, schemeIdx] : sortKeysOfSchemes) {
      for (; prefixIt != prefixes.end() && *prefixIt < sortKey; ++prefixIt) {
        slots_.push_back(Slot{std::move(*prefixIt), noScheme, 0});
      }
      addSlotsForScheme(schemeIdx, sortKey);
    }
    for (; prefixIt != prefixes.end(); ++prefixIt) {
      slots_.push_back(Slot{std::move(*prefixIt), noScheme, 0});
    }
    finishInitialization();
  }

  // Try to encode the given string as an `Id`. If the encoding fails, return
  // `std::nullopt`. This happens in one of the following cases:
  //
  // 1. The string is not an `<iriref-in-angle-brackets>`
  // 2. The string does not start with any of the prefixes (neither with one of
  //    the plain prefixes, nor with one of the prefixes of one of the schemes)
  // 3. For a plain prefix: after the matching prefix, there are characters
  //    other than `[0-9]`, or there are more digits than fit into
  //    `NumBitsEncoding` (4 bits / digit)
  // 4. For a scheme: the scheme cannot encode the IRI
  std::optional<Id> encode(std::string_view repr) const {
    for (const auto& entry : dispatch_) {
      if (!ql::starts_with(repr, entry.prefix_)) {
        continue;
      }
      if (entry.schemeIdx_ == noScheme) {
        return encodeWithPlainPrefix(repr.substr(entry.prefix_.size()),
                                     entry.tag_);
      }
      return encodeWithScheme(repr, entry.schemeIdx_);
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
    // Get only the rightmost bits that represent the digits.
    auto [prefixIdx, digitEncoding] = splitIntoPrefixIdxAndPayload(id);
    const auto& slot = slots_.at(prefixIdx);
    if (!slot.isScheme()) {
      return toStringWithGivenPrefix(digitEncoding, slot.prefix_);
    }
    const auto& info = schemes_.at(slot.schemeIdx_);
    return info.scheme_->decode(slot.localTag_,
                                digitEncoding >> info.payloadShift_);
  }

  // Extract the raw numbers that are encoded in the given `Id`, in the order
  // in which they appear in the IRI. For a plain prefix this is always a
  // single number, for a scheme it is whatever the scheme reports (e.g.
  // `{123, 24}` for `<somePrefix://num_123_anotherNum_24>`).
  qlever::EncodedIriScheme::Numbers decodeNumbers(Id id) const {
    auto [prefixIdx, digitEncoding] = splitIntoPrefixIdxAndPayload(id);
    const auto& slot = slots_.at(prefixIdx);
    if (!slot.isScheme()) {
      return {decodeDecimalFrom64Bit(digitEncoding)};
    }
    const auto& info = schemes_.at(slot.schemeIdx_);
    return info.scheme_->decodeNumbers(slot.localTag_,
                                       digitEncoding >> info.payloadShift_);
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
  // returned decoded. NOTE: This function assumes that the `Id` was encoded
  // using a plain prefix; for the `Id`s of an `EncodedIriScheme` use
  // `decodeNumbers` above.
  static std::pair<uint64_t, uint64_t> splitIntoPrefixIdxAndDecodedPayload(
      Id id) {
    auto [prefix, payload] = splitIntoPrefixIdxAndPayload(id);
    return {prefix, decodeDecimalFrom64Bit(payload)};
  }

  // The index of a plain prefix. This is the same prefix that is used for
  // `makeIdFromPrefixIdxAndPayload` and returned from
  // `splitIntoPrefixIdxAndPayload`. Return `std::nullopt` if there is no plain
  // prefix that is equal to the argument (in particular, the prefixes of the
  // `EncodedIriScheme`s are not considered here).
  std::optional<uint64_t> getIndexOfPrefix(
      std::string_view prefixWithoutAngleBrackets) const {
    auto prefix = absl::StrCat("<", prefixWithoutAngleBrackets);
    auto it = ql::ranges::find_if(slots_, [&prefix](const Slot& slot) {
      return !slot.isScheme() && slot.prefix_ == prefix;
    });
    if (it == slots_.end()) {
      return std::nullopt;
    }
    return static_cast<size_t>(it - slots_.begin());
  }

  // The schemes that are used by this manager, together with the tags that
  // they occupy.
  const std::vector<SchemeInfo>& schemes() const { return schemes_; }

  // Check that the schemes of this manager are exactly the `expectedSchemes`
  // (compared via their name and their JSON representation), and throw a
  // descriptive exception if this is not the case. This is used to detect the
  // case that an index was built with a different set of schemes than the one
  // that the current process is configured with.
  void checkSchemesMatch(
      const std::vector<qlever::EncodedIriSchemePtr>& expectedSchemes) const {
    auto toStrings = [](auto&& schemes, auto getScheme) {
      std::vector<std::string> result;
      for (const auto& scheme : schemes) {
        result.push_back(getScheme(scheme)->toJsonWithName().dump());
      }
      ql::ranges::sort(result);
      return result;
    };
    auto actual = toStrings(
        schemes_, [](const SchemeInfo& info) { return info.scheme_; });
    auto expected =
        toStrings(expectedSchemes,
                  [](const qlever::EncodedIriSchemePtr& p) { return p; });
    if (actual != expected) {
      throw std::runtime_error(absl::StrCat(
          "The encoding schemes for IRIs that were configured differ from the "
          "ones that the index was built with. Configured: [",
          absl::StrJoin(expected, ", "), "], index: [",
          absl::StrJoin(actual, ", "), "]"));
    }
  }

  // Conversion to and from JSON.
  static constexpr const char* jsonKey_ =
      "prefixes-with-leading-angle-brackets";
  static constexpr const char* schemesJsonKey_ = "encoding-schemes";
  static constexpr const char* firstTagJsonKey_ = "first-tag";
  static constexpr const char* schemeJsonKey_ = "scheme";
  friend void to_json(nlohmann::json& j,
                      const EncodedIriManagerImpl& encodedIriManager) {
    std::vector<std::string> plainPrefixes;
    for (const auto& slot : encodedIriManager.slots_) {
      if (!slot.isScheme()) {
        plainPrefixes.push_back(slot.prefix_);
      }
    }
    j[jsonKey_] = std::move(plainPrefixes);
    // Only write the schemes if there are any, such that the JSON of an index
    // that doesn't use any schemes is exactly the same as before.
    if (encodedIriManager.schemes_.empty()) {
      return;
    }
    auto& schemes = j[schemesJsonKey_] = nlohmann::json::array();
    for (const auto& info : encodedIriManager.schemes_) {
      nlohmann::json entry;
      entry[firstTagJsonKey_] = info.firstTag_;
      entry[schemeJsonKey_] = info.scheme_->toJsonWithName();
      schemes.push_back(std::move(entry));
    }
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
    //
    // The same holds for the user-defined encoding schemes: their tags are
    // taken from the JSON and not recomputed, and the scheme objects are
    // restored via the global `EncodedIriSchemeRegistry`, which throws if a
    // scheme that the index uses is not registered in this process.
    auto prefixes = static_cast<std::vector<std::string>>(j[jsonKey_]);
    std::vector<std::pair<size_t, qlever::EncodedIriSchemePtr>> schemes;
    if (j.contains(schemesJsonKey_)) {
      for (const auto& entry : j[schemesJsonKey_]) {
        schemes.emplace_back(
            entry.at(firstTagJsonKey_).get<size_t>(),
            qlever::EncodedIriScheme::fromJsonWithName(entry[schemeJsonKey_]));
      }
    }
    encodedIriManager =
        EncodedIriManagerImpl{FromJson{}, std::move(prefixes), schemes};
  }

  // Hash support for use in `TestIndexConfig`.
  template <typename H>
  friend H AbslHashValue(H h, const EncodedIriManagerImpl& manager) {
    h = H::combine(std::move(h), manager.slots_);
    for (const auto& info : manager.schemes_) {
      h = H::combine(std::move(h), info.scheme_->toJsonWithName().dump(),
                     info.firstTag_);
    }
    return h;
  }

  // Equality comparison for use in `TestIndexConfig`. Two managers are equal
  // if they assign the same tags to the same prefixes and use equal schemes
  // (compared via their JSON representation).
  friend bool operator==(const EncodedIriManagerImpl& a,
                         const EncodedIriManagerImpl& b) {
    return a.slots_ == b.slots_ && a.schemesAsJson() == b.schemesAsJson();
  }
  friend bool operator!=(const EncodedIriManagerImpl& a,
                         const EncodedIriManagerImpl& b) {
    return !(a == b);
  }

  // Encode the `numberStr` (which may only consist of digits) into a 64-bit
  // number.
  static constexpr uint64_t encodeDecimalToNBit(std::string_view numberStr) {
    return qlever::encodedIri::encodeDecimalNibbles(numberStr, NumBitsEncoding);
  }

  // The inverse of `encodeDecimalToNBit`. The result is appended to the
  // `result` string.
  static void decodeDecimalFrom64Bit(std::string& result, uint64_t encoded) {
    qlever::encodedIri::decodeDecimalNibbles(result, encoded, NumBitsEncoding);
  }

  // Overload of `decodeDecimalFrom64Bit` that returns the result as a
  // `uint64_t`.
  static uint64_t decodeDecimalFrom64Bit(uint64_t encoded) {
    return qlever::encodedIri::decodeDecimalNibblesToNumber(encoded,
                                                            NumBitsEncoding);
  }

 private:
  // Restore a manager from its JSON representation (see `from_json`). In
  // contrast to the public constructor, the tags are not computed from the
  // order of the prefixes, but taken from the arguments: the schemes occupy
  // the tags that are stored in `schemes` (a pair of the first tag and the
  // scheme), and the `prefixesWithAngleBrackets` fill up the remaining tags in
  // their given order.
  EncodedIriManagerImpl(
      FromJson, std::vector<std::string> prefixesWithAngleBrackets,
      const std::vector<std::pair<size_t, qlever::EncodedIriSchemePtr>>&
          schemes) {
    if (prefixesWithAngleBrackets.empty() && schemes.empty()) {
      return;
    }
    size_t numTagsTotal = prefixesWithAngleBrackets.size();
    for (const auto& [firstTag, scheme] : schemes) {
      checkSchemeIsValid(scheme);
      numTagsTotal += scheme->numTags();
    }
    slots_.resize(numTagsTotal);
    std::vector<bool> tagIsUsed(numTagsTotal, false);
    for (const auto& [firstTag, scheme] : schemes) {
      auto schemeIdx = schemes_.size();
      schemes_.push_back(makeSchemeInfo(scheme));
      schemes_.back().firstTag_ = firstTag;
      auto prefix = smallestPrefixOfScheme(*scheme);
      for (size_t localTag = 0; localTag < scheme->numTags(); ++localTag) {
        size_t tag = firstTag + localTag;
        if (tag >= numTagsTotal || tagIsUsed.at(tag)) {
          throw std::runtime_error(absl::StrCat(
              "The stored tags of the encoding schemes for IRIs are "
              "inconsistent, the index seems to be corrupted (scheme \"",
              scheme->name(), "\", tag ", tag, ")"));
        }
        tagIsUsed.at(tag) = true;
        slots_.at(tag) = Slot{prefix, schemeIdx, localTag};
      }
    }
    auto prefixIt = prefixesWithAngleBrackets.begin();
    for (size_t tag = 0; tag < numTagsTotal; ++tag) {
      if (tagIsUsed.at(tag)) {
        continue;
      }
      AD_CORRECTNESS_CHECK(prefixIt != prefixesWithAngleBrackets.end());
      slots_.at(tag) = Slot{std::move(*prefixIt), noScheme, 0};
      ++prefixIt;
    }
    finishInitialization();
  }

  // Check that the `scheme` is not `nullptr` and that its parameters are
  // consistent with this manager. Throw otherwise.
  static void checkSchemeIsValid(const qlever::EncodedIriSchemePtr& scheme) {
    AD_CONTRACT_CHECK(scheme != nullptr,
                      "An encoding scheme for IRIs must not be `nullptr`");
    AD_CONTRACT_CHECK(!scheme->name().empty(),
                      "The name of an encoding scheme for IRIs must not be "
                      "empty");
    if (scheme->numTags() == 0) {
      throw std::runtime_error(absl::StrCat(
          "The encoding scheme for IRIs \"", scheme->name(),
          "\" has to reserve at least one tag, but reserves zero"));
    }
    if (scheme->numPayloadBits() > NumBitsEncoding) {
      throw std::runtime_error(absl::StrCat(
          "The encoding scheme for IRIs \"", scheme->name(), "\" requires ",
          scheme->numPayloadBits(), " bits for its payload, but at most ",
          NumBitsEncoding, " bits are available"));
    }
    if (scheme->prefixes().empty()) {
      throw std::runtime_error(
          absl::StrCat("The encoding scheme for IRIs \"", scheme->name(),
                       "\" has to specify at least one prefix, but specifies "
                       "none"));
    }
    for (const auto& prefix : scheme->prefixes()) {
      if (prefix.empty() || ql::starts_with(prefix, '<')) {
        throw std::runtime_error(absl::StrCat(
            "The prefixes of the encoding scheme for IRIs \"", scheme->name(),
            "\" must be nonempty and must not be enclosed in angle brackets; "
            "here is a violating prefix: \"",
            prefix, "\""));
      }
    }
  }

  // Create the `SchemeInfo` for the given `scheme` (with the `firstTag_` still
  // unset). The `scheme` must previously have been checked via
  // `checkSchemeIsValid`.
  static SchemeInfo makeSchemeInfo(const qlever::EncodedIriSchemePtr& scheme) {
    SchemeInfo info;
    info.scheme_ = scheme;
    info.numTags_ = scheme->numTags();
    info.numPayloadBits_ = scheme->numPayloadBits();
    info.payloadShift_ = NumBitsEncoding - info.numPayloadBits_;
    return info;
  }

  // The lexicographically smallest prefix of the `scheme`, with a leading `<`.
  // It determines the position of the tags of the scheme in the global order
  // of all tags.
  static std::string smallestPrefixOfScheme(
      const qlever::EncodedIriScheme& scheme) {
    auto prefixes = scheme.prefixes();
    AD_CORRECTNESS_CHECK(!prefixes.empty());
    return absl::StrCat("<", *ql::ranges::min_element(prefixes));
  }

  // Append the slots of the scheme with the index `schemeIdx` to `slots_` and
  // store the resulting first tag in the `SchemeInfo`.
  void addSlotsForScheme(size_t schemeIdx, const std::string& prefix) {
    auto& info = schemes_.at(schemeIdx);
    info.firstTag_ = slots_.size();
    for (size_t localTag = 0; localTag < info.numTags_; ++localTag) {
      slots_.push_back(Slot{prefix, schemeIdx, localTag});
    }
  }

  // Build the dispatch table from `slots_` and `schemes_`, and check that the
  // number of tags and the prefixes are valid. This is the last step of both
  // constructors.
  void finishInitialization() {
    if (slots_.size() > maxNumPrefixes_) {
      throw std::runtime_error(absl::StrCat(
          "The number of tags required by the prefixes specified with "
          "`--encode-as-id` and by the encoding schemes for IRIs is ",
          slots_.size(), ", which is too many; the maximum is ",
          maxNumPrefixes_));
    }
    // TODO<C++23> use `std::views::enumerate`.
    for (size_t tag = 0; tag < slots_.size(); ++tag) {
      const auto& slot = slots_.at(tag);
      if (!slot.isScheme()) {
        dispatch_.push_back(DispatchEntry{slot.prefix_, tag, noScheme});
        continue;
      }
      // A scheme is only added once, with all of its prefixes.
      if (slot.localTag_ != 0) {
        continue;
      }
      const auto& info = schemes_.at(slot.schemeIdx_);
      for (const auto& prefix : info.scheme_->prefixes()) {
        dispatch_.push_back(
            DispatchEntry{absl::StrCat("<", prefix), tag, slot.schemeIdx_});
      }
    }
    ql::ranges::sort(dispatch_,
                     [](const DispatchEntry& a, const DispatchEntry& b) {
                       return a.prefix_ < b.prefix_;
                     });
    // TODO<C++23> use `std::views::adjacent`.
    for (size_t i = 0; i + 1 < dispatch_.size(); ++i) {
      const auto& a = dispatch_.at(i).prefix_;
      const auto& b = dispatch_.at(i + 1).prefix_;
      if (ql::starts_with(b, a)) {
        throw std::runtime_error(absl::StrCat(
            "None of the prefixes specified with `--encode-as-id` or by an "
            "encoding scheme for IRIs "
            "may be a prefix of another; here is a violating pair: \"",
            a.substr(1), "\" and \"", b.substr(1), "\"."));
      }
    }
  }

  // The part of `encode` for a plain prefix: `rest` is the input with the
  // matching prefix already removed, and `tag` is the tag of that prefix.
  static std::optional<Id> encodeWithPlainPrefix(std::string_view rest,
                                                 size_t tag) {
    // Check that after the prefix, the string contains only digits and the
    // trailing '>'.
    auto numStringOpt = detail::matchDigitsPrefix(rest);
    if (!numStringOpt.has_value()) {
      return std::nullopt;
    }
    std::string_view numString = numStringOpt.value();
    if (numString.size() > NumDigits) {
      return std::nullopt;
    }
    return makeIdFromPrefixIdxAndPayload(tag, encodeDecimalToNBit(numString));
  }

  // The part of `encode` for the scheme with the index `schemeIdx`.
  std::optional<Id> encodeWithScheme(std::string_view repr,
                                     size_t schemeIdx) const {
    const auto& info = schemes_.at(schemeIdx);
    auto tagAndPayload = info.scheme_->encode(repr);
    if (!tagAndPayload.has_value()) {
      return std::nullopt;
    }
    auto [localTag, payload] = tagAndPayload.value();
    AD_CORRECTNESS_CHECK(localTag < info.numTags_,
                         "An encoding scheme for IRIs returned a tag that is "
                         "larger than the number of tags it reserved");
    AD_CORRECTNESS_CHECK(
        payload <= ad_utility::bitMaskForLowerBits(info.numPayloadBits_),
        "An encoding scheme for IRIs returned a payload that doesn't fit into "
        "the number of bits it reserved");
    return makeIdFromPrefixIdxAndPayload(info.firstTag_ + localTag,
                                         payload << info.payloadShift_);
  }

  // The JSON representations of all the schemes, together with their first
  // tag. Used for equality comparison.
  std::vector<std::pair<size_t, std::string>> schemesAsJson() const {
    std::vector<std::pair<size_t, std::string>> result;
    for (const auto& info : schemes_) {
      result.emplace_back(info.firstTag_,
                          info.scheme_->toJsonWithName().dump());
    }
    ql::ranges::sort(result);
    return result;
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

#endif  // QLEVER_SRC_INDEX_ENCODEDVALUES_H
