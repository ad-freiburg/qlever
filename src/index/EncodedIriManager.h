// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_ENCODEDVALUES_H
#define QLEVER_SRC_INDEX_ENCODEDVALUES_H

#include <absl/strings/str_cat.h>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "backports/three_way_comparison.h"
#include "global/Id.h"
#include "index/EncodedIriBitConstraint.h"
#include "index/EncodedIriNibbleEncoding.h"
#include "util/BitUtils.h"
#include "util/Log.h"
#include "util/Views.h"
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
// original IRIs. NOTE: This holds for prefixes without constraints. A prefix
// with constraints (see `encodedIris::BitRangeConstraint`) stores the number
// itself, so its encoded values are ordered numerically, which is the lexical
// order only among numbers of equal length. Each decimal digit is encoded as a
// 4-bit nibble, where digit `i` is encoded as `i+1` and converted to a
// hexadecimal number. The nibbles are stored left-aligned (not right-aligned)
// and filled on the right with zeroes.
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

  using BitRangeConstraint = encodedIris::BitRangeConstraint;
  using PrefixWithConstraints = encodedIris::PrefixWithConstraints;

  // The encoding of the digits for prefixes without constraints, see
  // `EncodedIriNibbleEncoding.h`.
  using NibbleEncoder = encodedIris::NibbleEncoder<NumBitsEncoding>;
  static constexpr size_t NibbleSize = NibbleEncoder::NibbleSize;
  static constexpr size_t NumDigits = NibbleEncoder::NumDigits;

  static_assert(NumBitsTotal <= 64);
  static_assert(NumBitsTags <= 64);

 private:
  // Wrap plain prefixes into `PrefixWithConstraints` without constraints.
  static std::vector<PrefixWithConstraints> toPrefixesWithoutConstraints(
      std::vector<std::string> prefixesWithoutAngleBrackets) {
    return ::ranges::to_vector(
        ad_utility::RvalueView{prefixesWithoutAngleBrackets} |
        ql::views::transform([](std::string&& prefix) {
          return PrefixWithConstraints{std::move(prefix)};
        }));
  }

 public:
  // The prefixes of the IRIs that will be encoded.
  std::vector<std::string> prefixes_;

  // The constraints for each of the `prefixes_`, in the same order and with the
  // same size. An empty entry means that the corresponding prefix has no
  // constraints, so almost all entries are typically empty. See
  // `encodedIris::BitRangeConstraint`.
  //
  // NOTE: This is a separate vector (and not a member of a combined struct), so
  // that the JSON representation of `prefixes_`, which is part of the index
  // metadata of every existing index, stays exactly as it was.
  std::vector<std::vector<BitRangeConstraint>> constraintsPerPrefix_;

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
      std::vector<std::string> prefixesWithoutAngleBrackets)
      : EncodedIriManagerImpl{ConstraintsTag{},
                              toPrefixesWithoutConstraints(
                                  std::move(prefixesWithoutAngleBrackets))} {}

  // Construct from a list of prefixes with constraints (see
  // `encodedIris::PrefixWithConstraints`). The prefixes have to be specified
  // without any brackets, exactly as for the constructor above.
  //
  // NOTE: This is a named factory and not a constructor, because a constructor
  // taking a `std::vector<PrefixWithConstraints>` would be ambiguous with the
  // one above for a braced initializer such as `{{"a", "b"}}`.
  static EncodedIriManagerImpl fromPrefixesWithConstraints(
      std::vector<PrefixWithConstraints> prefixesWithConstraints) {
    return EncodedIriManagerImpl{ConstraintsTag{},
                                 std::move(prefixesWithConstraints)};
  }

 private:
  // Tag to distinguish the constructor below from the public one above.
  struct ConstraintsTag {};

  EncodedIriManagerImpl(
      ConstraintsTag,
      std::vector<PrefixWithConstraints> prefixesWithConstraints) {
    // Add hardcoded prefixes. They never have constraints.
    for (const auto& prefix : HardcodedPrefixes) {
      // Adding a hardcoded prefix a second time in the constructor is an error.
      AD_CONTRACT_CHECK(
          ql::ranges::find(prefixesWithConstraints, prefix,
                           &PrefixWithConstraints::prefix_) ==
              prefixesWithConstraints.end(),
          "The prefix \"", prefix,
          "\" for the encoding of IRIs is always added automatically and must "
          "not be specified explicitly");
      prefixesWithConstraints.emplace_back(std::string{prefix});
    }
    if (prefixesWithConstraints.empty()) {
      return;
    }
    // Sort the prefixes lexicographically to make the ordering deterministic
    // (provided that the prefixes do not end with digits). The constraints are
    // moved along with their prefix.
    ql::ranges::sort(prefixesWithConstraints, std::less<>{},
                     &PrefixWithConstraints::prefix_);

    // Validate the constraints of each prefix, and normalize them into the form
    // that the (de)compression requires, see
    // `BitRangeConstraint::normalizeAndValidate`.
    for (auto& [prefix, constraints] : prefixesWithConstraints) {
      BitRangeConstraint::normalizeAndValidate(constraints, NumBitsEncoding,
                                               prefix);
    }

    // Remove duplicates. A prefix that is specified more than once with
    // different constraints is an error, because there would be no way to
    // decide which of them to apply.
    for (const auto& [a, b] :
         ad_utility::pairwiseView(prefixesWithConstraints)) {
      if (a.prefix_ == b.prefix_ && a.constraints_ != b.constraints_) {
        throw std::runtime_error(absl::StrCat(
            "The prefix \"", a.prefix_,
            "\" for the encoding of IRIs was specified more than once, but "
            "with different constraints"));
      }
    }
    // NOTE: `ql::ranges::unique` does not work because of a discrepancy in the
    // return types between `std::ranges` and `range-v3`.
    prefixesWithConstraints.erase(
        ::ranges::unique(prefixesWithConstraints, std::equal_to<>{},
                         &PrefixWithConstraints::prefix_),
        prefixesWithConstraints.end());

    // Split the prefixes and their constraints into the two parallel vectors
    // that this class stores. Both transformations move out of
    // `prefixesWithConstraints`, but each of them only touches one of the two
    // members, so they do not interfere.
    auto prefixesWithoutAngleBrackets = ::ranges::to_vector(
        prefixesWithConstraints |
        ql::views::transform([](PrefixWithConstraints& p) -> std::string&& {
          return std::move(p.prefix_);
        }));
    auto constraints = ::ranges::to_vector(
        prefixesWithConstraints |
        ql::views::transform(
            [](PrefixWithConstraints& p) -> std::vector<BitRangeConstraint>&& {
              return std::move(p.constraints_);
            }));

    if (prefixesWithoutAngleBrackets.size() > maxNumPrefixes_) {
      throw std::runtime_error(absl::StrCat(
          "Number of prefixes specified with `--encode-as-id` is ",
          prefixesWithoutAngleBrackets.size(), ", which is too many; ",
          "the maximum is ", maxNumPrefixes_));
    }

    for (const auto& [a, b] :
         ad_utility::pairwiseView(prefixesWithoutAngleBrackets)) {
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
      prefixes_.push_back(absl::StrCat("<", prefix));
    }
    constraintsPerPrefix_ = std::move(constraints);
    AD_CORRECTNESS_CHECK(constraintsPerPrefix_.size() == prefixes_.size());
  }

 public:
  // Try to encode the given string as an `Id`. If the encoding fails, return
  // `std::nullopt`. This happens in one of the following cases:
  //
  // 1. The string is not an `<iriref-in-angle-brackets>`
  // 2. The string does not start with any of the `prefixes_`
  // 3. After the matching prefix, there are characters other than `[0-9]`
  // 4. There are more digits than fit into `NumBitsEncoding` (4 bits / digit)
  //
  // For a prefix with constraints (see `encodedIris::BitRangeConstraint`), 4.
  // is replaced by the following cases: the number does not fit into 64 bits,
  // it has leading zeros, it violates one of the constraints, or the bits that
  // remain after removing the constrained ones do not fit into
  // `NumBitsEncoding`.
  std::optional<Id> encode(std::string_view repr) const {
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
    auto prefixIndex = static_cast<size_t>(it - prefixes_.begin());
    const auto& constraints = constraintsPerPrefix_.at(prefixIndex);

    // Prefixes with constraints use the constrained encoding, see
    // `BitRangeConstraint::encode`. Note that they have a completely different
    // payload layout, so the digit-based limit does not apply to them.
    if (!constraints.empty()) {
      auto payload =
          BitRangeConstraint::encode(numString, constraints, NumBitsEncoding);
      if (!payload.has_value()) {
        return std::nullopt;
      }
      return makeIdFromPrefixIdxAndPayload(prefixIndex, payload.value());
    }

    if (numString.size() > NumDigits) {
      return std::nullopt;
    }

    // Run the actual encoding.
    return makeIdFromPrefixIdxAndPayload(prefixIndex,
                                         NibbleEncoder::encode(numString));
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
    const auto& constraints = constraintsPerPrefix_.at(prefixIdx);
    if (!constraints.empty()) {
      return absl::StrCat(prefixes_.at(prefixIdx),
                          BitRangeConstraint::reinsertConstrainedBits(
                              digitEncoding, constraints),
                          ">");
    }
    return NibbleEncoder::toStringWithGivenPrefix(digitEncoding,
                                                  prefixes_.at(prefixIdx));
  }

  // Return the number that is encoded in `id`, provided that the prefix of `id`
  // has constraints. This is the exact inverse of the number that was parsed
  // from the IRI, which makes it possible to recover a number that does not fit
  // into `NumBitsEncoding` bits.
  //
  // PRECONDITION: `id` was encoded by this manager, using a prefix that has
  // constraints.
  uint64_t getNumberOfConstrainedId(Id id) const {
    AD_CORRECTNESS_CHECK(id.getDatatype() == Datatype::EncodedVal);
    auto [prefixIdx, payload] = splitIntoPrefixIdxAndPayload(id);
    const auto& constraints = constraintsPerPrefix_.at(prefixIdx);
    AD_CONTRACT_CHECK(!constraints.empty(),
                      "`getNumberOfConstrainedId` was called for an `Id` whose "
                      "prefix has no constraints");
    return BitRangeConstraint::reinsertConstrainedBits(payload, constraints);
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
    return {prefix, NibbleEncoder::decode(payload)};
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
  // The JSON key for the constraints. It is separate from `jsonKey_` and
  // optional, so that indexes that were built before the constraints existed
  // (and indexes without any constraints) can still be read, see `from_json`.
  static constexpr const char* constraintsJsonKey_ = "prefix-bit-constraints";
  friend void to_json(nlohmann::json& j,
                      const EncodedIriManagerImpl& encodedIriManager) {
    j[jsonKey_] = encodedIriManager.prefixes_;
    // Only write the constraints if there are any, so that the metadata of an
    // index without constraints is unchanged.
    if (ql::ranges::any_of(encodedIriManager.constraintsPerPrefix_,
                           [](const auto& c) { return !c.empty(); })) {
      j[constraintsJsonKey_] = encodedIriManager.constraintsPerPrefix_;
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
    encodedIriManager.prefixes_ =
        static_cast<std::vector<std::string>>(j[jsonKey_]);
    // The constraints are optional; an index that was built without them (or
    // without any constrained prefix) simply has no constrained prefixes.
    if (j.contains(constraintsJsonKey_)) {
      encodedIriManager.constraintsPerPrefix_ =
          j[constraintsJsonKey_]
              .template get<std::vector<std::vector<BitRangeConstraint>>>();
      if (encodedIriManager.constraintsPerPrefix_.size() !=
          encodedIriManager.prefixes_.size()) {
        throw std::runtime_error(absl::StrCat(
            "The index metadata specifies ", encodedIriManager.prefixes_.size(),
            " prefixes for the encoding of IRIs, but ",
            encodedIriManager.constraintsPerPrefix_.size(),
            " lists of constraints for them; the index is corrupted"));
      }
      // Validate and normalize, so that a broken set of constraints is reported
      // here and not later from deep inside the parser or the exporter.
      for (size_t i = 0; i < encodedIriManager.prefixes_.size(); ++i) {
        BitRangeConstraint::normalizeAndValidate(
            encodedIriManager.constraintsPerPrefix_.at(i), NumBitsEncoding,
            encodedIriManager.prefixes_.at(i));
      }
    } else {
      encodedIriManager.constraintsPerPrefix_.assign(
          encodedIriManager.prefixes_.size(), {});
    }
  }

  // Hash support for use in `TestIndexConfig`.
  template <typename H>
  friend H AbslHashValue(H h, const EncodedIriManagerImpl& manager) {
    return H::combine(std::move(h), manager.prefixes_,
                      manager.constraintsPerPrefix_);
  }

  // Equality operator for use in `TestIndexConfig`.
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(EncodedIriManagerImpl, prefixes_,
                                              constraintsPerPrefix_)
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
