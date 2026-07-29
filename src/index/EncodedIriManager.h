// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_ENCODEDVALUES_H
#define QLEVER_SRC_INDEX_ENCODEDVALUES_H

#include <absl/numeric/bits.h>
#include <absl/strings/numbers.h>

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

namespace encodedIris {
// A constraint on the number that follows a prefix: the bits in the half-open
// range `[bitStart_, bitEnd_)`, counted from the least significant bit, must
// have exactly the value `value_`. In other words, a number `v` fulfills the
// constraint if and only if
// `((v >> bitStart_) & bitMaskForLowerBits(bitEnd_ - bitStart_)) == value_`.
//
// Constraints make it possible to encode numbers that are too large to be
// encoded otherwise: the constrained bits carry no information, so they are not
// stored in the `Id` at all, but removed when encoding and re-inserted when
// decoding. For example, if the numbers to encode are known to have their bits
// `[16, 32)` all zero, then 16 bits fewer have to be stored, and numbers up to
// 2^64 become encodable although only `NumBitsEncoding` bits are available.
//
// NOTE: A number that violates any of the constraints is simply not encoded
// (the IRI is then stored in the vocabulary as usual), so constraints never
// lose information. It is therefore safe, but pointless, to specify a
// constraint that no number fulfills. For the same reason, a number with
// leading zeros is not encoded: only the number is stored, not the digits, so
// `007` and `7` would otherwise become the same `Id`.
//
// NOTE: Constrained prefixes store the number in binary, so their encoded
// values are ordered numerically rather than lexicographically. If the
// constraints pin the highest bits (which is what makes them useful), all
// encodable numbers have the same number of digits, and the two orders
// coincide.
struct BitRangeConstraint {
  size_t bitStart_ = 0;
  size_t bitEnd_ = 0;
  uint64_t value_ = 0;

  BitRangeConstraint() = default;

  BitRangeConstraint(size_t bitStart, size_t bitEnd, uint64_t value)
      : bitStart_{bitStart}, bitEnd_{bitEnd}, value_{value} {
    AD_CONTRACT_CHECK(bitStart < bitEnd,
                      "The bit range of a constraint for an encoded IRI must "
                      "be nonempty, but got the empty range starting at ",
                      bitStart);
    AD_CONTRACT_CHECK(bitEnd <= 64,
                      "The bit range of a constraint for an encoded IRI must "
                      "lie within the 64 bits of the encoded number, but got "
                      "the end ",
                      bitEnd);
    // A constraint on all 64 bits would leave no bits to encode at all, and
    // would make the encoded number a constant.
    AD_CONTRACT_CHECK(size() < 64,
                      "A constraint for an encoded IRI must not constrain all "
                      "64 bits of the encoded number");
    AD_CONTRACT_CHECK(
        value <= ad_utility::bitMaskForLowerBits(size()),
        "The value of a constraint for an encoded IRI must fit into its bit "
        "range, but the value ",
        value, " does not fit into ", size(), " bits");
  }

  // The number of bits that this constraint covers.
  size_t size() const { return bitEnd_ - bitStart_; }

  // Return true if `value` fulfills this constraint.
  bool matches(uint64_t value) const {
    return ((value >> bitStart_) & ad_utility::bitMaskForLowerBits(size())) ==
           value_;
  }

  friend void to_json(nlohmann::json& j, const BitRangeConstraint& c) {
    j = nlohmann::json{{"bit-start", c.bitStart_},
                       {"bit-end", c.bitEnd_},
                       {"value", c.value_}};
  }
  friend void from_json(const nlohmann::json& j, BitRangeConstraint& c) {
    c = BitRangeConstraint{j.at("bit-start").get<size_t>(),
                           j.at("bit-end").get<size_t>(),
                           j.at("value").get<uint64_t>()};
  }
  template <typename H>
  friend H AbslHashValue(H h, const BitRangeConstraint& c) {
    return H::combine(std::move(h), c.bitStart_, c.bitEnd_, c.value_);
  }
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(BitRangeConstraint, bitStart_,
                                              bitEnd_, value_)
};

// A prefix for the encoding of IRIs, together with the (possibly empty) list of
// constraints on the number that follows it. With an empty list of constraints
// the prefix behaves exactly as a prefix specified as a plain string.
struct PrefixWithConstraints {
  std::string prefix_;
  std::vector<BitRangeConstraint> constraints_;

  // Create a prefix without constraints.
  explicit PrefixWithConstraints(std::string prefix)
      : prefix_{std::move(prefix)} {}

  PrefixWithConstraints(std::string prefix,
                        std::vector<BitRangeConstraint> constraints)
      : prefix_{std::move(prefix)}, constraints_{std::move(constraints)} {}
};
}  // namespace encodedIris

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
  static_assert(NumBitsTags <= 64);
  static_assert(NumDigits > 0);

  using BitRangeConstraint = encodedIris::BitRangeConstraint;
  using PrefixWithConstraints = encodedIris::PrefixWithConstraints;

 private:
  // Projection onto the prefix, for the sorting and the deduplication in the
  // constructor.
  static constexpr auto getPrefix =
      [](const PrefixWithConstraints& p) -> const std::string& {
    return p.prefix_;
  };

  // Wrap plain prefixes into `PrefixWithConstraints` without constraints.
  static std::vector<PrefixWithConstraints> toPrefixesWithoutConstraints(
      std::vector<std::string> prefixesWithoutAngleBrackets) {
    std::vector<PrefixWithConstraints> result;
    result.reserve(prefixesWithoutAngleBrackets.size());
    for (auto& prefix : prefixesWithoutAngleBrackets) {
      result.emplace_back(std::move(prefix));
    }
    return result;
  }

  // Bring the constraints of `prefix` into the form that
  // `removeConstrainedBits` and `reinsertConstrainedBits` require (sorted by
  // `bitStart_`, non-overlapping) and check that they leave few enough bits to
  // be encodable at all. Throw if they do not.
  static void normalizeAndValidateConstraints(PrefixWithConstraints& prefix) {
    auto& constraints = prefix.constraints_;
    if (constraints.empty()) {
      return;
    }
    ql::ranges::sort(constraints, std::less<>{},
                     [](const BitRangeConstraint& c) { return c.bitStart_; });
    size_t numConstrainedBits = 0;
    for (size_t i = 0; i < constraints.size(); ++i) {
      if (i > 0 &&
          constraints.at(i - 1).bitEnd_ > constraints.at(i).bitStart_) {
        throw std::runtime_error(absl::StrCat(
            "The bit ranges of the constraints for the encoded IRIs with the "
            "prefix \"",
            prefix.prefix_, "\" must not overlap, but the ranges [",
            constraints.at(i - 1).bitStart_, ", ",
            constraints.at(i - 1).bitEnd_, ") and [",
            constraints.at(i).bitStart_, ", ", constraints.at(i).bitEnd_,
            ") do"));
      }
      numConstrainedBits += constraints.at(i).size();
    }
    // The bits that are not constrained have to fit into the payload, else no
    // number could ever be encoded with this prefix.
    size_t numRemainingBits = 64 - numConstrainedBits;
    if (numRemainingBits > NumBitsEncoding) {
      throw std::runtime_error(absl::StrCat(
          "The constraints for the encoded IRIs with the prefix \"",
          prefix.prefix_, "\" constrain ", numConstrainedBits,
          " of the 64 bits of the encoded number, so ", numRemainingBits,
          " bits remain, but only ", NumBitsEncoding,
          " bits are available; constrain at least ", 64 - NumBitsEncoding,
          " bits"));
    }
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
          !ad_utility::contains(
              prefixesWithConstraints | ql::views::transform(getPrefix),
              prefix),
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
    ql::ranges::sort(prefixesWithConstraints, std::less<>{}, getPrefix);

    // Validate the constraints of each prefix, and normalize them into the form
    // that the (de)compression below requires: sorted by `bitStart_` and
    // non-overlapping.
    for (auto& prefixWithConstraints : prefixesWithConstraints) {
      normalizeAndValidateConstraints(prefixWithConstraints);
    }

    // Remove duplicates. A prefix that is specified more than once with
    // different constraints is an error, because there would be no way to
    // decide which of them to apply.
    //
    // NOTE: `ql::ranges::unique` does not work because of a discrepancy in the
    // return types between `std::ranges` and `range-v3`.
    for (size_t i = 1; i < prefixesWithConstraints.size(); ++i) {
      const auto& a = prefixesWithConstraints.at(i - 1);
      const auto& b = prefixesWithConstraints.at(i);
      if (a.prefix_ == b.prefix_ && a.constraints_ != b.constraints_) {
        throw std::runtime_error(absl::StrCat(
            "The prefix \"", a.prefix_,
            "\" for the encoding of IRIs was specified more than once, but "
            "with different constraints"));
      }
    }
    prefixesWithConstraints.erase(
        ::ranges::unique(prefixesWithConstraints, std::equal_to<>{}, getPrefix),
        prefixesWithConstraints.end());

    std::vector<std::string> prefixesWithoutAngleBrackets;
    std::vector<std::vector<BitRangeConstraint>> constraints;
    prefixesWithoutAngleBrackets.reserve(prefixesWithConstraints.size());
    constraints.reserve(prefixesWithConstraints.size());
    for (auto& prefixWithConstraints : prefixesWithConstraints) {
      prefixesWithoutAngleBrackets.push_back(
          std::move(prefixWithConstraints.prefix_));
      constraints.push_back(std::move(prefixWithConstraints.constraints_));
    }

    if (prefixesWithoutAngleBrackets.size() > maxNumPrefixes_) {
      throw std::runtime_error(absl::StrCat(
          "Number of prefixes specified with `--encode-as-id` is ",
          prefixesWithoutAngleBrackets.size(), ", which is too many; ",
          "the maximum is ", maxNumPrefixes_));
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
    // `encodeWithConstraints`. Note that they have a completely different
    // payload layout, so the digit-based limit does not apply to them.
    if (!constraints.empty()) {
      auto payload = encodeWithConstraints(numString, constraints);
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
                                         encodeDecimalToNBit(numString));
  }

  // Encode `numString` (a nonempty sequence of decimal digits) under the given
  // nonempty, normalized `constraints`, or return `std::nullopt` if it cannot
  // be encoded. The latter happens if the number does not fit into 64 bits, if
  // it violates one of the constraints, or if the remaining bits do not fit
  // into `NumBitsEncoding`.
  static std::optional<uint64_t> encodeWithConstraints(
      std::string_view numString,
      const std::vector<BitRangeConstraint>& constraints) {
    // Reject leading zeros. The constrained encoding stores the number, not the
    // digits, so `007` and `7` would otherwise become the same `Id` although
    // they are different IRIs. (The plain encoding below has no such problem,
    // because it stores one nibble per digit.)
    if (numString.size() > 1 && numString.front() == '0') {
      return std::nullopt;
    }
    uint64_t value = 0;
    if (!absl::SimpleAtoi(numString, &value)) {
      // The number does not fit into 64 bits.
      return std::nullopt;
    }
    if (!ql::ranges::all_of(constraints, [value](const auto& constraint) {
          return constraint.matches(value);
        })) {
      return std::nullopt;
    }
    uint64_t payload = removeConstrainedBits(value, constraints);
    if (payload > ad_utility::bitMaskForLowerBits(NumBitsEncoding)) {
      return std::nullopt;
    }
    return payload;
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
                          reinsertConstrainedBits(digitEncoding, constraints),
                          ">");
    }
    return toStringWithGivenPrefix(digitEncoding, prefixes_.at(prefixIdx));
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
    return reinsertConstrainedBits(payload, constraints);
  }

  // Remove all bits that are covered by one of the `constraints` from `value`,
  // moving the remaining bits down so that no gaps remain. The order of the
  // remaining bits is preserved.
  //
  // PRECONDITION: `constraints` is sorted by `bitStart_` and non-overlapping.
  static uint64_t removeConstrainedBits(
      uint64_t value, const std::vector<BitRangeConstraint>& constraints) {
    uint64_t result = 0;
    size_t outputBitPos = 0;
    size_t inputBitPos = 0;
    for (const auto& constraint : constraints) {
      size_t numBitsToCopy = constraint.bitStart_ - inputBitPos;
      if (numBitsToCopy > 0) {
        uint64_t bits = (value >> inputBitPos) &
                        ad_utility::bitMaskForLowerBits(numBitsToCopy);
        result |= bits << outputBitPos;
        outputBitPos += numBitsToCopy;
      }
      inputBitPos = constraint.bitEnd_;
    }
    // Copy the bits above the last constraint. Note that `inputBitPos == 64` is
    // possible (a constraint that ends at the most significant bit), in which
    // case there is nothing left to copy and shifting by 64 would be undefined.
    if (inputBitPos < 64) {
      result |= (value >> inputBitPos) << outputBitPos;
    }
    return result;
  }

  // The inverse of `removeConstrainedBits`: move the bits of `payload` back to
  // their original positions and re-insert the values of the `constraints`.
  //
  // PRECONDITION: `constraints` is sorted by `bitStart_` and non-overlapping.
  static uint64_t reinsertConstrainedBits(
      uint64_t payload, const std::vector<BitRangeConstraint>& constraints) {
    uint64_t result = 0;
    size_t inputBitPos = 0;
    size_t outputBitPos = 0;
    for (const auto& constraint : constraints) {
      size_t numBitsToCopy = constraint.bitStart_ - outputBitPos;
      if (numBitsToCopy > 0) {
        uint64_t bits = (payload >> inputBitPos) &
                        ad_utility::bitMaskForLowerBits(numBitsToCopy);
        result |= bits << outputBitPos;
        inputBitPos += numBitsToCopy;
      }
      result |= constraint.value_ << constraint.bitStart_;
      outputBitPos = constraint.bitEnd_;
    }
    // See the note in `removeConstrainedBits`; here both positions can be 64.
    if (outputBitPos < 64 && inputBitPos < 64) {
      result |= (payload >> inputBitPos) << outputBitPos;
    }
    return result;
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
        PrefixWithConstraints prefix{
            encodedIriManager.prefixes_.at(i),
            std::move(encodedIriManager.constraintsPerPrefix_.at(i))};
        normalizeAndValidateConstraints(prefix);
        encodedIriManager.constraintsPerPrefix_.at(i) =
            std::move(prefix.constraints_);
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
    // NOTE: `encoded == 0` gives `numTrailingZeroNibbles > NumDigits`, so the
    // length has to be clamped, else it would underflow. A payload of `0` never
    // comes from the digit encoding (which always sets the highest nibble), but
    // a prefix with constraints can produce it, and the payload of a corrupted
    // index can be anything.
    size_t len = NumDigits - std::min(numTrailingZeroNibbles, NumDigits);
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
