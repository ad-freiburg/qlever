// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_ENCODEDIRIBITCONSTRAINT_H
#define QLEVER_SRC_INDEX_ENCODEDIRIBITCONSTRAINT_H

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "backports/three_way_comparison.h"
#include "util/BitUtils.h"
#include "util/json.h"

namespace encodedIris {
// A constraint on the number that follows a prefix for the encoding of IRIs
// (see `EncodedIriManager.h`): the bits in the half-open range `[start_,
// end_)`, counted from the least significant bit, must have exactly the value
// `value_`. In other words, a number `v` fulfills the constraint if and only if
// `((v >> start_) & bitMaskForLowerBits(end_ - start_)) == value_`.
//
// Constraints make it possible to encode numbers that are too large to be
// encoded otherwise: the constrained bits carry no information, so they are not
// stored in the `Id` at all, but removed when encoding and re-inserted when
// decoding. For example, if the numbers to encode are known to have their bits
// `[16, 32)` all zero, then 16 bits fewer have to be stored, and numbers up to
// 2^64 become encodable although fewer than 64 bits are available.
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
  // The number of bits of the number that a constraint applies to.
  static constexpr size_t numBitsTotal = 64;

  // The first bit of the constrained range, counted from the least significant
  // bit.
  uint8_t start_ = 0;
  // The bit after the last bit of the constrained range, so the range is
  // `[start_, end_)`.
  uint8_t end_ = 0;
  // The required value of the constrained bits, in the range
  // `[0, 2 ** (end_ - start_))`.
  uint64_t value_ = 0;

  BitRangeConstraint() = default;

  // Construct from the bit range `[start, end)` and the required `value`, and
  // check that they are consistent (see the `AD_CONTRACT_CHECK`s in the
  // definition).
  BitRangeConstraint(size_t start, size_t end, uint64_t value);

  // The number of bits that this constraint covers.
  size_t size() const { return static_cast<size_t>(end_) - start_; }

  // Return true if `value` fulfills this constraint.
  bool matches(uint64_t value) const;

  // Bring `constraints` into the form that all the functions below require
  // (sorted by `start_`, non-overlapping) and check that they leave few enough
  // bits for a number to be encodable in `numBitsAvailable` bits at all. Throw
  // if they do not. The `prefix` is only used for the error messages.
  static void normalizeAndValidate(std::vector<BitRangeConstraint>& constraints,
                                   size_t numBitsAvailable,
                                   std::string_view prefix);

  // Encode `numString` (a nonempty sequence of decimal digits) into at most
  // `numBitsAvailable` bits under the given nonempty `constraints`, or return
  // `std::nullopt` if it cannot be encoded. The latter happens if the number
  // does not fit into 64 bits, if it has leading zeros, if it violates one of
  // the constraints, or if the remaining bits do not fit into
  // `numBitsAvailable`.
  //
  // PRECONDITION: `constraints` is nonempty and normalized, see
  // `normalizeAndValidate`.
  static std::optional<uint64_t> encode(
      std::string_view numString,
      const std::vector<BitRangeConstraint>& constraints,
      size_t numBitsAvailable);

  // Remove all bits that are covered by one of the `constraints` from `value`,
  // moving the remaining bits down so that no gaps remain. The order of the
  // remaining bits is preserved.
  //
  // PRECONDITION: `constraints` is normalized, see `normalizeAndValidate`.
  static uint64_t removeConstrainedBits(
      uint64_t value, const std::vector<BitRangeConstraint>& constraints);

  // The inverse of `removeConstrainedBits`: move the bits of `payload` back to
  // their original positions and re-insert the values of the `constraints`.
  //
  // PRECONDITION: `constraints` is normalized, see `normalizeAndValidate`.
  static uint64_t reinsertConstrainedBits(
      uint64_t payload, const std::vector<BitRangeConstraint>& constraints);

  friend void to_json(nlohmann::json& j, const BitRangeConstraint& c) {
    j = nlohmann::json{
        {"start", c.start_}, {"end", c.end_}, {"value", c.value_}};
  }
  friend void from_json(const nlohmann::json& j, BitRangeConstraint& c) {
    c = BitRangeConstraint{j.at("start").get<size_t>(),
                           j.at("end").get<size_t>(),
                           j.at("value").get<uint64_t>()};
  }
  template <typename H>
  friend H AbslHashValue(H h, const BitRangeConstraint& c) {
    return H::combine(std::move(h), c.start_, c.end_, c.value_);
  }
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(BitRangeConstraint, start_, end_,
                                              value_)
};

// A prefix for the encoding of IRIs, together with the (possibly empty) list of
// constraints on the number that follows it. With an empty list of constraints
// the prefix behaves exactly as a prefix that is specified as a plain string:
// the digits that follow the prefix are stored one nibble per digit (see
// `EncodedIriNibbleEncoding.h`), which preserves the lexicographic order of the
// IRIs and their leading zeros, but limits the number to the few digits that
// fit into the payload of an `Id`.
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

#endif  // QLEVER_SRC_INDEX_ENCODEDIRIBITCONSTRAINT_H
