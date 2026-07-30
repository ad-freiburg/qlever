// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/EncodedIriBitConstraint.h"

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include <stdexcept>

#include "backports/algorithm.h"
#include "util/Views.h"

namespace encodedIris {

// ____________________________________________________________________________
BitRangeConstraint::BitRangeConstraint(size_t start, size_t end, uint64_t value)
    : start_{static_cast<uint8_t>(start)},
      end_{static_cast<uint8_t>(end)},
      value_{value} {
  AD_CONTRACT_CHECK(start < end,
                    "The bit range of a constraint for an encoded IRI must "
                    "be nonempty, but got the empty range starting at ",
                    start);
  AD_CONTRACT_CHECK(end <= numBitsTotal,
                    "The bit range of a constraint for an encoded IRI must "
                    "lie within the ",
                    numBitsTotal,
                    " bits of the encoded number, but got the end ", end);
  // A constraint on all bits would leave no bits to encode at all, and would
  // make the encoded number a constant.
  AD_CONTRACT_CHECK(size() < numBitsTotal,
                    "A constraint for an encoded IRI must not constrain all ",
                    numBitsTotal, " bits of the encoded number");
  AD_CONTRACT_CHECK(
      value <= ad_utility::bitMaskForLowerBits(size()),
      "The value of a constraint for an encoded IRI must fit into its bit "
      "range, but the value ",
      value, " does not fit into ", size(), " bits");
}

// ____________________________________________________________________________
bool BitRangeConstraint::matches(uint64_t value) const {
  return ((value >> start_) & ad_utility::bitMaskForLowerBits(size())) ==
         value_;
}

// ____________________________________________________________________________
void BitRangeConstraint::normalizeAndValidate(
    std::vector<BitRangeConstraint>& constraints, size_t numBitsAvailable,
    std::string_view prefix) {
  if (constraints.empty()) {
    return;
  }
  ql::ranges::sort(constraints, std::less<>{}, &BitRangeConstraint::start_);
  for (const auto& [a, b] : ad_utility::pairwiseView(constraints)) {
    if (a.end_ > b.start_) {
      // NOTE: The bit indices are cast to `size_t`, else they would be
      // formatted as characters and not as numbers.
      throw std::runtime_error(absl::StrCat(
          "The bit ranges of the constraints for the encoded IRIs with the "
          "prefix \"",
          prefix, "\" must not overlap, but the ranges [", size_t{a.start_},
          ", ", size_t{a.end_}, ") and [", size_t{b.start_}, ", ",
          size_t{b.end_}, ") do"));
    }
  }
  size_t numConstrainedBits = 0;
  for (const auto& constraint : constraints) {
    numConstrainedBits += constraint.size();
  }
  // The bits that are not constrained have to fit into the payload, else no
  // number could ever be encoded with this prefix.
  size_t numRemainingBits = numBitsTotal - numConstrainedBits;
  if (numRemainingBits > numBitsAvailable) {
    throw std::runtime_error(absl::StrCat(
        "The constraints for the encoded IRIs with the prefix \"", prefix,
        "\" constrain ", numConstrainedBits, " of the ", numBitsTotal,
        " bits of the encoded number, so ", numRemainingBits,
        " bits remain, but only ", numBitsAvailable,
        " bits are available; constrain at least ",
        numBitsTotal - numBitsAvailable, " bits"));
  }
}

// ____________________________________________________________________________
std::optional<uint64_t> BitRangeConstraint::encode(
    std::string_view numString,
    const std::vector<BitRangeConstraint>& constraints,
    size_t numBitsAvailable) {
  AD_CORRECTNESS_CHECK(!constraints.empty());
  // Reject leading zeros. The constrained encoding stores the number, not the
  // digits, so `007` and `7` would otherwise become the same `Id` although they
  // are different IRIs. (The nibble encoding has no such problem, because it
  // stores one nibble per digit.)
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
  if (payload > ad_utility::bitMaskForLowerBits(numBitsAvailable)) {
    return std::nullopt;
  }
  return payload;
}

// ____________________________________________________________________________
uint64_t BitRangeConstraint::removeConstrainedBits(
    uint64_t value, const std::vector<BitRangeConstraint>& constraints) {
  uint64_t result = 0;
  size_t outputBitPos = 0;
  size_t inputBitPos = 0;
  for (const auto& constraint : constraints) {
    size_t numBitsToCopy = constraint.start_ - inputBitPos;
    if (numBitsToCopy > 0) {
      uint64_t bits = (value >> inputBitPos) &
                      ad_utility::bitMaskForLowerBits(numBitsToCopy);
      result |= bits << outputBitPos;
      outputBitPos += numBitsToCopy;
    }
    inputBitPos = constraint.end_;
  }
  // Copy the bits above the last constraint. Note that
  // `inputBitPos == numBitsTotal` is possible (a constraint that ends at the
  // most significant bit), in which case there is nothing left to copy and
  // shifting by 64 would be undefined.
  if (inputBitPos < numBitsTotal) {
    result |= (value >> inputBitPos) << outputBitPos;
  }
  return result;
}

// ____________________________________________________________________________
uint64_t BitRangeConstraint::reinsertConstrainedBits(
    uint64_t payload, const std::vector<BitRangeConstraint>& constraints) {
  uint64_t result = 0;
  size_t inputBitPos = 0;
  size_t outputBitPos = 0;
  for (const auto& constraint : constraints) {
    size_t numBitsToCopy = constraint.start_ - outputBitPos;
    if (numBitsToCopy > 0) {
      uint64_t bits = (payload >> inputBitPos) &
                      ad_utility::bitMaskForLowerBits(numBitsToCopy);
      result |= bits << outputBitPos;
      inputBitPos += numBitsToCopy;
    }
    result |= constraint.value_ << constraint.start_;
    outputBitPos = constraint.end_;
  }
  // See the note in `removeConstrainedBits`; here both positions can be 64.
  if (outputBitPos < numBitsTotal && inputBitPos < numBitsTotal) {
    result |= (payload >> inputBitPos) << outputBitPos;
  }
  return result;
}

}  // namespace encodedIris
