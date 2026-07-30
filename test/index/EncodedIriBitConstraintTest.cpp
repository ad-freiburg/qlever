// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "index/EncodedIriBitConstraint.h"

namespace {
using encodedIris::BitRangeConstraint;
using Constraints = std::vector<BitRangeConstraint>;

// The number of bits that are available for the payload in the default
// `EncodedIriManager`. The numbers used in the tests below are typical of the
// use case that motivated the constraints: external 64-bit IDs whose top three
// bits are a constant tag `001` and which have a window of zero bits, so that
// 12 of the 64 bits carry no information and do not have to be stored.
constexpr size_t numBitsAvailable = 52;

// A constraint set whose numbers have bits [61, 64) equal to `001` and bits
// [23, 32) equal to zero; 3 + 9 = 12 constrained bits, so 52 remain, which is
// exactly `numBitsAvailable`.
Constraints laneConstraints() {
  Constraints constraints{{61, 64, 0b001}, {23, 32, 0u}};
  BitRangeConstraint::normalizeAndValidate(constraints, numBitsAvailable,
                                           "lane_");
  return constraints;
}

// ____________________________________________________________________________
TEST(BitRangeConstraint, sizeAndMatches) {
  BitRangeConstraint c{61, 64, 0b001};
  EXPECT_EQ(c.size(), 3u);
  EXPECT_TRUE(c.matches(1ULL << 61));
  EXPECT_FALSE(c.matches(0));
  EXPECT_FALSE(c.matches(3ULL << 61));
  // The bits outside of the range are irrelevant.
  EXPECT_TRUE(c.matches((1ULL << 61) | 12345ULL));
}

// ____________________________________________________________________________
TEST(BitRangeConstraint, invalidConstraintsAreRejected) {
  using testing::HasSubstr;
  // An empty bit range.
  AD_EXPECT_THROW_WITH_MESSAGE(BitRangeConstraint(5, 5, 0),
                               HasSubstr("must be nonempty"));
  // A bit range that leaves the 64 bits of the number.
  AD_EXPECT_THROW_WITH_MESSAGE(BitRangeConstraint(60, 65, 0),
                               HasSubstr("within the 64 bits"));
  // A constraint on all 64 bits.
  AD_EXPECT_THROW_WITH_MESSAGE(BitRangeConstraint(0, 64, 0),
                               HasSubstr("must not constrain all 64 bits"));
  // A value that does not fit into its range.
  AD_EXPECT_THROW_WITH_MESSAGE(BitRangeConstraint(0, 2, 4),
                               HasSubstr("must fit into its bit"));
}

// ____________________________________________________________________________
TEST(BitRangeConstraint, normalizeAndValidate) {
  using testing::HasSubstr;
  // Constraints in an arbitrary order are sorted by `start_`, because the order
  // carries no meaning.
  Constraints constraints{{61, 64, 0b001}, {23, 32, 0u}};
  BitRangeConstraint::normalizeAndValidate(constraints, numBitsAvailable,
                                           "lane_");
  EXPECT_THAT(constraints,
              testing::ElementsAre(BitRangeConstraint(23, 32, 0),
                                   BitRangeConstraint(61, 64, 0b001)));

  // An empty set of constraints is always fine (the prefix then uses the nibble
  // encoding).
  Constraints empty;
  EXPECT_NO_THROW(
      BitRangeConstraint::normalizeAndValidate(empty, numBitsAvailable, "x"));
  EXPECT_TRUE(empty.empty());

  // Overlapping ranges, in both input orders (the normalization sorts first, so
  // the error message always names the lower range first).
  Constraints overlapping{{10, 20, 0}, {15, 25, 0}};
  AD_EXPECT_THROW_WITH_MESSAGE(
      BitRangeConstraint::normalizeAndValidate(overlapping, numBitsAvailable,
                                               "pfx"),
      testing::AllOf(HasSubstr("must not overlap"), HasSubstr("[10, 20)"),
                     HasSubstr("[15, 25)"), HasSubstr("pfx")));
  Constraints overlappingReversed{{15, 25, 0}, {10, 20, 0}};
  AD_EXPECT_THROW_WITH_MESSAGE(
      BitRangeConstraint::normalizeAndValidate(overlappingReversed,
                                               numBitsAvailable, "pfx"),
      testing::AllOf(HasSubstr("must not overlap"), HasSubstr("[10, 20)")));

  // Directly adjacent ranges do not overlap and are accepted.
  Constraints adjacent{{52, 58, 0}, {58, 64, 0}};
  EXPECT_NO_THROW(BitRangeConstraint::normalizeAndValidate(
      adjacent, numBitsAvailable, "pfx"));

  // Too few constrained bits: 64 - 4 = 60 bits remain, but only 52 are
  // available.
  Constraints tooFew{{0, 4, 0}};
  AD_EXPECT_THROW_WITH_MESSAGE(
      BitRangeConstraint::normalizeAndValidate(tooFew, numBitsAvailable, "pfx"),
      HasSubstr("constrain at least 12 bits"));
}

// ____________________________________________________________________________
TEST(BitRangeConstraint, encode) {
  auto constraints = laneConstraints();
  // A real ID that fulfills the constraints.
  uint64_t value = 2343140642651111426ULL;
  ASSERT_EQ((value >> 61) & 0b111, 0b001u);
  ASSERT_EQ((value >> 23) & 0x1FFu, 0u);
  auto payload = BitRangeConstraint::encode(std::to_string(value), constraints,
                                            numBitsAvailable);
  ASSERT_TRUE(payload.has_value());
  EXPECT_EQ(
      BitRangeConstraint::reinsertConstrainedBits(payload.value(), constraints),
      value);

  // A number that violates a constraint, one that does not fit into 64 bits,
  // and one with leading zeros are all not encoded.
  EXPECT_FALSE(BitRangeConstraint::encode("42", constraints, numBitsAvailable)
                   .has_value());
  EXPECT_FALSE(BitRangeConstraint::encode("99999999999999999999", constraints,
                                          numBitsAvailable)
                   .has_value());
  EXPECT_FALSE(BitRangeConstraint::encode(absl::StrCat("0", value), constraints,
                                          numBitsAvailable)
                   .has_value());
  // `0` itself is a single digit and has no leading zero, so it is not affected
  // by that check (it violates the constraints here and is rejected for that
  // reason).
  EXPECT_FALSE(BitRangeConstraint::encode("0", constraints, numBitsAvailable)
                   .has_value());
}

// ____________________________________________________________________________
TEST(BitRangeConstraint, exhaustiveBitRoundTrip) {
  // Exhaustively check the bit (de)compression for a constraint set with a gap,
  // over all values of the unconstrained bits in a small window.
  Constraints constraints{{61, 64, 0b001}, {22, 23, 0}, {24, 32, 0u}};
  BitRangeConstraint::normalizeAndValidate(constraints, numBitsAvailable,
                                           "roadPart_");
  uint64_t base = 1ULL << 61;
  for (uint64_t low = 0; low < 512; ++low) {
    for (uint64_t bit23 : {0ULL, 1ULL << 23}) {
      uint64_t value = base | bit23 | low;
      uint64_t payload =
          BitRangeConstraint::removeConstrainedBits(value, constraints);
      EXPECT_LE(payload, ad_utility::bitMaskForLowerBits(numBitsAvailable));
      EXPECT_EQ(
          BitRangeConstraint::reinsertConstrainedBits(payload, constraints),
          value);
    }
  }
}

// ____________________________________________________________________________
TEST(BitRangeConstraint, payloadZero) {
  // The number that consists of exactly the constrained values with all free
  // bits zero is encoded as a payload of `0`.
  auto constraints = laneConstraints();
  uint64_t value = 1ULL << 61;
  EXPECT_EQ(BitRangeConstraint::removeConstrainedBits(value, constraints), 0u);
  EXPECT_EQ(BitRangeConstraint::reinsertConstrainedBits(0, constraints), value);
}

// ____________________________________________________________________________
TEST(BitRangeConstraint, json) {
  BitRangeConstraint c{61, 64, 0b001};
  nlohmann::json j = c;
  EXPECT_EQ(j.at("start").get<size_t>(), 61u);
  EXPECT_EQ(j.at("end").get<size_t>(), 64u);
  EXPECT_EQ(j.at("value").get<uint64_t>(), 0b001u);
  EXPECT_EQ(j.get<BitRangeConstraint>(), c);
  // The validation also happens on deserialization.
  nlohmann::json invalid = c;
  invalid["end"] = 65;
  AD_EXPECT_THROW_WITH_MESSAGE(invalid.get<BitRangeConstraint>(),
                               testing::HasSubstr("within the 64 bits"));
}

}  // namespace
