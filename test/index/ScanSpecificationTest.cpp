//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "index/ScanSpecification.h"

// _____________________________________________________________________________
TEST(ScanSpecification, validate) {
  Id i = Id::makeFromInt(42);
  auto n = std::nullopt;
  using S = ScanSpecification;
  EXPECT_NO_THROW(S(i, i, i));
  EXPECT_NO_THROW(S(i, i, n));
  EXPECT_NO_THROW(S(i, n, n));
  EXPECT_NO_THROW(S(n, n, n));

  EXPECT_ANY_THROW(S(n, i, i));
  EXPECT_ANY_THROW(S(n, n, i));
  EXPECT_ANY_THROW(S(n, i, n));
  EXPECT_ANY_THROW(S(i, n, i));
}

// _____________________________________________________________________________
TEST(ScanSpecification, ScanSpecificationAsTripleComponent) {
  Id i = Id::makeFromInt(42);
  TripleComponent iTc{42};
  auto n = std::nullopt;
  using S = ScanSpecification;
  using STc = ScanSpecificationAsTripleComponent;

  EXPECT_ANY_THROW(STc(n, iTc, iTc));
  EXPECT_ANY_THROW(STc(n, n, iTc));
  EXPECT_ANY_THROW(STc(n, iTc, n));
  EXPECT_ANY_THROW(STc(iTc, n, iTc));

  const auto& index = ad_utility::testing::getQec()->getIndex();
  auto toScanSpec = [&index](const STc& s) {
    return s.toScanSpecification(index);
  };

  // Match that a `ScanSpecificationAsTripleComponent` has the expected number
  // of columns, and yields the expected `ScanSpecification` when
  // `toScanSpecification` is called on it.
  auto matchScanSpec =
      [&toScanSpec](const std::optional<ScanSpecification> spec,
                    size_t numColumns = 0) -> ::testing::Matcher<const STc&> {
    auto innerMatcher = [&toScanSpec, &spec] {
      return ::testing::ResultOf(toScanSpec, ::testing::Eq(spec));
    };
    if (!spec.has_value()) {
      return innerMatcher();
    } else {
      return ::testing::AllOf(
          innerMatcher(),
          AD_PROPERTY(STc, numColumns, ::testing::Eq(numColumns)));
    }
  };
  EXPECT_THAT(STc(iTc, iTc, iTc), matchScanSpec(S(i, i, i), 0));
  EXPECT_THAT(STc(iTc, iTc, n), matchScanSpec(S(i, i, n), 1));
  EXPECT_THAT(STc(iTc, n, n), matchScanSpec(S(i, n, n), 2));
  EXPECT_THAT(STc(n, n, n), matchScanSpec(S(n, n, n), 3));

  // Test the resolution of vocab entries.
  auto getId = ad_utility::testing::makeGetId(index);
  auto x = getId("<x>");
  TripleComponent xIri = TripleComponent::Iri::fromIriref("<x>");

  EXPECT_THAT(STc(xIri, xIri, xIri), matchScanSpec(S(x, x, x), 0));

  // For an entry that is not in the vocabulary, the complete result of
  // `toScanSpecification` is `nullopt`.
  TripleComponent notInVocab =
      TripleComponent::Iri::fromIriref("<thisIriIsNotContained>");
  EXPECT_THAT(STc(notInVocab, xIri, xIri), matchScanSpec(std::nullopt));
  EXPECT_THAT(STc(xIri, notInVocab, xIri), matchScanSpec(std::nullopt));
  EXPECT_THAT(STc(xIri, xIri, notInVocab), matchScanSpec(std::nullopt));
}
