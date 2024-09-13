//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "index/ScanSpecification.h"

TEST(ScanSpecification, getters) {
  Id i = Id::makeFromInt(42);
  Id j = Id::makeFromInt(47);
  Id k = Id::makeFromInt(49);
  auto s = ScanSpecification{i, j, k};
  EXPECT_EQ(s.col0Id(), i);
  EXPECT_EQ(s.col1Id(), j);
  EXPECT_EQ(s.col2Id(), k);
  AD_EXPECT_NULLOPT(s.graphsToFilter());

  auto graphsToFilter = ad_utility::HashSet<Id>{i, k};

  auto n = std::nullopt;
  s = ScanSpecification{n, n, n, {}, graphsToFilter};
  AD_EXPECT_NULLOPT(s.col0Id());
  AD_EXPECT_NULLOPT(s.col1Id());
  AD_EXPECT_NULLOPT(s.col2Id());
  EXPECT_THAT(s.graphsToFilter(),
              ::testing::Optional(
                  ::testing::UnorderedElementsAreArray(graphsToFilter)));
}
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
  auto matchScanSpec = [&toScanSpec](const ScanSpecification spec,
                                     size_t numColumns =
                                         0) -> ::testing::Matcher<const STc&> {
    auto innerMatcher = [&toScanSpec, &spec] {
      using namespace ::testing;
      auto graphMatcher = [&spec]() -> Matcher<const S::Graphs&> {
        if (!spec.graphsToFilter().has_value()) {
          return Eq(std::nullopt);
        } else {
          return Optional(
              UnorderedElementsAreArray(spec.graphsToFilter().value()));
        }
      };
      return ResultOf(toScanSpec,
                      AllOf(AD_PROPERTY(S, col0Id, spec.col0Id()),
                            AD_PROPERTY(S, col1Id, spec.col1Id()),
                            AD_PROPERTY(S, col2Id, spec.col2Id()),
                            AD_PROPERTY(S, graphsToFilter, graphMatcher())));
    };
    return AllOf(AD_PROPERTY(STc, numColumns, numColumns), innerMatcher());
  };
  EXPECT_THAT(STc(iTc, iTc, iTc), matchScanSpec(S(i, i, i), 0));
  EXPECT_THAT(STc(iTc, iTc, n), matchScanSpec(S(i, i, n), 1));
  EXPECT_THAT(STc(iTc, n, n), matchScanSpec(S(i, n, n), 2));
  EXPECT_THAT(STc(n, n, n), matchScanSpec(S(n, n, n), 3));
  // An Example with graph Ids
  using GIri = ad_utility::HashSet<TripleComponent>;
  using G = ad_utility::HashSet<Id>;
  ad_utility::HashSet<Id> graphs{i};
  EXPECT_THAT(STc(n, n, n, GIri{iTc}), matchScanSpec(S(n, n, n, {}, G{i}), 3));
  // Test that the matcher is in fact sensitive to the Graph ID.
  EXPECT_THAT(STc(n, n, n, GIri{iTc}),
              ::testing::Not(matchScanSpec(S(n, n, n), 3)));

  // Test the resolution of vocab entries.
  auto getId = ad_utility::testing::makeGetId(index);
  auto x = getId("<x>");
  TripleComponent xIri = TripleComponent::Iri::fromIriref("<x>");

  EXPECT_THAT(STc(xIri, xIri, xIri), matchScanSpec(S(x, x, x), 0));

  // For an entry that is not in the vocabulary, the complete result of
  // `toScanSpecification` is `nullopt`.
  TripleComponent notInVocab =
      TripleComponent::Iri::fromIriref("<thisIriIsNotContained>");
  LocalVocabEntry localVocabEntry{notInVocab.getIri()};
  auto localVocabId = Id::makeFromLocalVocabIndex(&localVocabEntry);
  EXPECT_THAT(STc(notInVocab, xIri, xIri),
              matchScanSpec(S(localVocabId, x, x)));
  EXPECT_THAT(STc(xIri, notInVocab, xIri),
              matchScanSpec(S(x, localVocabId, x)));
  EXPECT_THAT(STc(xIri, xIri, notInVocab, GIri{xIri, notInVocab}),
              matchScanSpec(S(x, x, localVocabId, {}, G{x, localVocabId})));
  EXPECT_THAT(STc(xIri, xIri, notInVocab, GIri{xIri, notInVocab}),
              ::testing::Not(matchScanSpec(S(x, x, localVocabId))));
}
