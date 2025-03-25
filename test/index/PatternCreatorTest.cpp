//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "../util/IdTestHelpers.h"
#include "global/SpecialIds.h"
#include "index/PatternCreator.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/Serializer.h"

namespace {
auto V = ad_utility::testing::VocabId;
auto I = ad_utility::testing::IntId;
ad_utility::MemorySize memForStxxl = 10_MB;

using TripleVec = std::vector<std::array<Id, 3>>;

static const Id idOfHasPattern =
    Id::makeFromVocabIndex(VocabIndex::make(120394835));

// Convert a PSOSorter to a vector of triples for easier handling
TripleVec getVectorFromSorter(PatternCreator::PSOSorter&& sorter) {
  TripleVec triples;
  for (const auto& triple : sorter.sortedView()) {
    triples.push_back(static_cast<std::array<Id, 3>>(triple));
  }
  return triples;
}

using ad_utility::source_location;
}  // namespace

TEST(PatternStatistics, Initialization) {
  PatternStatistics patternStatistics{50, 25, 4};
  ASSERT_EQ(patternStatistics.numDistinctSubjectPredicatePairs_, 50u);
  ASSERT_FLOAT_EQ(patternStatistics.avgNumDistinctPredicatesPerSubject_, 2.0);
  ASSERT_FLOAT_EQ(patternStatistics.avgNumDistinctSubjectsPerPredicate_, 12.5);
}

TEST(PatternStatistics, Serialization) {
  PatternStatistics patternStatistics{50, 25, 4};
  ad_utility::serialization::ByteBufferWriteSerializer writer;
  writer << patternStatistics;
  ad_utility::serialization::ByteBufferReadSerializer reader{
      std::move(writer).data()};

  PatternStatistics statistics2;
  reader >> statistics2;

  ASSERT_EQ(statistics2.numDistinctSubjectPredicatePairs_, 50u);
  ASSERT_FLOAT_EQ(statistics2.avgNumDistinctPredicatesPerSubject_, 2.0);
  ASSERT_FLOAT_EQ(statistics2.avgNumDistinctSubjectsPerPredicate_, 12.5);
}

// Create patterns from a small SPO-sorted sequence of triples.
auto createExamplePatterns(PatternCreator& creator) {
  using A = std::array<Id, NumColumnsIndexBuilding + 1>;
  std::vector<A> expected;

  // push the `triple` with the `isIgnored` information to the pattern creator,
  // and expect that the triple gets the `patternIdx` assigned by pushing the
  // corresponding info to `expected`.
  auto graphPayload = Id::makeFromInt(2365);
  auto push = [&creator, &expected, graphPayload](std::array<Id, 3> triple,
                                                  bool isIgnoredTriple,
                                                  size_t patternIdx) {
    const auto& [s, p, o] = triple;
    auto withGraph = std::array{s, p, o, graphPayload};
    static_assert(NumColumnsIndexBuilding == 4,
                  "The following lines have to be changed once additional "
                  "payload columns are added");
    creator.processTriple(withGraph, isIgnoredTriple);
    expected.push_back(
        A{triple[0], triple[1], triple[2], graphPayload, I(patternIdx)});
  };

  // The first subject gets the first pattern. We have an ignored triple at the
  // end which doesn't count towards the pattern.
  push({V(0), V(10), V(20)}, false, 0);
  push({V(0), V(10), V(21)}, false, 0);
  push({V(0), V(11), V(18)}, false, 0);
  push({V(0), V(12), V(18)}, true, 0);

  // New subject, different predicates, so a new pattern.
  push({V(1), V(10), V(18)}, false, 1);
  // Ignored triple, but `V(1)` has other non-ignored triple, so it will have a
  // pattern, but `V(11)` will not contribute to that pattern.
  push({V(1), V(11), V(18)}, true, 1);
  push({V(1), V(12), V(18)}, false, 1);
  push({V(1), V(13), V(18)}, false, 1);

  // All the triples for subject `V(2)` are ignored, so it will not have a
  // pattern.
  push({V(2), V(13), V(18)}, true, NO_PATTERN);
  push({V(2), V(14), V(18)}, true, NO_PATTERN);

  // New subject, but has the same predicate and therefore patterns as `V(0)`.
  // We have an ignored triple at the beginning, which doesn't count towards
  // the pattern.
  push({V(3), V(9), V(18)}, true, 0);
  push({V(3), V(10), V(28)}, false, 0);
  push({V(3), V(11), V(29)}, false, 0);
  push({V(3), V(11), V(45)}, false, 0);

  ql::ranges::sort(expected, SortByOSP{});
  auto tripleOutputs = std::move(creator).getTripleSorter();
  auto& triples = *tripleOutputs.triplesWithSubjectPatternsSortedByOsp_;
  static constexpr size_t numCols = NumColumnsIndexBuilding + 1;
  std::vector<std::array<Id, numCols>> actual;
  for (auto& block : triples.getSortedBlocks<numCols>()) {
    for (const auto& row : block) {
      actual.push_back(static_cast<std::array<Id, numCols>>(row));
    }
  }
  EXPECT_THAT(actual, ::testing::ElementsAreArray(expected));
  return std::move(tripleOutputs.hasPatternPredicateSortedByPSO_);
}

// Assert that the contents of patterns read from `filename` match the triples
// from the `createExamplePatterns` function.
void assertPatternContents(const std::string& filename,
                           const TripleVec& addedTriples,
                           source_location l = source_location ::current()) {
  auto tr = generateLocationTrace(l);
  double averageNumSubjectsPerPredicate;
  double averageNumPredicatesPerSubject;
  uint64_t numDistinctSubjectPredicatePairs;
  CompactVectorOfStrings<Id> patterns;

  PatternCreator::readPatternsFromFile(
      filename, averageNumSubjectsPerPredicate, averageNumPredicatesPerSubject,
      numDistinctSubjectPredicatePairs, patterns);

  ASSERT_EQ(numDistinctSubjectPredicatePairs, 7);
  ASSERT_FLOAT_EQ(averageNumPredicatesPerSubject, 7.0 / 3.0);
  ASSERT_FLOAT_EQ(averageNumSubjectsPerPredicate, 7.0 / 4.0);

  // We have two patterns: (10, 11) and (10, 12, 13).
  ASSERT_EQ(patterns.size(), 2);

  ASSERT_EQ(patterns[0].size(), 2);
  ASSERT_EQ(patterns[0][0], V(10));
  ASSERT_EQ(patterns[0][1], V(11));

  ASSERT_EQ(patterns[1].size(), 3);
  ASSERT_EQ(patterns[1][0], V(10));
  ASSERT_EQ(patterns[1][1], V(12));
  ASSERT_EQ(patterns[1][2], V(13));

  // We have 4 subjects 0, 1, 2, 3. Subject 2 has no pattern, because
  // it has no triples. Subjects 0 and 3 have the first pattern, subject 1 has
  // the second pattern.
  auto pat = idOfHasPattern;
  // auto pred = qlever::specialIds().at(HAS_PREDICATE_PREDICATE);
  TripleVec expectedTriples;
  expectedTriples.push_back(std::array{V(0), pat, I(0)});
  expectedTriples.push_back(std::array{V(1), pat, I(1)});
  expectedTriples.push_back(std::array{V(3), pat, I(0)});
  ql::ranges::sort(expectedTriples, SortByPSO{});
  EXPECT_THAT(addedTriples, ::testing::ElementsAreArray(expectedTriples));
}

TEST(PatternCreator, writeAndReadWithFinish) {
  std::string filename = "patternCreator.test.tmp";
  PatternCreator creator{filename, idOfHasPattern, memForStxxl};
  auto hashPatternAsPSOPtr = createExamplePatterns(creator);
  creator.finish();

  assertPatternContents(filename,
                        getVectorFromSorter(std::move(*hashPatternAsPSOPtr)));
  ad_utility::deleteFile(filename);
}
