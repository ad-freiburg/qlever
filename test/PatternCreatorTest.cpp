//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "./util/IdTestHelpers.h"
#include "global/SpecialIds.h"
#include "index/PatternCreator.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/Serializer.h"

namespace {
auto V = ad_utility::testing::VocabId;
auto I = ad_utility::testing::IntId;
ad_utility::MemorySize memForStxxl = 10_MB;

using TripleVec = std::vector<std::array<Id, 3>>;

// Convert a PSOSorter to a vector of triples for easier handling
TripleVec getVectorFromSorter(PatternCreator::PSOSorter&& sorter) {
  TripleVec triples;
  for (const auto& triple : sorter.sortedView()) {
    triples.push_back(static_cast<std::array<Id, 3>>(triple));
  }
  return triples;
}
}  // namespace

TEST(PatternStatistics, Initialization) {
  PatternStatistics patternStatistics{50, 25, 4};
  ASSERT_EQ(patternStatistics._numDistinctSubjectPredicatePairs, 50u);
  ASSERT_FLOAT_EQ(patternStatistics._avgNumDistinctPredicatesPerSubject, 2.0);
  ASSERT_FLOAT_EQ(patternStatistics._avgNumDistinctSubjectsPerPredicate, 12.5);
}

TEST(PatternStatistics, Serialization) {
  PatternStatistics patternStatistics{50, 25, 4};
  ad_utility::serialization::ByteBufferWriteSerializer writer;
  writer << patternStatistics;
  ad_utility::serialization::ByteBufferReadSerializer reader{
      std::move(writer).data()};

  PatternStatistics statistics2;
  reader >> statistics2;

  ASSERT_EQ(statistics2._numDistinctSubjectPredicatePairs, 50u);
  ASSERT_FLOAT_EQ(statistics2._avgNumDistinctPredicatesPerSubject, 2.0);
  ASSERT_FLOAT_EQ(statistics2._avgNumDistinctSubjectsPerPredicate, 12.5);
}

// Create patterns from a small SPO-sorted sequence of triples.
void createExamplePatterns(PatternCreator& creator) {
  creator.processTriple({V(0), V(10), V(20)}, false);
  creator.processTriple({V(0), V(10), V(21)}, false);
  creator.processTriple({V(0), V(11), V(18)}, false);
  creator.processTriple({V(1), V(10), V(18)}, false);
  creator.processTriple({V(1), V(12), V(18)}, false);
  creator.processTriple({V(1), V(13), V(18)}, false);
  creator.processTriple({V(3), V(10), V(28)}, false);
  creator.processTriple({V(3), V(11), V(29)}, false);
  creator.processTriple({V(3), V(11), V(45)}, false);
}

// Assert that the contents of patterns read from `filename` match the triples
// from the `createExamplePatterns` function.
void assertPatternContents(const std::string& filename,
                           const TripleVec& addedTriples) {
  double averageNumSubjectsPerPredicate;
  double averageNumPredicatesPerSubject;
  uint64_t numDistinctSubjectPredicatePairs;
  CompactVectorOfStrings<Id> patterns;

  PatternCreator::readPatternsFromFile(
      filename, averageNumSubjectsPerPredicate, averageNumPredicatesPerSubject,
      numDistinctSubjectPredicatePairs, patterns);
  // TODO<joka921> Also test the created triples.

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
  auto pat = qlever::specialIds.at(HAS_PATTERN_PREDICATE);
  auto pred = qlever::specialIds.at(HAS_PREDICATE_PREDICATE);
  TripleVec expectedTriples;
  expectedTriples.push_back(std::array{V(0), pat, I(0)});
  expectedTriples.push_back(std::array{V(1), pat, I(1)});
  expectedTriples.push_back(std::array{V(3), pat, I(0)});
  expectedTriples.push_back(std::array{V(0), pred, V(10)});
  expectedTriples.push_back(std::array{V(0), pred, V(11)});
  expectedTriples.push_back(std::array{V(1), pred, V(10)});
  expectedTriples.push_back(std::array{V(1), pred, V(12)});
  expectedTriples.push_back(std::array{V(1), pred, V(13)});
  expectedTriples.push_back(std::array{V(3), pred, V(10)});
  expectedTriples.push_back(std::array{V(3), pred, V(11)});
  std::ranges::sort(expectedTriples, SortByPSO{});
  EXPECT_THAT(addedTriples, ::testing::ElementsAreArray(expectedTriples));
}

TEST(PatternCreator, writeAndReadWithFinish) {
  std::string filename = "patternCreator.test.tmp";
  PatternCreator creator{filename, memForStxxl};
  createExamplePatterns(creator);
  creator.finish();

  assertPatternContents(
      filename,
      getVectorFromSorter(std::move(creator).getHasPatternSortedByPSO()));
  ad_utility::deleteFile(filename);
}

TEST(PatternCreator, writeAndReadWithDestructor) {
  std::string filename = "patternCreator.test.tmp";
  TripleVec triples;
  {
    PatternCreator creator{filename, memForStxxl};
    createExamplePatterns(creator);
    // the extraction of the sorter automatically calls `finish`.
    triples =
        getVectorFromSorter(std::move(creator).getHasPatternSortedByPSO());
  }

  assertPatternContents(filename, triples);
  ad_utility::deleteFile(filename);
}

TEST(PatternCreator, writeAndReadWithDestructorAndFinish) {
  std::string filename = "patternCreator.test.tmp";
  TripleVec triples;
  {
    PatternCreator creator{filename, memForStxxl};
    createExamplePatterns(creator);
    creator.finish();
    triples =
        getVectorFromSorter(std::move(creator).getHasPatternSortedByPSO());
  }

  assertPatternContents(filename, triples);
  ad_utility::deleteFile(filename);
}
