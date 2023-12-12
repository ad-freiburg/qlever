//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./util/IdTestHelpers.h"
#include "index/PatternCreator.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/Serializer.h"

namespace {
auto V = ad_utility::testing::VocabId;
}

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
void createExamplePatterns(PatternCreator& creator) {
  creator.processTriple({V(0), V(10), V(20)});
  creator.processTriple({V(0), V(10), V(21)});
  creator.processTriple({V(0), V(11), V(18)});
  creator.processTriple({V(1), V(10), V(18)});
  creator.processTriple({V(1), V(12), V(18)});
  creator.processTriple({V(1), V(13), V(18)});
  creator.processTriple({V(3), V(10), V(28)});
  creator.processTriple({V(3), V(11), V(29)});
  creator.processTriple({V(3), V(11), V(45)});
}

// Assert that the contents of patterns read from `filename` match the triples
// from the `createExamplePatterns` function.
void assertPatternContents(const std::string& filename) {
  double averageNumSubjectsPerPredicate;
  double averageNumPredicatesPerSubject;
  uint64_t numDistinctSubjectPredicatePairs;
  CompactVectorOfStrings<Id> patterns;
  std::vector<PatternID> subjectToPattern;

  PatternCreator::readPatternsFromFile(
      filename, averageNumSubjectsPerPredicate, averageNumPredicatesPerSubject,
      numDistinctSubjectPredicatePairs, patterns, subjectToPattern);

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

  ASSERT_EQ(subjectToPattern.size(), 4);
  ASSERT_EQ(0, subjectToPattern[0]);
  ASSERT_EQ(1, subjectToPattern[1]);
  ASSERT_EQ(NO_PATTERN, subjectToPattern[2]);
  ASSERT_EQ(0, subjectToPattern[3]);
}

TEST(PatternCreator, writeAndReadWithFinish) {
  std::string filename = "patternCreator.test.tmp";
  PatternCreator creator{filename};
  createExamplePatterns(creator);
  creator.finish();

  assertPatternContents(filename);
  ad_utility::deleteFile(filename);
}

TEST(PatternCreator, writeAndReadWithDestructor) {
  std::string filename = "patternCreator.test.tmp";
  {
    PatternCreator creator{filename};
    createExamplePatterns(creator);
    // The destructor of  `creator` at the following `} automatically runs
    // `creator.finish()`
  }

  assertPatternContents(filename);
  ad_utility::deleteFile(filename);
}

TEST(PatternCreator, writeAndReadWithDestructorAndFinish) {
  std::string filename = "patternCreator.test.tmp";
  {
    PatternCreator creator{filename};
    createExamplePatterns(creator);
    creator.finish();
    // The destructor of `creator` at the following `}` does not run
    // `creator.finish()` because it has already been manually called.
  }

  assertPatternContents(filename);
  ad_utility::deleteFile(filename);
}
