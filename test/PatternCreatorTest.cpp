//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/index/PatternCreator.h"
#include "../src/util/Serializer/Serializer.h"

TEST(PatternStatistics, Initialization) {
  PatternStatistics patternStatistics{50, 25, 4};
  ASSERT_EQ(patternStatistics._numDistinctSubjectPredicate, 50u);
  ASSERT_FLOAT_EQ(patternStatistics._avgNumPredicatesPerSubject, 2.0);
  ASSERT_FLOAT_EQ(patternStatistics._avgNumSubjectsPerPredicate, 12.5);
}

TEST(PatternStatistics, Serialization) {
  PatternStatistics patternStatistics{50, 25, 4};
  ad_utility::serialization::ByteBufferWriteSerializer writer;
  writer << patternStatistics;
  ad_utility::serialization::ByteBufferReadSerializer reader{
      std::move(writer).data()};

  PatternStatistics statistics2;
  reader >> statistics2;

  ASSERT_EQ(statistics2._numDistinctSubjectPredicate, 50u);
  ASSERT_FLOAT_EQ(statistics2._avgNumPredicatesPerSubject, 2.0);
  ASSERT_FLOAT_EQ(statistics2._avgNumSubjectsPerPredicate, 12.5);
}

// Helper function: push a small spo permutation to a PatternCreator
void pushTriples(PatternCreator& creator) {
  creator.pushTriple({0, 10, 20});
  creator.pushTriple({0, 10, 21});
  creator.pushTriple({0, 11, 18});
  creator.pushTriple({1, 12, 18});
  creator.pushTriple({1, 13, 18});
  creator.pushTriple({3, 10, 28});
  creator.pushTriple({3, 10, 29});
  creator.pushTriple({3, 11, 45});
}

// Assert that the contents of patterns read from `filename` match the triples
// from the `pushTriples` function.
void assertPatternContents(std::string filename) {
  double averageNumSubjectsPerPredicate;
  double averageNumPredicatesPerSubject;
  uint64_t numDistinctSubjectPredicatePairs;
  CompactVectorOfStrings<Id> patterns;
  std::vector<PatternID> subjectToPattern;

  PatternCreator::readPatternsFromFile(
      filename, averageNumSubjectsPerPredicate, averageNumPredicatesPerSubject,
      numDistinctSubjectPredicatePairs, patterns, subjectToPattern);

  ASSERT_EQ(numDistinctSubjectPredicatePairs, 6);
  ASSERT_FLOAT_EQ(averageNumPredicatesPerSubject, 2.0);
  ASSERT_FLOAT_EQ(averageNumSubjectsPerPredicate, 6.0 / 4.0);

  // We have to patterns: (10, 11) and (12, 13)
  ASSERT_EQ(patterns.size(), 2);

  ASSERT_EQ(patterns[0].size(), 2);
  ASSERT_EQ(patterns[0][0], 10);
  ASSERT_EQ(patterns[0][1], 11);

  ASSERT_EQ(patterns[1].size(), 2);
  ASSERT_EQ(patterns[1][0], 12);
  ASSERT_EQ(patterns[1][1], 13);

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
  pushTriples(creator);
  creator.finish();

  assertPatternContents(filename);
  ad_utility::deleteFile(filename);
}

TEST(PatternCreator, writeAndReadWithDestructor) {
  std::string filename = "patternCreator.test.tmp";
  {
    PatternCreator creator{filename};
    pushTriples(creator);
    // The destructor of the following } automatically runs
    // `creator.finish()`
  }

  assertPatternContents(filename);
  ad_utility::deleteFile(filename);
}

TEST(PatternCreator, writeAndReadWithDestructorAndFinish) {
  std::string filename = "patternCreator.test.tmp";
  {
    PatternCreator creator{filename};
    pushTriples(creator);
    creator.finish();
    // The destructor of the following } does not run `creator.finish()`,
    // because it has already been manually called.
  }

  assertPatternContents(filename);
  ad_utility::deleteFile(filename);
}
