// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <string_view>
#include <vector>

#include "engine/RlePrefixCompressor.h"
#include "global/Id.h"
#include "global/ValueId.h"

using namespace ql::engine::rle;

TEST(RlePrefixCompressorTest, BasicRunLengthConstantFolding) {
  RlePrefixFormatter formatter{
      RleFormatterConfig{.prefix_ = "<", .suffix_ = ">", .delimiter_ = " "}};

  auto id = ValueId::makeFromVocabIndex(VocabIndex::make(100));
  std::string_view rawTerm = "http://example.org/entity/Q42";

  std::array<char, 4096> buffer{};
  char* curr = buffer.data();

  // 100 identical rows
  for (size_t i = 0; i < 100; ++i) {
    curr = formatter.formatPrefix(id, rawTerm, curr);
  }

  const auto& stats = formatter.stats();
  EXPECT_EQ(stats.totalTerms_, 100u);
  EXPECT_EQ(stats.cacheMisses_, 1u);
  EXPECT_EQ(stats.cacheHits_, 99u);
  EXPECT_DOUBLE_EQ(stats.reductionPercentage(), 99.0);

  std::string expectedRow = "<http://example.org/entity/Q42> ";
  std::string expectedFull;
  for (size_t i = 0; i < 100; ++i) {
    expectedFull += expectedRow;
  }

  std::string_view actual(buffer.data(), curr - buffer.data());
  EXPECT_EQ(actual, expectedFull);
}

TEST(RlePrefixCompressorTest, DynamicSwitchingWhenRunEnds) {
  RlePrefixFormatter formatter{
      RleFormatterConfig{.prefix_ = "<", .suffix_ = ">", .delimiter_ = "\t"}};

  auto idA = ValueId::makeFromVocabIndex(VocabIndex::make(1));
  auto idB = ValueId::makeFromVocabIndex(VocabIndex::make(2));
  auto idC = ValueId::makeFromVocabIndex(VocabIndex::make(3));

  std::string_view termA = "http://example.org/A";
  std::string_view termB = "http://example.org/B";
  std::string_view termC = "http://example.org/C";

  std::array<char, 4096> buffer{};
  char* curr = buffer.data();

  // 5 of A
  for (size_t i = 0; i < 5; ++i) {
    curr = formatter.formatPrefix(idA, termA, curr);
  }
  // 3 of B
  for (size_t i = 0; i < 3; ++i) {
    curr = formatter.formatPrefix(idB, termB, curr);
  }
  // 4 of C
  for (size_t i = 0; i < 4; ++i) {
    curr = formatter.formatPrefix(idC, termC, curr);
  }

  const auto& stats = formatter.stats();
  EXPECT_EQ(stats.totalTerms_, 12u);
  EXPECT_EQ(stats.cacheMisses_, 3u);
  EXPECT_EQ(stats.cacheHits_, 9u);

  std::string expected;
  for (size_t i = 0; i < 5; ++i) expected += "<http://example.org/A>\t";
  for (size_t i = 0; i < 3; ++i) expected += "<http://example.org/B>\t";
  for (size_t i = 0; i < 4; ++i) expected += "<http://example.org/C>\t";

  std::string_view actual(buffer.data(), curr - buffer.data());
  EXPECT_EQ(actual, expected);
}

TEST(RlePrefixCompressorTest, UnsortedAlternatingIds) {
  RlePrefixFormatter formatter{
      RleFormatterConfig{.prefix_ = "<", .suffix_ = ">", .delimiter_ = " "}};

  auto idA = ValueId::makeFromVocabIndex(VocabIndex::make(1));
  auto idB = ValueId::makeFromVocabIndex(VocabIndex::make(2));
  std::string_view termA = "http://example.org/A";
  std::string_view termB = "http://example.org/B";

  std::array<char, 1024> buffer{};
  char* curr = buffer.data();

  // Alternating: A, B, A, B
  curr = formatter.formatPrefix(idA, termA, curr);
  curr = formatter.formatPrefix(idB, termB, curr);
  curr = formatter.formatPrefix(idA, termA, curr);
  curr = formatter.formatPrefix(idB, termB, curr);

  const auto& stats = formatter.stats();
  EXPECT_EQ(stats.totalTerms_, 4u);
  EXPECT_EQ(stats.cacheMisses_, 4u);
  EXPECT_EQ(stats.cacheHits_, 0u);

  std::string expected =
      "<http://example.org/A> <http://example.org/B> <http://example.org/A> "
      "<http://example.org/B> ";
  std::string_view actual(buffer.data(), curr - buffer.data());
  EXPECT_EQ(actual, expected);
}

TEST(RlePrefixCompressorTest, LookupFunctorAvoidance) {
  RlePrefixFormatter formatter{
      RleFormatterConfig{.prefix_ = "<", .suffix_ = ">", .delimiter_ = " "}};

  auto idA = ValueId::makeFromVocabIndex(VocabIndex::make(10));
  auto idB = ValueId::makeFromVocabIndex(VocabIndex::make(20));

  size_t lookupCount = 0;
  auto lookup = [&](ValueId id) -> std::string_view {
    ++lookupCount;
    if (id == idA) {
      return "http://example.org/subjectA";
    }
    return "http://example.org/subjectB";
  };

  std::array<char, 4096> buffer{};
  char* curr = buffer.data();

  // 50 of A, then 50 of B
  for (size_t i = 0; i < 50; ++i) {
    curr = formatter.formatPrefixWithLookup(idA, lookup, curr);
  }
  for (size_t i = 0; i < 50; ++i) {
    curr = formatter.formatPrefixWithLookup(idB, lookup, curr);
  }

  EXPECT_EQ(lookupCount, 2u);  // Lookup called ONLY twice across 100 rows!
  EXPECT_EQ(formatter.stats().totalTerms_, 100u);
  EXPECT_EQ(formatter.stats().cacheMisses_, 2u);
  EXPECT_EQ(formatter.stats().cacheHits_, 98u);
  EXPECT_DOUBLE_EQ(formatter.stats().reductionPercentage(), 98.0);
}

TEST(RlePrefixCompressorTest, BatchFormatting) {
  RlePrefixFormatter formatter{
      RleFormatterConfig{.prefix_ = "<", .suffix_ = ">", .delimiter_ = " "}};

  std::vector<ValueId> ids = {ValueId::makeFromVocabIndex(VocabIndex::make(1)),
                              ValueId::makeFromVocabIndex(VocabIndex::make(1)),
                              ValueId::makeFromVocabIndex(VocabIndex::make(2)),
                              ValueId::makeFromVocabIndex(VocabIndex::make(2)),
                              ValueId::makeFromVocabIndex(VocabIndex::make(2))};
  std::vector<std::string_view> terms = {
      "http://example.org/x", "http://example.org/x", "http://example.org/y",
      "http://example.org/y", "http://example.org/y"};

  std::array<char, 1024> buffer{};
  char* end = formatter.formatBatch(ids, terms, buffer.data());

  std::string_view actual(buffer.data(), end - buffer.data());
  std::string expected =
      "<http://example.org/x> <http://example.org/x> <http://example.org/y> "
      "<http://example.org/y> <http://example.org/y> ";
  EXPECT_EQ(actual, expected);
  EXPECT_EQ(formatter.stats().cacheHits_, 3u);
  EXPECT_EQ(formatter.stats().cacheMisses_, 2u);
}

TEST(RlePrefixCompressorTest, MultiColumnTripleFormattingNTriples) {
  auto tripleFormatter = RleTripleFormatter::makeNTriplesFormatter();

  auto subjId = ValueId::makeFromVocabIndex(VocabIndex::make(1));
  auto predId1 = ValueId::makeFromVocabIndex(VocabIndex::make(10));
  auto predId2 = ValueId::makeFromVocabIndex(VocabIndex::make(11));
  auto objId = ValueId::makeFromVocabIndex(VocabIndex::make(100));

  std::string_view subjStr = "http://example.org/subj";
  std::string_view predStr1 = "http://example.org/pred1";
  std::string_view predStr2 = "http://example.org/pred2";
  std::string_view objStr = "http://example.org/obj";

  std::array<char, 2048> buffer{};
  char* curr = buffer.data();

  // Triple 1: (subj, pred1, obj)
  curr = tripleFormatter.formatTriple(subjId, subjStr, predId1, predStr1, objId,
                                      objStr, curr);
  // Triple 2: (subj, pred1, obj) -> both S and P should hit cache
  curr = tripleFormatter.formatTriple(subjId, subjStr, predId1, predStr1, objId,
                                      objStr, curr);
  // Triple 3: (subj, pred2, obj) -> S hits cache, P misses
  curr = tripleFormatter.formatTriple(subjId, subjStr, predId2, predStr2, objId,
                                      objStr, curr);

  std::string expected =
      "<http://example.org/subj> <http://example.org/pred1> "
      "<http://example.org/obj> .\n"
      "<http://example.org/subj> <http://example.org/pred1> "
      "<http://example.org/obj> .\n"
      "<http://example.org/subj> <http://example.org/pred2> "
      "<http://example.org/obj> .\n";

  std::string_view actual(buffer.data(), curr - buffer.data());
  EXPECT_EQ(actual, expected);

  EXPECT_EQ(tripleFormatter.subjectFormatter().stats().cacheHits_, 2u);
  EXPECT_EQ(tripleFormatter.subjectFormatter().stats().cacheMisses_, 1u);
  EXPECT_EQ(tripleFormatter.predicateFormatter().stats().cacheHits_, 1u);
  EXPECT_EQ(tripleFormatter.predicateFormatter().stats().cacheMisses_, 2u);
}

TEST(RlePrefixCompressorTest, MultiColumnTripleFormattingTsv) {
  auto tsvFormatter = RleTripleFormatter::makeTsvFormatter();

  auto subjId = ValueId::makeFromVocabIndex(VocabIndex::make(1));
  auto predId = ValueId::makeFromVocabIndex(VocabIndex::make(2));
  auto objId = ValueId::makeFromVocabIndex(VocabIndex::make(3));

  std::string_view s = "http://s";
  std::string_view p = "http://p";
  std::string_view o = "http://o";

  std::array<char, 512> buffer{};
  char* curr =
      tsvFormatter.formatTriple(subjId, s, predId, p, objId, o, buffer.data());

  std::string_view actual(buffer.data(), curr - buffer.data());
  EXPECT_EQ(actual, "<http://s>\t<http://p>\t<http://o>\n");
}

TEST(RlePrefixCompressorTest, ResetAndInvalidate) {
  RlePrefixFormatter formatter{
      RleFormatterConfig{.prefix_ = "<", .suffix_ = ">", .delimiter_ = " "}};

  auto id = ValueId::makeFromVocabIndex(VocabIndex::make(1));
  std::string_view term = "http://test";

  std::array<char, 256> buffer{};
  formatter.formatPrefix(id, term, buffer.data());
  EXPECT_EQ(formatter.stats().totalTerms_, 1u);

  formatter.reset();
  EXPECT_EQ(formatter.stats().totalTerms_, 0u);
  EXPECT_EQ(formatter.stats().cacheHits_, 0u);
  EXPECT_EQ(formatter.stats().cacheMisses_, 0u);

  // Next format must be a cache miss again
  formatter.formatPrefix(id, term, buffer.data());
  EXPECT_EQ(formatter.stats().cacheMisses_, 1u);
  EXPECT_EQ(formatter.stats().cacheHits_, 0u);
}
