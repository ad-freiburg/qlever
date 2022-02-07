//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/index/vocabulary/PrefixCompressor.h"

TEST(PrefixCompressor, CompressionPreservesWords) {
  PrefixCompressor p;
  p.initializePrefixes(std::vector<std::string>{"alph", "alpha", "al"});

  std::vector<std::string> words{
      "a",     "al",       "alp",     "alph",
      "alpha", "alphabet", "betabet", std::string{0, 0, 'a', 1}};

  for (const auto& word : words) {
    ASSERT_NE(p.compress(word), word);
    ASSERT_EQ(p.decompress(p.compress(word)), word);
  }
}

TEST(PrefixCompressor, OverlappingPrefixes) {
  PrefixCompressor p;
  p.initializePrefixes(std::vector<std::string>{"alph", "alpha", "al"});

  // 1 byte prefix for "alpha" + "bet"
  ASSERT_EQ(p.compress("alphabet").size(), 4u);

  // no found prefix, so the encoding is one byte longer;
  std::string_view s = "nothing";
  ASSERT_EQ(p.compress(s).size(), s.size() + 1);

  // Matches the shorter prefix "al"
  ASSERT_EQ(p.compress("alfa").size(), 3u);

  // Matches no prefix, but is a prefix of some of the prefixes
  ASSERT_EQ(p.compress("a").size(), 2u);
}

TEST(PrefixCompressor, TooManyPrefixesYieldError) {
  PrefixCompressor p;
  std::vector<std::string> tooLong;
  for (size_t i = 0; i < NUM_COMPRESSION_PREFIXES + 1; ++i) {
    tooLong.push_back(std::to_string(i));
  }
  ASSERT_THROW(p.initializePrefixes(tooLong), ad_semsearch::Exception);
}

TEST(PrefixCompressor, MaximumNumberOfPrefixes) {
  PrefixCompressor p;
  std::vector<std::string> justRight;
  for (size_t i = 0; i < NUM_COMPRESSION_PREFIXES; ++i) {
    justRight.push_back("aaaaa" + std::to_string(i));
  }

  p.initializePrefixes(justRight);

  // Check that all prefixes are correctly found
  for (const auto& prefix : justRight) {
    auto comp = p.compress(prefix);
    ASSERT_EQ(comp.size(), 1u);
    ASSERT_EQ(prefix, p.decompress(comp));
  }
}
