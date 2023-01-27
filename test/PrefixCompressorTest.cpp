//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/index/vocabulary/PrefixCompressor.h"

TEST(PrefixCompressor, CompressionPreservesWords) {
  PrefixCompressor p;
  p.buildCodebook(std::vector<std::string>{"alph", "alpha", "al"});

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
  p.buildCodebook(std::vector<std::string>{"alph", "alpha", "al"});

  // 1 byte for prefix "alpha" + 3 bytes for "bet".
  ASSERT_EQ(p.compress("alphabet").size(), 4u);

  // The encoding is one byte longer because of the "no prefix" code.
  std::string_view s = "nothing";
  ASSERT_EQ(p.compress(s).size(), s.size() + 1);

  // Matches the shorter prefix "al".
  ASSERT_EQ(p.compress("alfa").size(), 3u);

  // Matches no prefix, but is a prefix of some of the prefixes.
  ASSERT_EQ(p.compress("a").size(), 2u);
}

TEST(PrefixCompressor, TooManyPrefixesThrow) {
  PrefixCompressor p;
  std::vector<std::string> tooManyPrefixes;
  for (size_t i = 0; i < NUM_COMPRESSION_PREFIXES + 1; ++i) {
    tooManyPrefixes.push_back(std::to_string(i));
  }
  ASSERT_THROW(p.buildCodebook(tooManyPrefixes), ad_utility::Exception);
}

TEST(PrefixCompressor, MaximumNumberOfPrefixes) {
  PrefixCompressor p;
  std::vector<std::string> maximalNumberOfPrefixes;
  for (size_t i = 0; i < NUM_COMPRESSION_PREFIXES; ++i) {
    maximalNumberOfPrefixes.push_back("aaaaa" + std::to_string(i));
  }

  p.buildCodebook(maximalNumberOfPrefixes);

  // Check that all prefixes are correctly found
  for (const auto& prefix : maximalNumberOfPrefixes) {
    auto comp = p.compress(prefix);
    ASSERT_EQ(comp.size(), 1u);
    ASSERT_EQ(prefix, p.decompress(comp));
  }
}
