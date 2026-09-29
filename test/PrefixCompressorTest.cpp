// Copyright 2022 - 2026, The QLever Authors, in particular:
//
// 2022 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <limits>
#include <optional>
#include <string_view>

#include "backports/span.h"
#include "index/vocabulary/PrefixCompressor.h"
#include "index/vocabulary/PrefixHeuristic.h"
#include "util/GTestHelpers.h"
#include "util/Views.h"

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

// _____________________________________________________________________________
TEST(PrefixCompressor, DecompressIntoMatchesDecompress) {
  PrefixCompressor p;
  p.buildCodebook(std::vector<std::string>{"alp", "alpha", "al"});
  auto checkWord = [&](std::string_view word) {
    const std::string compressed = p.compress(word);
    const std::string decompressedWord = p.decompress(compressed);
    std::string decompressedIntoBuffer(p.maxDecompressedSize(compressed), '\0');
    const size_t numWritten = p.decompressInto(
        compressed, ql::span<char>{decompressedIntoBuffer.data(),
                                   decompressedIntoBuffer.size()});
    EXPECT_EQ(numWritten, decompressedWord.size());
    EXPECT_EQ(std::string_view(decompressedIntoBuffer.data(), numWritten),
              decompressedWord);
    EXPECT_EQ(decompressedWord, word);
  };
  for (std::string_view word :
       {"", "a", "al", "alp", "alpine", "alpha", "alphabet", "nothing"}) {
    checkWord(word);
  }
  // A word that is exactly a prefix compresses to the code byte alone.
  ASSERT_EQ(p.compress("alpha").size(), 1u);
  AD_EXPECT_THROW_WITH_MESSAGE(static_cast<void>(p.maxDecompressedSize("")),
                               ::testing::HasSubstr("!compressedWord.empty()"));

  std::string emptyInputBuffer(1, '\0');
  AD_EXPECT_THROW_WITH_MESSAGE(
      static_cast<void>(p.decompressInto(
          "",
          ql::span<char>{emptyInputBuffer.data(), emptyInputBuffer.size()})),
      ::testing::HasSubstr("!compressedWord.empty()"));
  AD_EXPECT_THROW_WITH_MESSAGE(static_cast<void>(p.decompress("")),
                               ::testing::HasSubstr("!compressedWord.empty()"));

  const std::string compressed = p.compress("alphabet");
  std::string undersizedBuffer(1, '\0');
  AD_EXPECT_THROW_WITH_MESSAGE(
      static_cast<void>(p.decompressInto(
          compressed,
          ql::span<char>{undersizedBuffer.data(), undersizedBuffer.size()})),
      ::testing::HasSubstr("out.size() >= decompressedSizeWithIndex"));
}

// _____________________________________________________________________________
TEST(PrefixCompressor, PrefixIndexBoundaryMarkers) {
  PrefixCompressor p;
  p.buildCodebook(std::vector<std::string>{"alpha"});

  const std::string compressedAlpha = p.compress("alpha");
  const std::string compressedBeta = p.compress("beta");

  EXPECT_EQ(p.prefixIndex(compressedAlpha), 0u);
  EXPECT_FALSE(p.prefixIndex(compressedBeta).has_value());
  EXPECT_FALSE(p.prefixIndex(std::string(1, static_cast<char>(NO_PREFIX_CHAR)))
                   .has_value());
  EXPECT_FALSE(p.prefixIndex(std::string(1, '\0')).has_value());
  EXPECT_EQ(p.maxDecompressedSize(compressedAlpha), 5u);
  EXPECT_EQ(p.maxDecompressedSize(compressedBeta), 4u);

  std::string decompressedOutput(p.maxDecompressedSize(compressedAlpha), '\0');
  EXPECT_EQ(p.decompressInto(compressedAlpha,
                             ql::span<char>{decompressedOutput.data(),
                                            decompressedOutput.size()}),
            5u);
  EXPECT_EQ(decompressedOutput, "alpha");

  // Accept an oversized buffer, but write and report only the decompressed
  // size.
  std::string oversizedBuffer(p.maxDecompressedSize(compressedAlpha) + 7, 'x');
  EXPECT_EQ(
      p.decompressInto(compressedAlpha, ql::span<char>{oversizedBuffer.data(),
                                                       oversizedBuffer.size()}),
      5u);
  EXPECT_EQ(std::string_view(oversizedBuffer.data(), 5), "alpha");
}

// _____________________________________________________________________________
// Focused boundary coverage for the static `prefixIndex` helper: the exact
// valid range [MIN_COMPRESSION_PREFIX, MIN_COMPRESSION_PREFIX +
// NUM_COMPRESSION_PREFIXES) maps to indices [0, NUM_COMPRESSION_PREFIXES),
// everything else yields `std::nullopt`.
TEST(PrefixCompressor, PrefixIndexBoundaries) {
  const auto byteWord = [](unsigned int byte) {
    return std::string(1, static_cast<char>(byte));
  };
  EXPECT_FALSE(PrefixCompressor::prefixIndex("").has_value());
  EXPECT_FALSE(
      PrefixCompressor::prefixIndex(byteWord(MIN_COMPRESSION_PREFIX - 1))
          .has_value());
  EXPECT_EQ(PrefixCompressor::prefixIndex(byteWord(MIN_COMPRESSION_PREFIX)),
            0u);
  EXPECT_EQ(PrefixCompressor::prefixIndex(byteWord(
                MIN_COMPRESSION_PREFIX + NUM_COMPRESSION_PREFIXES - 1)),
            size_t{NUM_COMPRESSION_PREFIXES} - 1);
  EXPECT_FALSE(PrefixCompressor::prefixIndex(
                   byteWord(MIN_COMPRESSION_PREFIX + NUM_COMPRESSION_PREFIXES))
                   .has_value());
  EXPECT_FALSE(PrefixCompressor::prefixIndex(byteWord(0)).has_value());
}

// _____________________________________________________________________________
// The private per-word helpers check their preconditions themselves. The public
// functions establish these preconditions before calling them, so the checks
// can only be violated by calling the helpers directly.
TEST(PrefixCompressor, HelperContractChecks) {
  PrefixCompressor p;
  p.buildCodebook(std::vector<std::string>{"alpha"});

  // Prefix index outside the codebook.
  AD_EXPECT_THROW_WITH_MESSAGE(
      static_cast<void>(
          p.decompressedSizeWithIndex(0, size_t{NUM_COMPRESSION_PREFIXES})),
      ::testing::HasSubstr("*prefixIdx < prefixToCode_.size()"));
  // Prefix size plus rest size overflows `size_t`.
  AD_EXPECT_THROW_WITH_MESSAGE(
      static_cast<void>(p.decompressedSizeWithIndex(
          std::numeric_limits<size_t>::max(), size_t{0})),
      ::testing::HasSubstr("prefixSize <="));
  EXPECT_EQ(p.decompressedSizeWithIndex(3, size_t{0}), 8u);
  EXPECT_EQ(p.decompressedSizeWithIndex(3, std::nullopt), 3u);

  const std::string compressed = p.compress("alphabet");
  const auto prefixIdx = PrefixCompressor::prefixIndex(compressed);
  ASSERT_EQ(prefixIdx, 0u);
  // The output cannot hold the prefix.
  std::string tooSmallForPrefix(4, '\0');
  AD_EXPECT_THROW_WITH_MESSAGE(
      static_cast<void>(p.decompressIntoWithIndex(
          compressed, prefixIdx,
          ql::span<char>{tooSmallForPrefix.data(), tooSmallForPrefix.size()})),
      ::testing::HasSubstr("prefix.size() <= out.size()"));
  // The output holds the prefix but not the rest.
  std::string tooSmallForRest(6, '\0');
  AD_EXPECT_THROW_WITH_MESSAGE(
      static_cast<void>(p.decompressIntoWithIndex(
          compressed, prefixIdx,
          ql::span<char>{tooSmallForRest.data(), tooSmallForRest.size()})),
      ::testing::HasSubstr("rest.size() <= out.size() - outputSize"));
}

// _____________________________________________________________________________
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

// _____________________________________________________________________________
TEST(PrefixCompressor, prefixCompression) {
  using namespace ::testing;

  EXPECT_THAT(calculatePrefixes({}, 1), UnorderedElementsAre());
  EXPECT_THAT(calculatePrefixes({"", "a", "ab", "abc"}, 1),
              UnorderedElementsAre("a"));
  EXPECT_THAT(calculatePrefixes({"", "a", "ab", "abc"}, 2),
              UnorderedElementsAre("a", "ab"));
  EXPECT_THAT(calculatePrefixes({"", "a", "ab", "abc", "abcd"}, 2),
              UnorderedElementsAre("a", "ab"));
  EXPECT_THAT(calculatePrefixes({"", "a", "ab", "abc", "abcd"}, 3),
              UnorderedElementsAre("a", "ab", "abc"));
  EXPECT_THAT(calculatePrefixes({"", "a", "ab", "abc", "abcd"}, 4),
              UnorderedElementsAre("", "a", "ab", "abc"));
  EXPECT_THAT(calculatePrefixes({"a", "b"}, 1), UnorderedElementsAre(""));
  EXPECT_THAT(calculatePrefixes({"a", "b"}, 2), UnorderedElementsAre("", ""));

  // Newlines handling
  std::vector<std::string> input;
  for (size_t i : ad_utility::integerRange<size_t>(200)) {
    input.push_back(absl::StrCat("\"\"\"\nabc\t\n34as\n\ndj", i, "\"\"\""));
  }

  // There must be at least one of the compression prefixes that compresses the
  // common structure of the literals.
  EXPECT_THAT(calculatePrefixes(input, 127),
              Contains(ContainsRegex("\nabc\t\n")));
}
