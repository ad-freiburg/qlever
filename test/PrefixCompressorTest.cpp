// Copyright 2022 - 2026, The QLever Authors, in particular:
//
// 2022 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>

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
TEST(PrefixCompressor, decompressIntoMatchesDecompress) {
  using namespace ::testing;
  PrefixCompressor p;
  p.buildCodebook(std::vector<std::string>{"alph", "alpha", "al"});
  auto checkWord = [&](std::string_view word) {
    const std::string compressed = p.compress(word);
    const std::string viaString = p.decompress(compressed);
    std::string intoBuf(p.maxDecompressedSize(compressed), '\0');
    const size_t n = p.decompressInto(
        compressed, ql::span<char>{intoBuf.data(), intoBuf.size()});
    EXPECT_THAT(n, Eq(viaString.size()));
    EXPECT_THAT(std::string_view(intoBuf.data(), n), Eq(viaString));
    EXPECT_THAT(viaString, Eq(word));
  };
  for (std::string_view word :
       {"a", "al", "alp", "alph", "alpha", "alphabet", "nothing"}) {
    checkWord(word);
  }
  const std::string onlyPrefix = p.compress("alpha");
  ASSERT_EQ(onlyPrefix.size(), 1u);
  checkWord("alpha");
  AD_EXPECT_THROW_WITH_MESSAGE(static_cast<void>(p.maxDecompressedSize("")),
                               ::testing::HasSubstr("!compressedWord.empty()"));
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
