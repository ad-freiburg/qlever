//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/index/vocabulary/SimpleVocabulary.h"
using Vocab = SimpleVocabulary;

std::ostream& operator<<(std::ostream& o, const Vocab::SearchResult& w) {
  o << w._id << ", ";
  if (w._word) {
    o << w._word.value();
  } else {
    o << "nullopt";
  }

  return o;
}

namespace {

auto vocabsEqual = [](const auto& a, const auto& b) {
  ASSERT_EQ(a.size(), b.size());
  for (size_t i = 0; i < a.size(); ++i) {
    ASSERT_EQ(a[i], b[i]);
  }
};

using SearchResult = SimpleVocabulary::SearchResult;

TEST(SimpleVocabulary, Compiles) { Vocab c; }

auto createVocabulary(const std::vector<std::string>& words) {
  Vocab::Words w;
  w.build(words);
  return Vocab(std::move(w));
}

TEST(SimpleVocabulary, LowerBound) {
  const std::vector<string> words{"alpha", "beta",    "camma",
                                  "delta", "epsilon", "frikadelle"};
  const auto vocab = createVocabulary(words);
  ASSERT_EQ(vocab.size(), words.size());

  for (size_t i = 0; i < vocab.size(); ++i) {
    SearchResult c{i, words[i]};
    EXPECT_EQ(vocab.lower_bound(words[i], std::less<>{}), c);
    auto smallerWord = words[i];
    smallerWord.back()--;
    EXPECT_EQ(vocab.lower_bound(smallerWord, std::less<>{}), c);
  }

  {
    SearchResult c{0, "alpha"};
    ASSERT_EQ(vocab.lower_bound("a", std::less<>{}), c);
  }

  {
    SearchResult c{words.size(), std::nullopt};
    ASSERT_EQ(vocab.lower_bound("xi", std::less<>{}), c);
  }
}

TEST(SimpleVocabulary, UpperBound) {
  std::vector<string> words{"alpha", "beta",    "camma",
                            "delta", "epsilon", "frikadelle"};
  auto vocab = createVocabulary(words);
  ASSERT_EQ(vocab.size(), words.size());

  for (size_t i = 1; i < vocab.size(); ++i) {
    SearchResult c{i, words[i]};
    EXPECT_EQ(vocab.upper_bound(words[i - 1], std::less<>{}), c);
    auto biggerWord = words[i - 1];
    biggerWord.back()++;
    EXPECT_EQ(vocab.upper_bound(biggerWord, std::less<>{}), c);
  }

  {
    SearchResult c{0, words.front()};
    ASSERT_EQ(vocab.upper_bound("alph", std::less<>{}), c);
  }

  {
    SearchResult c{words.size(), std::nullopt};
    ASSERT_EQ(vocab.upper_bound(words.back(), std::less<>{}), c);
  }
}

TEST(SimpleVocabulary, LowerBoundAlternativeComparator) {
  const std::vector<string> words{"4", "33", "222", "1111"};
  auto comp = [](const auto& a, const auto& b) {
    return std::stoi(std::string{a}) < std::stoi(b);
  };

  const auto vocab = createVocabulary(words);
  ASSERT_EQ(vocab.size(), words.size());

  for (size_t i = 0; i < vocab.size(); ++i) {
    SearchResult c{i, words[i]};
    EXPECT_EQ(vocab.lower_bound(words[i], comp), c);
    auto smallerWord = std::to_string(std::stoi(words[i]) - 1);
    EXPECT_EQ(vocab.lower_bound(smallerWord, comp), c);
  }

  {
    SearchResult c{words.size(), std::nullopt};
    ASSERT_EQ(vocab.lower_bound("99999", comp), c);
  }
}

TEST(SimpleVocabulary, UpperBoundAlternativeComparator) {
  const std::vector<string> words{"4", "33", "222", "1111"};
  auto comp = [](const auto& a, const auto& b) {
    return std::stoi(std::string{a}) < std::stoi(std::string{b});
  };

  const auto vocab = createVocabulary(words);
  ASSERT_EQ(vocab.size(), words.size());

  for (size_t i = 1; i < vocab.size(); ++i) {
    SearchResult c{i, words[i]};
    EXPECT_EQ(vocab.upper_bound(words[i - 1], comp), c);
    auto biggerWord = std::to_string(std::stoi(words[i - 1]) + 1);
    EXPECT_EQ(vocab.upper_bound(biggerWord, comp), c);
  }

  {
    SearchResult c{words.size(), std::nullopt};
    ASSERT_EQ(vocab.upper_bound(words.back(), comp), c);
  }

  {
    SearchResult c{0, words.front()};
    ASSERT_EQ(vocab.upper_bound("3", comp), c);
  }
}

TEST(SimpleVocabulary, AccessOperator) {
  // Not in any particulary order
  const std::vector<std::string> words{"alpha", "delta", "beta", "42",
                                       "31",    "0",     "al"};
  const auto vocab = createVocabulary(words);
  vocabsEqual(vocab, words);
}

TEST(SimpleVocabulary, ReadAndWriteFromFile) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42",
                                       "31",    "0",     "al"};
  const auto vocab = createVocabulary(words);
  vocab.writeToFile("testvocab.dat");

  Vocab readVocab;
  readVocab.readFromFile("testvocab.dat");
  vocabsEqual(vocab, readVocab);
  ad_utility::deleteFile("testvocab.dat");
}
}  // namespace