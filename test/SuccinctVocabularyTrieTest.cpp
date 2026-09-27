// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include "index/vocabulary/SuccinctVocabularyTrie.h"

using namespace ql::index::vocab;

// _____________________________________________________________________________
TEST(SuccinctVocabularyTrieTest, HardwarePopcntRank1) {
  SuccinctVocabularyTrie trie;

  // Bits: 0b1011 (11 in decimal) -> 3 set bits in first 4 positions
  std::vector<uint64_t> bits = {0b1011ULL, 0xFFULL};
  std::vector<char> labels = {'a', 'b', 'c'};
  trie.setMockTopology(bits, labels);

  EXPECT_EQ(trie.rank1(0), 0u);
  EXPECT_EQ(trie.rank1(1), 1u);
  EXPECT_EQ(trie.rank1(2), 2u);
  EXPECT_EQ(trie.rank1(3), 2u);
  EXPECT_EQ(trie.rank1(4), 3u);
  EXPECT_EQ(trie.rank1(64), 3u);
  EXPECT_EQ(trie.rank1(72), 11u);  // 3 from first word + 8 from second word
}

// _____________________________________________________________________________
TEST(SuccinctVocabularyTrieTest, Rank1EdgeCases) {
  // Without a topology every rank is zero.
  SuccinctVocabularyTrie empty;
  EXPECT_EQ(empty.rank1(0), 0u);
  EXPECT_EQ(empty.rank1(1000), 0u);
  EXPECT_EQ(empty.bitVectorSize(), 0u);

  SuccinctVocabularyTrie trie;
  std::vector<uint64_t> bits = {~0ULL, 0ULL, 1ULL << 63};
  trie.setMockTopology(bits, {'a'});
  EXPECT_EQ(trie.bitVectorSize(), 192u);
  EXPECT_EQ(trie.totalNodes(), 1u);
  // Last bit of a full word and the empty word after it.
  EXPECT_EQ(trie.rank1(63), 63u);
  EXPECT_EQ(trie.rank1(64), 64u);
  EXPECT_EQ(trie.rank1(128), 64u);
  // The only set bit of the third word is its highest one.
  EXPECT_EQ(trie.rank1(191), 64u);
  EXPECT_EQ(trie.rank1(192), 65u);
  // Positions beyond the bit vector count all set bits.
  EXPECT_EQ(trie.rank1(10'000), 65u);

  // Replacing the topology rebuilds the rank checkpoints.
  trie.setMockTopology({0b1ULL}, {'a'});
  EXPECT_EQ(trie.rank1(64), 1u);
  EXPECT_EQ(trie.rank1(192), 1u);
}
