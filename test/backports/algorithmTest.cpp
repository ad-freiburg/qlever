//
// Created by kalmbacj on 12/6/24.
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <gtest/gtest.h>

#include <vector>

#include "backports/algorithm.h"

TEST(Range, Sort) {}

class AlgorithmBackportTest : public ::testing::Test {
 protected:
  void SetUp() override { testVector = {5, 3, 8, 1, 2, 7, 4, 6}; }
  std::vector<int> testVector;
};

// _____________________________________________________________________________
TEST_F(AlgorithmBackportTest, EraseSingleValue) {
  EXPECT_EQ(ql::backports::erase(testVector, 4), 1);
  std::vector<int> expected{5, 3, 8, 1, 2, 7, 6};
  EXPECT_EQ(testVector, expected);
}

// _____________________________________________________________________________
TEST_F(AlgorithmBackportTest, EraseIfRemovesCorrectElements) {
  auto isEven = [](int x) { return x % 2 == 0; };
  EXPECT_EQ(ql::backports::erase_if(testVector, isEven), 4);
  std::vector<int> expected{5, 3, 1, 7};
  EXPECT_EQ(testVector, expected);
}
