// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <gtest/gtest.h>

#include <forward_list>
#include <list>
#include <vector>

#include "backports/shift.h"

class ShiftTest : public ::testing::Test {
 protected:
  void SetUp() override {
    vec = {1, 2, 3, 4, 5, 6, 7, 8};
    list = {1, 2, 3, 4, 5, 6, 7, 8};
    fwd_list = {1, 2, 3, 4, 5, 6, 7, 8};
  }

  std::vector<int> vec;
  std::list<int> list;
  std::forward_list<int> fwd_list;
};

// Given an input vector (random access iterator) and a shift value,
// When shifting left by that value,
// Expect the elements to be correctly shifted.
TEST_F(ShiftTest, ShiftVectorLeftNormal) {
  const auto result = ql::shift_left(vec.begin(), vec.end(), 3);
  const std::vector<int> expected_vec{4, 5, 6, 7, 8, 6, 7, 8};
  EXPECT_EQ(result, std::next(vec.begin(), 5));
  EXPECT_EQ(vec, expected_vec);
}

// Given an input list (bidirectional iterator) and a shift value,
// When shifting left by that value,
// Expect the elements to be correctly shifted.
TEST_F(ShiftTest, ShiftListLeftNormal) {
  const auto result = ql::shift_left(list.begin(), list.end(), 2);
  const std::vector<int> result_list(list.begin(), list.end());
  const std::vector<int> expected_vec{3, 4, 5, 6, 7, 8, 7, 8};
  EXPECT_EQ(result, std::next(list.begin(), 6));
  EXPECT_EQ(result_list, expected_vec);
}

// Given an input vector and a shift value of zero,
// When shifting left by that value,
// Expect the vector to remain unchanged.
TEST_F(ShiftTest, ShiftVectorLeftZero) {
  const std::vector<int> original = vec;
  const auto result = ql::shift_left(vec.begin(), vec.end(), 0);
  EXPECT_EQ(result, vec.begin());
  EXPECT_EQ(vec, original);
}

// Given an input vector and a shift value equal to the vector size,
// When shifting left by that value,
// Expect the vector to remain unchanged.
TEST_F(ShiftTest, ShiftVectorLeftExactSize) {
  const std::vector<int> original = vec;
  const auto result = ql::shift_left(vec.begin(), vec.end(), vec.size());
  EXPECT_EQ(result, vec.begin());
  EXPECT_EQ(vec, original);
}

// Given an input vector and a shift value greater than the vector size,
// When shifting left by that value,
// Expect the vector to remain unchanged.
TEST_F(ShiftTest, ShiftVectorLeftMoreThanSize) {
  const std::vector<int> original = vec;
  const auto result = ql::shift_left(vec.begin(), vec.end(), vec.size() + 5);
  EXPECT_EQ(result, vec.begin());
  EXPECT_EQ(vec, original);
}

// Given an empty vector and a shift value,
// When shifting left by that value,
// Expect no change and the begin iterator to be returned.
TEST_F(ShiftTest, ShiftVectorLeftEmptyRange) {
  std::vector<int> empty_vec;
  const auto result = ql::shift_left(empty_vec.begin(), empty_vec.end(), 3);
  EXPECT_EQ(result, empty_vec.begin());
}

// Given a vector and a partial range within it,
// When shifting left a portion of the vector,
// Expect only the specified range to be shifted.
TEST_F(ShiftTest, ShiftVectorLeftPartialRange) {
  ql::shift_left(vec.begin() + 2, vec.begin() + 6, 2);
  const std::vector<int> expected{1, 2, 5, 6, 5, 6, 7, 8};
  EXPECT_EQ(vec, expected);
}

// Given an input vector (random access iterator) and a shift value,
// When shifting right by that value,
// Expect the elements to be correctly shifted.
TEST_F(ShiftTest, ShiftVectorRightNormal) {
  const auto result = ql::shift_right(vec.begin(), vec.end(), 3);
  EXPECT_EQ(result, vec.begin() + 3);
  const std::vector<int> expected_vec{1, 2, 3, 1, 2, 3, 4, 5};
  EXPECT_EQ(vec, expected_vec);
}

// Given an input list (bidirectional iterator) and a shift value,
// When shifting right by that value,
// Expect the elements to be correctly shifted.
TEST_F(ShiftTest, ShiftListRightNormal) {
  const auto result = ql::shift_right(list.begin(), list.end(), 3);
  EXPECT_EQ(result, std::next(list.begin(), 3));
  const std::vector<int> result_list(list.begin(), list.end());
  const std::vector<int> expected_vec{1, 2, 3, 1, 2, 3, 4, 5};
  EXPECT_EQ(result_list, expected_vec);
}

// Given an input forward_list (forward iterator) and a shift value,
// When shifting right by that value,
// Expect the elements to be correctly shifted.
TEST_F(ShiftTest, ShiftForwardListRightNormal) {
  ql::shift_right(fwd_list.begin(), std::next(fwd_list.begin(), 8), 3);
  std::vector<int> result_vec;
  for (const auto& i : fwd_list) {
    result_vec.push_back(i);
  }
  const std::vector<int> expected_vec{4, 5, 3, 1, 2, 3, 4, 5};
  EXPECT_EQ(result_vec, expected_vec);
}

// Given an input vector and a shift value of zero,
// When shifting right by that value,
// Expect the vector to remain unchanged.
TEST_F(ShiftTest, ShiftVectorRightZero) {
  const std::vector<int> original = vec;
  const auto result = ql::shift_right(vec.begin(), vec.end(), 0);
  EXPECT_EQ(result, vec.end());
  EXPECT_EQ(vec, original);
}

// Given an input vector and a shift value equal to the vector size,
// When shifting right by that value,
// Expect the vector to remain unchanged.
TEST_F(ShiftTest, ShiftVectorRightExactSize) {
  const std::vector<int> original = vec;
  const auto result = ql::shift_right(vec.begin(), vec.end(), vec.size());
  EXPECT_EQ(result, vec.end());
  EXPECT_EQ(vec, original);
}

// Given an input vector and a shift value greater than the vector size,
// When shifting right by that value,
// Expect the vector to remain unchanged.
TEST_F(ShiftTest, ShiftVectorRightMoreThanSize) {
  const std::vector<int> original = vec;
  const auto result = ql::shift_right(vec.begin(), vec.end(), vec.size() + 5);
  EXPECT_EQ(result, vec.end());
  EXPECT_EQ(vec, original);
}

// Given an empty vector and a shift value,
// When shifting right by that value,
// Expect no change and the end iterator to be returned.
TEST_F(ShiftTest, ShiftVectorRightEmptyRange) {
  std::vector<int> empty_vec;
  const auto result = ql::shift_right(empty_vec.begin(), empty_vec.end(), 3);
  EXPECT_EQ(result, empty_vec.end());
}

// Given a vector and a partial range within it,
// When shifting right a portion of the vector,
// Expect only the specified range to be shifted.
TEST_F(ShiftTest, ShiftVectorRightPartialRange) {
  ql::shift_right(vec.begin() + 2, vec.begin() + 6, 2);
  const std::vector<int> expected{1, 2, 3, 4, 3, 4, 7, 8};
  EXPECT_EQ(vec, expected);
}
