// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <gmock/gmock.h>

#include <forward_list>
#include <list>
#include <vector>

#include "backports/shift.h"

// Tests for the ql::shift_left and ql::shift_right functions.
template <typename T>
class ShiftTest : public ::testing::Test {
 protected:
  void SetUp() override { data_ = {1, 2, 3, 4, 5, 6, 7, 8}; }

  T data_;
  using type = T;
};

using Types =
    ::testing::Types<std::vector<int>, std::list<int>, std::forward_list<int>>;
TYPED_TEST_SUITE(ShiftTest, Types);

// Given an input input (random access iterator) and a shift value,
// When shifting left by that value,
// Expect the elements to be correctly shifted.
TYPED_TEST(ShiftTest, ShiftLeftNormal) {
  auto& data = this->data_;
  const auto result = ql::shift_left(data.begin(), data.end(), 3);
  const std::vector<int> expected_data{4, 5, 6, 7, 8, 6, 7, 8};
  EXPECT_EQ(result, std::next(data.begin(), 5));
  EXPECT_THAT(data, ::testing::ElementsAreArray(expected_data));
}

// Given an input input and a shift value of zero,
// When shifting left by that value,
// Expect the input to remain unchanged.
TYPED_TEST(ShiftTest, ShiftVectorLeftZero) {
  auto& data = this->data_;
  const auto original = data;
  const auto result = ql::shift_left(data.begin(), data.end(), 0);
  EXPECT_EQ(result, data.begin());
  EXPECT_THAT(data, ::testing::ElementsAreArray(original));
}

// Given an input and a shift value equal to the size of the input,
// When shifting left by that value,
// Expect the input to remain unchanged.
TYPED_TEST(ShiftTest, ShiftLeftExactSize) {
  auto& data = this->data_;
  auto original = data;
  auto shift = ql::ranges::distance(data.begin(), data.end());
  const auto result = ql::shift_left(data.begin(), data.end(), shift);
  EXPECT_EQ(result, data.begin());
  EXPECT_THAT(data, ::testing::ElementsAreArray(original));
}

// Given an input and a shift value greater than the input size,
// When shifting left by that value,
// Expect the input to remain unchanged.
TYPED_TEST(ShiftTest, ShiftVectorLeftMoreThanSize) {
  auto& data = this->data_;
  auto original = data;
  auto shift = ql::ranges::distance(data.begin(), data.end()) + 5;
  const auto result = ql::shift_left(data.begin(), data.end(), shift);
  EXPECT_EQ(result, data.begin());
  EXPECT_THAT(data, ::testing::ElementsAreArray(original));
}

// Given an empty input and a shift value,
// When shifting left by that value,
// Expect no change and the begin iterator to be returned.
TYPED_TEST(ShiftTest, ShiftLeftEmptyRange) {
  decltype(this->data_) emptyData;
  const auto result = ql::shift_left(emptyData.begin(), emptyData.end(), 3);
  EXPECT_EQ(result, emptyData.begin());
}

// Given a vector and a partial range within it,
// When shifting left a portion of the input,
// Expect only the specified range to be shifted.
TYPED_TEST(ShiftTest, ShiftLeftPartialRange) {
  auto& data = this->data_;
  ql::shift_left(std::next(data.begin(), 2), std::next(data.begin(), 6), 2);
  const std::vector expected{1, 2, 5, 6, 5, 6, 7, 8};
  EXPECT_THAT(data, ::testing::ElementsAreArray(expected));
}

// Given an input (random access iterator) and a shift value,
// When shifting right by that value,
// Expect the elements to be correctly shifted.
TYPED_TEST(ShiftTest, ShiftRightNormal) {
  auto& data_ = this->data_;
  const auto result = ql::shift_right(data_.begin(), data_.end(), 3);
  EXPECT_EQ(result, std::next(data_.begin(), 3));
  // The first three elements are moved out, so they can have any value.
  // They are used as a temporary buffer when shifting forward ranges right.
  auto x = ::testing::_;
  EXPECT_THAT(data_, ::testing::ElementsAre(x, x, x, 1, 2, 3, 4, 5));
}

// Shift right by more than half of the size of the input. This covers a special
// optimization branch in the `shif_right` for `forward_iterators` that are not
// bidirectional.
TYPED_TEST(ShiftTest, ShiftRightLarge) {
  auto& data_ = this->data_;
  const auto result = ql::shift_right(data_.begin(), data_.end(), 6);
  EXPECT_EQ(result, std::next(data_.begin(), 6));
  // The first three elements are moved out, so they can have any value.
  // They are used as a temporary buffer when shifting forward ranges right.
  auto x = ::testing::_;
  EXPECT_THAT(data_, ::testing::ElementsAre(x, x, x, x, x, x, 1, 2));
}

// Given an input and a shift value of zero,
// When shifting right by that value,
// Expect the input to remain unchanged.
TYPED_TEST(ShiftTest, ShiftRightZero) {
  auto& data_ = this->data_;
  const auto original = data_;
  const auto result = ql::shift_right(data_.begin(), data_.end(), 0);
  EXPECT_EQ(result, data_.end());
  EXPECT_EQ(data_, original);
}

// Given an input and a shift value equal to the input size,
// When shifting right by that value,
// Expect the input to remain unchanged.
TYPED_TEST(ShiftTest, ShiftVectorRightExactSize) {
  auto& data_ = this->data_;
  const auto original = data_;
  auto size = ql::ranges::distance(data_.begin(), data_.end());
  const auto result = ql::shift_right(data_.begin(), data_.end(), size);
  EXPECT_EQ(result, data_.end());
  EXPECT_EQ(data_, original);
}

// Given an input and a shift value greater than the input size,
// When shifting right by that value,
// Expect the input to remain unchanged.
TYPED_TEST(ShiftTest, ShiftVectorRightMoreThanSize) {
  auto& data_ = this->data_;
  const auto original = data_;
  auto size = ql::ranges::distance(data_.begin(), data_.end());
  const auto result = ql::shift_right(data_.begin(), data_.end(), size + 5);
  EXPECT_EQ(result, data_.end());
  EXPECT_EQ(data_, original);
}

// Given an empty input and a shift value,
// When shifting right by that value,
// Expect no change and the end iterator to be returned.
TYPED_TEST(ShiftTest, ShiftVectorRightEmptyRange) {
  decltype(this->data_) empty_data_;
  const auto result =
      ql::shift_right(empty_data_.begin(), empty_data_.end(), 3);
  EXPECT_EQ(result, empty_data_.end());
}

// Given a input and a partial range within it,
// When shifting right a portion of the input,
// Expect only the specified range to be shifted.
TYPED_TEST(ShiftTest, ShiftVectorRightPartialRange) {
  auto& data_ = this->data_;
  ql::shift_right(std::next(data_.begin(), 2), std::next(data_.begin(), 6), 2);
  const std::vector<int> expected{1, 2, 3, 4, 3, 4, 7, 8};
  EXPECT_THAT(data_, ::testing::ElementsAreArray(expected));
}
