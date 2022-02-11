//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <algorithm>

#include "../src/util/Iterators.h"

TEST(RandomAccessIterator, Iterator) {
  auto testIterator = [](const auto& input, auto begin, auto end) {
    auto it = begin;
    ASSERT_EQ(input[0], *it);
    ASSERT_EQ(input[0], *it++);
    ASSERT_EQ(input[1], *it);
    ASSERT_EQ(input[2], *++it);
    ASSERT_EQ(input[2], *it);
    ASSERT_EQ(input[2], *it--);
    ASSERT_EQ(input[1], *it);
    ASSERT_EQ(input[0], *--it);

    ASSERT_EQ(input[2], it[2]);
    ASSERT_EQ(input[2], *(it + 2));
    ASSERT_EQ(input[2], *(2 + it));
    ASSERT_EQ(input[3], *(it += 3));
    ASSERT_EQ(input[2], *(it += -1));
    ASSERT_EQ(input[2], *(it));
    ASSERT_EQ(input[0], *(it -= 2));
    ASSERT_EQ(input[2], *(it -= -2));
    ASSERT_EQ(input[2], *(it));

    it = end + (-1);
    ASSERT_EQ(input.back(), *it);

    ASSERT_EQ(static_cast<int64_t>(input.size()), end - begin);
  };

  std::vector<int> f{3, 62, 1023, -43817, 14, 42};
  using Iterator = ad_utility::IteratorForAccessOperator<
      std::vector<int>, ad_utility::AccessViaBracketOperator>;

  Iterator begin = Iterator{&f, 0};
  Iterator end = Iterator{&f, f.size()};

  testIterator(f, begin, end);
}
