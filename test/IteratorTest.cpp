//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <algorithm>
#include <string>

#include "../src/util/Iterators.h"

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

TEST(RandomAccessIterator, Vector) {
  std::vector<int> f{3, 62, 1023, -43817, 14, 42};
  using Iterator = ad_utility::IteratorForAccessOperator<
      std::vector<int>, ad_utility::AccessViaBracketOperator,
      ad_utility::IsConst::False>;

  using ConstIterator = ad_utility::IteratorForAccessOperator<
      std::vector<int>, ad_utility::AccessViaBracketOperator,
      ad_utility::IsConst::True>;

  Iterator begin = Iterator{&f, 0};
  Iterator end = Iterator{&f, f.size()};
  testIterator(f, begin, end);

  ConstIterator cbegin{&f, 0};
  ConstIterator cend{&f, f.size()};
  testIterator(f, cbegin, cend);
}

TEST(RandomAccessIterator, DummyRandomAccessContainer) {
  struct TestRandomAccessContainer {
    uint64_t get(uint64_t index) const { return 42 * index; }

    // operator[] is required for the test lambda.
    uint64_t operator[](uint64_t index) const { return get(index); }

    [[nodiscard]] auto front() const { return get(0); }
    [[nodiscard]] auto back() const { return get(42); }
    [[nodiscard]] auto size() const { return 43; }
  };

  auto getFromTestContainer = [](const TestRandomAccessContainer& container,
                                 uint64_t i) { return container.get(i); };

  using Iterator =
      ad_utility::IteratorForAccessOperator<TestRandomAccessContainer,
                                            decltype(getFromTestContainer)>;
  TestRandomAccessContainer d;
  Iterator begin = Iterator{&d, 0};
  Iterator end = Iterator{&d, 43};
  testIterator(d, begin, end);
}

TEST(Iterator, makeForwardingIterator) {
  auto forwardFirstElement = []<typename T>(T&& vector) {
    return *ad_utility::makeForwardingIterator<T>(vector.begin());
  };

  std::vector<std::string> vector{"hello"};
  auto vector2 = vector;
  ASSERT_EQ("hello", forwardFirstElement(vector));
  // Nothing was moved.
  ASSERT_EQ(vector, vector2);

  ASSERT_EQ("hello", forwardFirstElement(std::move(vector)));
  // The first element in the vector was now moved from.
  ASSERT_EQ(1u, vector.size());
  ASSERT_TRUE(vector[0].empty());
}
