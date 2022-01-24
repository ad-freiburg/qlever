//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <algorithm>
#include <iterator>

#include "../src/global/Pattern.h"

std::vector<std::string> strings{"alpha", "b", "3920193",
                                 "<Qlever-internal-langtag>"};
std::vector<std::vector<int>> ints{{1, 2, 3}, {42}, {6, 5, -4, 96}, {-38, 0}};

auto iterablesEqual(const auto& a, const auto& b) {
  ASSERT_EQ(a.size(), b.size());
  for (size_t i = 0; i < a.size(); ++i) {
    ASSERT_EQ(a[i], b[i]);
  }
}

static auto vectorsEqual = [](const auto& compactVector,
                              const auto& compareVector) {
  ASSERT_EQ(compactVector.size(), compareVector.size());
  for (size_t i = 0; i < compactVector.size(); ++i) {
    using value_type =
        typename std::decay_t<decltype(compareVector)>::value_type;
    value_type a(compactVector[i].begin(), compactVector[i].end());
    iterablesEqual(a, compareVector[i]);
  }
};

using CompactVectorChar = CompactVectorOfStrings<char>;
using CompactVectorInt = CompactVectorOfStrings<int>;

TEST(CompactVectorOfStrings, Build) {
  CompactVectorInt i;
  i.build(ints);
  vectorsEqual(i, ints);

  CompactVectorChar s;
  s.build(strings);
  vectorsEqual(s, strings);
}

TEST(CompactVectorOfStrings, Iterator) {
  auto testIterator = []<typename V>(const V&, const auto& input) {
    V s;
    s.build(input);

    auto it = s.begin();
    using std::ranges::equal;
    ASSERT_TRUE(equal(input[0], *it));
    ASSERT_TRUE(equal(input[0], *it++));
    ASSERT_TRUE(equal(input[1], *it));
    ASSERT_TRUE(equal(input[2], *++it));
    ASSERT_TRUE(equal(input[2], *it));
    ASSERT_TRUE(equal(input[2], *it--));
    ASSERT_TRUE(equal(input[1], *it));
    ASSERT_TRUE(equal(input[0], *--it));

    ASSERT_TRUE(equal(input[2], it[2]));
    ASSERT_TRUE(equal(input[2], *(it + 2)));
    ASSERT_TRUE(equal(input[2], *(2 + it)));
    ASSERT_TRUE(equal(input[3], *(it += 3)));
    ASSERT_TRUE(equal(input[2], *(it += -1)));
    ASSERT_TRUE(equal(input[2], *(it)));
    ASSERT_TRUE(equal(input[0], *(it -= 2)));
    ASSERT_TRUE(equal(input[2], *(it -= -2)));
    ASSERT_TRUE(equal(input[2], *(it)));

    it = s.end() + (-1);
    ASSERT_TRUE(equal(input.back(), *it));

    ASSERT_EQ(static_cast<int64_t>(s.size()), s.end() - s.begin());
  };
  testIterator(CompactVectorChar{}, strings);
  testIterator(CompactVectorInt{}, ints);
}

TEST(CompactVectorOfStrings, IteratorCategory) {
  using It = CompactVectorOfStrings<char>::Iterator;
  static_assert(std::random_access_iterator<It>);
}

TEST(CompactVectorOfStrings, Serialization) {
  auto testSerialization = []<typename V>(const V&, auto& input) {
    {
      V vector;
      vector.build(input);
      ad_utility::serialization::FileWriteSerializer ser{"_writerTest.dat"};
      ser << vector;
    }  // The destructor finishes writing the file.

    V vector;
    ad_utility::serialization::FileReadSerializer ser{"_writerTest.dat"};
    ser >> vector;

    vectorsEqual(input, vector);

    ad_utility::deleteFile("_writerTest.dat");
  };

  testSerialization(CompactVectorChar{}, strings);
  testSerialization(CompactVectorInt{}, ints);
}

TEST(CompactVectorOfStrings, SerializationWithPush) {
  auto testSerializationWithPush = []<typename V>(const V&, auto& input) {
    {
      typename V::Writer w{"_writerTest.dat"};
      for (const auto& s : input) {
        w.push(s.data(), s.size());
      }
    }  // The constructor finishes writing the file.

    V vector;
    ad_utility::serialization::FileReadSerializer ser{"_writerTest.dat"};
    ser >> vector;

    vectorsEqual(input, vector);

    ad_utility::deleteFile("_writerTest.dat");
  };
  testSerializationWithPush(CompactVectorChar{}, strings);
  testSerializationWithPush(CompactVectorInt{}, ints);
}

TEST(CompactVectorOfStrings, DiskIterator) {
  auto testDiskIterator = []<typename V>(const V&, auto& input) {
    {
      typename V::Writer w{"_writerTest.dat"};
      for (const auto& s : input) {
        w.push(s.data(), s.size());
      }
    }  // The constructor finishes writing the file.

    auto iterator = V::diskIterator("_writerTest.dat");

    size_t i = 0;
    for (const auto& el : iterator) {
      ASSERT_EQ(el, input[i++]);
    }
    ASSERT_EQ(i, input.size());
    ad_utility::deleteFile("_writerTest.dat");
  };
  testDiskIterator(CompactVectorChar{}, strings);
  testDiskIterator(CompactVectorInt{}, ints);
};
