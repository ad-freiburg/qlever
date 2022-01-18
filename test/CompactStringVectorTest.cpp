//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <iterator>

#include "../src/global/Pattern.h"

std::vector<std::string> strings{"alpha", "b", "3920193",
                                 "<Qlever-internal-langtag>"};
std::vector<std::vector<int>> ints{{1, 2, 3}, {42}, {6, 5, -4, 96}};

auto iterablesEqual(const auto& a, const auto& b) {
  ASSERT_EQ(a.size(), b.size());
  for (size_t i = 0; i < a.size(); ++i) {
    ASSERT_EQ(a[i], b[i]);
  }
}

static auto equalWithIndex = [](const auto& compactVec,
                                const auto& compareVec) {
  ASSERT_EQ(compactVec.size(), compareVec.size());
  for (size_t i = 0; i < compactVec.size(); ++i) {
    using value_type = typename std::decay_t<decltype(compareVec)>::value_type;
    value_type a(compactVec[i].begin(), compactVec[i].end());
    iterablesEqual(a, compareVec[i]);
  }
};

using StringVec = CompactStringVector<char>;
using IntVec = CompactStringVector<int>;

TEST(CompactStringVector, Build) {
  IntVec i;
  i.build(ints);
  equalWithIndex(i, ints);

  StringVec s;
  s.build(strings);
  equalWithIndex(s, strings);
}

TEST(CompactStringVector, Iterator) {
  StringVec s;
  s.build(strings);

  auto it = s.begin();
  ASSERT_EQ(strings[0], *it);
  ASSERT_EQ(strings[0], *it++);
  ASSERT_EQ(strings[1], *it);
  ASSERT_EQ(strings[2], *++it);
  ASSERT_EQ(strings[2], *it);
  ASSERT_EQ(strings[2], *it--);
  ASSERT_EQ(strings[1], *it);
  ASSERT_EQ(strings[0], *--it);

  ASSERT_EQ(strings[2], it[2]);
  ASSERT_EQ(strings[2], *(it + 2));
  ASSERT_EQ(strings[2], *(2 + it));
  ASSERT_EQ(strings[2], *(it += 2));
  ASSERT_EQ(strings[2], *(it));
  ASSERT_EQ(strings[0], *(it -= 2));
  ASSERT_EQ(strings[0], *(it));

  it = s.end() + (-1);
  ASSERT_EQ(strings.back(), *it);

  ASSERT_EQ(static_cast<int64_t>(s.size()), s.end() - s.begin());
}

TEST(CompactStringVector, IteratorCategory) {
  using It = CompactStringVector<char>::Iterator;
  static_assert(std::random_access_iterator<It>);
}

TEST(CompactStringVector, Serialization) {
  auto testForVec = [](const auto& vectorForType, auto& input) {
    using V = std::decay_t<decltype(vectorForType)>;
    {
      V vector;
      vector.build(input);
      ad_utility::serialization::FileWriteSerializer ser{"_writerTest.dat"};
      ser << vector;
    }  // The destructor finishes writing the file.

    V vector;
    ad_utility::serialization::FileReadSerializer ser{"_writerTest.dat"};
    ser >> vector;

    equalWithIndex(input, vector);

    ad_utility::deleteFile("_writerTest.dat");
  };

  testForVec(StringVec{}, strings);
  testForVec(IntVec{}, ints);
}

TEST(CompactStringVector, SerializationWithPush) {
  auto testForVec = [](const auto& vectorForType, auto& input) {
    using V = std::decay_t<decltype(vectorForType)>;
    {
      typename V::Writer w{"_writerTest.dat"};
      for (const auto& s : input) {
        w.push(s.data(), s.size());
      }
    }  // The constructor finishes writing the file.

    V vector;
    ad_utility::serialization::FileReadSerializer ser{"_writerTest.dat"};
    ser >> vector;

    equalWithIndex(input, vector);

    ad_utility::deleteFile("_writerTest.dat");
  };
  testForVec(StringVec{}, strings);
  testForVec(IntVec{}, ints);
}

TEST(CompactStringVector, DiskIterator) {
  auto testForVec = [](const auto& vectorForType, auto& input) {
    using V = std::decay_t<decltype(vectorForType)>;
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
  testForVec(StringVec{}, strings);
  testForVec(IntVec{}, ints);
};
