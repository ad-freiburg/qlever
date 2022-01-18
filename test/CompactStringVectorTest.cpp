//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <iterator>

#include "../src/global/Pattern.h"

std::vector<std::string> strings{"alpha", "b", "3920193",
                                 "<Qlever-internal-langtag>"};
std::vector<std::vector<int>> ints{{1, 2, 3}, {42}, {6, 5, -4, 96}};

auto equalWithIndex = [](const auto& compactVec, const auto& compareVec) {
  ASSERT_EQ(compactVec.size(), compareVec.size());
  for (size_t i = 0; i < compactVec.size(); ++i) {
    using value_type = typename std::decay_t<decltype(compareVec)>::value_type;
    value_type a(compactVec[i].begin(), compactVec[i].end());
    ASSERT_EQ(a, compareVec[i]);
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
  {
    StringVec w;
    w.build(strings);
    ad_utility::serialization::FileWriteSerializer ser{"_writerTest.dat"};
    ser << w;
  }  // The destructor finishes writing the file.

  CompactStringVector<char> v;
  ad_utility::serialization::FileReadSerializer ser{"_writerTest.dat"};
  ser >> v;

  equalWithIndex(strings, v);

  ad_utility::deleteFile("_writerTest.dat");
}

TEST(CompactStringVector, SerializationWithPush) {
  {
    CompactStringVectorWriter<char> w{"_writerTest.dat"};
    for (const auto& s : strings) {
      w.push(s.data(), s.size());
    }
  }  // The constructor finishes writing the file.

  CompactStringVector<char> v;
  ad_utility::serialization::FileReadSerializer ser{"_writerTest.dat"};
  ser >> v;

  equalWithIndex(strings, v);

  ad_utility::deleteFile("_writerTest.dat");
}

TEST(CompactStringVector, DiskIterator) {
  {
    CompactStringVectorWriter<char> w{"_writerTest.dat"};
    for (const auto& s : strings) {
      w.push(s.data(), s.size());
    }
  }  // The constructor finishes writing the file.

  auto iterator = CompactStringVectorDiskIterator<char>("_writerTest.dat");

  size_t i = 0;
  for (auto el : iterator) {
    ASSERT_EQ(el, strings[i++]);
  }
  ASSERT_EQ(i, strings.size());
}
