//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <iterator>

#include "../src/global/Pattern.h"

template <std::random_access_iterator>
using T = bool;

TEST(CompactStringVector, IteratorCategory) {
  using It = CompactStringVector<char, size_t>::Iterator;
  using X = T<It>;
  [[maybe_unused]] X x;
  ASSERT_TRUE(std::input_iterator<It>);
  ASSERT_TRUE(std::forward_iterator<It>);
}

TEST(CompactStringVector, ReadWrite) {
  std::vector<std::string> strings{"alpha", "beta", "gamma", "delta"};

  {
    CompactStringVectorWriter<size_t, char> w{"_writerTest.dat"};
    for (const auto& s : strings) {
      w.push(s.data(), s.size());
    }
  }  // The constructor finishes writing the file.

  CompactStringVector<size_t, char> v;
  ad_utility::serialization::FileReadSerializer ser{"_writerTest.dat"};
  ser >> v;
  ASSERT_EQ(v.size(), strings.size());

  for (size_t i = 0; i < v.size(); ++i) {
    ASSERT_EQ(v[i], strings[i]);
  }
}
