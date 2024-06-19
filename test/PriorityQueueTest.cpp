// Copyright 2020, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)

#include <gtest/gtest.h>

#include <string>

#include "../src/util/Cache.h"

using ad_utility::HeapBasedPQ;
using ad_utility::TreeBasedPQ;
using std::string;
using namespace std::literals;

TEST(PqTest, TreeSimple) {
  auto test = [](auto& pq) {
    ASSERT_EQ(pq.size(), 0u);
    auto h = pq.insert(42, "sense");
    ASSERT_EQ(pq.size(), 1u);
    ASSERT_EQ(42, h.score());
    ASSERT_EQ("sense"s, h.value());
    h = pq.pop();
    ASSERT_EQ(pq.size(), 0u);
    ASSERT_EQ(42, h.score());
    ASSERT_EQ("sense"s, h.value());
  };
  TreeBasedPQ<int, string> pq;
  test(pq);
  HeapBasedPQ<int, string> pq2;
  test(pq2);
}

TEST(PqTest, TreeInsertPop) {
  auto test = [](auto& pq) {
    std::vector<std::pair<int, string>> v{{3, "3"},    {2, "2"}, {7, "7"},
                                          {5, "5"},    {1, "1"}, {512, "512"},
                                          {-42, "-42"}};
    // insert a bunch of elements and check that all the handles and sizes match
    for (size_t i = 0; i < v.size(); ++i) {
      auto h = pq.insert(v[i].first, v[i].second);
      ASSERT_EQ(pq.size(), i + 1);
      ASSERT_EQ(v[i].first, h.score());
      ASSERT_EQ(v[i].second, h.value());
    }

    // basically check that we can use our PQ to implement heap sort
    std::sort(v.begin(), v.end());
    for (size_t i = 0; i < v.size(); ++i) {
      auto h = pq.pop();
      ASSERT_EQ(pq.size(), v.size() - (i + 1));
      ASSERT_EQ(v[i].first, h.score());
      ASSERT_EQ(v[i].second, h.value());
    }
  };
  TreeBasedPQ<int, string> pq;
  test(pq);
  HeapBasedPQ<int, string> pq2;
  test(pq2);
}

TEST(PqTest, TreeUpdateKey) {
  auto test = [](auto& pq) {
    auto h1 = pq.insert(1, "alpha");
    auto h2 = pq.insert(2, "beta");
    ASSERT_EQ(pq.size(), 2ul);
    pq.updateKey(-42, &h2);
    ASSERT_EQ(pq.size(), 2ul);
    ASSERT_EQ(h2.score(), -42);
    ASSERT_EQ(h2.value(), "beta");

    // we should now pop h2 before h1
    auto hPop = pq.pop();
    ASSERT_EQ(pq.size(), 1ul);
    ASSERT_EQ(hPop.score(), -42);
    ASSERT_EQ(hPop.value(), "beta");

    pq.updateKey(12, &h1);
    ASSERT_EQ(pq.size(), 1ul);

    hPop = pq.pop();
    ASSERT_EQ(pq.size(), 0ul);
    ASSERT_EQ(hPop.score(), 12);
    ASSERT_EQ(hPop.value(), "alpha");
  };
  TreeBasedPQ<int, string> pq;
  test(pq);
  HeapBasedPQ<int, string> pq2;
  test(pq2);
}

TEST(PqTest, TreeUpdateReinsert) {
  auto test = [](auto& pq) {
    pq.insert(1, "alpha"s);
    auto h = pq.pop();
    ASSERT_EQ(pq.size(), 0ul);
    ASSERT_THROW(pq.updateKey(15, &h), ad_utility::NotInPQException);
    ASSERT_EQ(pq.size(), 0ul);
    auto h2 = pq.insert(500, "alot"s);  // codespell-ignore
    ASSERT_EQ(pq.size(), 1ul);
    h = pq.pop();
    ASSERT_EQ(h.score(), 500);
    ASSERT_EQ(h.value(), "alot"s);  // codespell-ignore
  };
  TreeBasedPQ<int, string> pq;
  test(pq);
  HeapBasedPQ<int, string> pq2;
  test(pq2);
}

TEST(PqTest, TreeErase) {
  auto test = [](auto& pq, auto TT) {
    auto h3 = pq.insert(3, "gamma"s);
    auto h = pq.insert(1, "alpha"s);
    auto h2 = pq.insert(2, "beta"s);
    ASSERT_EQ(pq.size(), 3ul);
    if constexpr (decltype(TT)::value) {
      ASSERT_TRUE(pq.contains(h2));
    }
    pq.erase(std::move(h2));
    if constexpr (decltype(TT)::value) {
      ASSERT_FALSE(pq.contains(h2));
    }
    ASSERT_EQ(pq.size(), 2ul);
    auto hPop = pq.pop();
    ASSERT_EQ(pq.size(), 1ul);
    ASSERT_EQ(hPop.score(), 1);
    ASSERT_EQ(hPop.value(), "alpha"s);

    hPop = pq.pop();
    ASSERT_EQ(pq.size(), 0ul);
    ASSERT_EQ(hPop.score(), 3);
    ASSERT_EQ(hPop.value(), "gamma"s);
  };
  TreeBasedPQ<int, string> pq;
  test(pq, std::true_type{});
  HeapBasedPQ<int, string> pq2;
  test(pq2, std::false_type{});
}

TEST(PqTest, TreeEraseUpdate) {
  auto test = [](auto& pq, auto TT) {
    auto h3 = pq.insert(3, "gamma"s);
    auto h = pq.insert(1, "alpha"s);
    auto h2 = pq.insert(2, "beta"s);
    ASSERT_EQ(pq.size(), 3ul);
    if constexpr (decltype(TT)::value) {
      ASSERT_TRUE(pq.contains(h2));
    }
    pq.erase(std::move(h2));
    if constexpr (decltype(TT)::value) {
      ASSERT_FALSE(pq.contains(h2));
    }
    ASSERT_EQ(pq.size(), 2ul);
    pq.updateKey(42, &h);
    auto hPop = pq.pop();
    ASSERT_EQ(pq.size(), 1ul);
    ASSERT_EQ(hPop.score(), 3);
    ASSERT_EQ(hPop.value(), "gamma"s);

    hPop = pq.pop();
    ASSERT_EQ(pq.size(), 0ul);
    ASSERT_EQ(hPop.score(), 42);
    ASSERT_EQ(hPop.value(), "alpha"s);
  };
  TreeBasedPQ<int, string> pq;
  test(pq, std::true_type{});
  HeapBasedPQ<int, string> pq2;
  test(pq2, std::false_type{});
}

TEST(PqTest, TreeClear) {
  auto test = [](auto& pq) {
    pq.insert(3, "bim");
    pq.insert(4, "bim");
    pq.insert(5, "bam");
    pq.insert(3, "bim");
    pq.insert(-3, "bim");
    ASSERT_EQ(pq.size(), 5ul);
    pq.clear();
    ASSERT_EQ(pq.size(), 0ul);
    ASSERT_THROW(pq.pop(), ad_utility::EmptyPopException);
  };
  TreeBasedPQ<int, string> pq;
  test(pq);
  HeapBasedPQ<int, string> pq2;
  test(pq2);
}

TEST(PqTest, Simple) {
  ad_utility::HeapBasedPQ<int, std::string, std::less<>> pq{std::less<>()};
  pq.insert(3, "hello");
  auto handle = pq.insert(2, "bye");
  handle = pq.insert(37, " 37");
  pq.updateKey(1, &handle);
  handle = pq.pop();
  ASSERT_EQ(1, handle.score());
  ASSERT_EQ(" 37", handle.value());

  handle = pq.pop();
  ASSERT_EQ(2, handle.score());
  ASSERT_EQ("bye", handle.value());
  handle = pq.pop();
  ASSERT_EQ(3, handle.score());
  ASSERT_EQ("hello", handle.value());
  ASSERT_EQ(0ul, pq.size());
}

TEST(PqTest, MapBased) {
  ad_utility::TreeBasedPQ<int, std::string, std::less<>> pq{std::less<>()};
  pq.insert(3, "hello");
  auto handle = pq.insert(2, "bye");
  handle = pq.insert(37, " 37");
  pq.updateKey(1, &handle);
  handle = pq.pop();
  ASSERT_EQ(1, handle.mScore);
  ASSERT_EQ(" 37", *handle.mValue);

  handle = pq.pop();
  ASSERT_EQ(2, handle.mScore);
  ASSERT_EQ("bye", *handle.mValue);
  handle = pq.pop();
  ASSERT_EQ(3, handle.mScore);
  ASSERT_EQ("hello", *handle.mValue);
  ASSERT_EQ(0u, pq.size());
}
