// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)

#include <gtest/gtest.h>

#include <string>

#include "../src/util/TupleHelpers.h"

using namespace ad_tuple_helpers;
using namespace std::literals;

TEST(TupleHelpersTest, SetupFromCallable) {
  {
    auto f = [](const size_t x) { return std::to_string(x); };
    ASSERT_EQ(setupTupleFromCallable<3>(f), std::tuple("0"s, "1"s, "2"s));
    static_assert(
        std::is_same_v<decltype(setupTupleFromCallable<3>(f)),
                       std::tuple<std::string, std::string, std::string>>);
  }

  {
    // lambda that returns a lambda
    auto f = [](const size_t x) {
      return [x](const size_t y) { return static_cast<int>(x + y); };
    };
    auto tuple = setupTupleFromCallable<3>(f);
    ASSERT_EQ(std::get<0>(tuple)(3), 3);
    static_assert(std::is_same_v<decltype(std::get<0>(tuple)(4)), int>);
    ASSERT_EQ(std::get<1>(tuple)(3), 4);
    static_assert(std::is_same_v<decltype(std::get<1>(tuple)(4)), int>);
    ASSERT_EQ(std::get<2>(tuple)(3), 5);
    static_assert(std::is_same_v<decltype(std::get<2>(tuple)(4)), int>);
  }
  {
    // don't ask me why but this also works with generic lambdas in the tuple;
    auto f = [](const size_t x) {
      return [x](const auto& y) { return std::vector(x, y); };
    };
    auto tup = setupTupleFromCallable<3>(f);
    ASSERT_EQ(std::get<0>(tup)("kart"s), std::vector<std::string>());

    ASSERT_EQ(std::get<1>(tup)("kart"s), std::vector{"kart"s});
    ASSERT_EQ(std::get<2>(tup)("offel"s), std::vector({"offel"s, "offel"s}));
    static_assert(std::is_same_v<decltype(std::get<0>(tup)("kart"s)),
                                 std::vector<std::string>>);
    static_assert(std::is_same_v<decltype(std::get<1>(tup)("kart"s)),
                                 std::vector<std::string>>);
    static_assert(std::is_same_v<decltype(std::get<2>(tup)("kart"s)),
                                 std::vector<std::string>>);

    ASSERT_EQ(std::get<0>(tup)(3), std::vector<int>());
    ASSERT_EQ(std::get<1>(tup)(3), std::vector({3}));
    ASSERT_EQ(std::get<2>(tup)(3), std::vector({3, 3}));

    static_assert(
        std::is_same_v<decltype(std::get<0>(tup)(3)), std::vector<int>>);
    static_assert(
        std::is_same_v<decltype(std::get<1>(tup)(3)), std::vector<int>>);
    static_assert(
        std::is_same_v<decltype(std::get<2>(tup)(3)), std::vector<int>>);
  }
}

TEST(TupleHelpersTest, ToUniquePtrTuple) {
  {
    auto x = toUniquePtrTuple(3, "kartoffel"s, true);
    ASSERT_EQ(3, *std::get<0>(x));
    ASSERT_EQ("kartoffel"s, *std::get<1>(x));
    ASSERT_TRUE(*std::get<2>(x));
    static_assert(
        std::is_same_v<decltype(std::get<0>(x)), std::unique_ptr<int>&>);
    static_assert(std::is_same_v<decltype(std::get<1>(x)),
                                 std::unique_ptr<std::string>&>);
    static_assert(
        std::is_same_v<decltype(std::get<2>(x)), std::unique_ptr<bool>&>);
    static_assert(std::tuple_size_v<decltype(x)> == 3);
    static_assert(std::is_same_v<toUniquePtrTuple_t<int, std::string, bool>,
                                 decltype(x)>);
  }

  {
    // check if forwarding works correctly
    std::string a{"kartoffel"};
    std::string b{"salat"};
    auto x = toUniquePtrTuple(a, std::move(b));
    ASSERT_EQ(a, "kartoffel"s);
    ASSERT_TRUE(b.empty());  // I am not sure if this is implementation-defined,
                             // but this should hold on all sane platforms
    ASSERT_EQ("kartoffel"s, *std::get<0>(x));
    ASSERT_EQ("salat"s, *std::get<1>(x));
    static_assert(std::is_same_v<decltype(std::get<0>(x)),
                                 std::unique_ptr<std::string>&>);
    static_assert(std::is_same_v<decltype(std::get<1>(x)),
                                 std::unique_ptr<std::string>&>);
    static_assert(std::tuple_size_v<decltype(x)> == 2);
    static_assert(std::is_same_v<toUniquePtrTuple_t<std::string, std::string>,
                                 decltype(x)>);
  }
}

TEST(TupleHelpersTest, ToRawPtrTuple) {
  {
    auto x = std::tuple(std::make_unique<int>(3),
                        std::make_unique<std::string>("kartoffel"),
                        std::make_unique<bool>(false));
    auto y = toRawPtrTuple(x);

    // the following lines make a static assertion fail, since temporary tuples
    // are not allowed auto z = toRawPtrTuple(std::move(x)); auto a =
    // toRawPtrTuple(std::tuple(std::make_unique<int>(4)));

    static_assert(std::tuple_size_v<decltype(y)> == 3);
    ASSERT_EQ(std::get<0>(x).get(), std::get<0>(y));
    ASSERT_EQ(std::get<1>(x).get(), std::get<1>(y));
    ASSERT_EQ(std::get<2>(x).get(), std::get<2>(y));

    ASSERT_EQ(3, *std::get<0>(y));
    ASSERT_EQ("kartoffel"s, *std::get<1>(y));
    ASSERT_EQ(false, *std::get<2>(y));
    static_assert(
        std::is_same_v<decltype(y), std::tuple<int*, std::string*, bool*>>);
  }

  {
    auto x = std::tuple(std::make_unique<int>(3),
                        std::make_unique<std::string>("kartoffel"),
                        std::make_unique<bool>(false));
    auto y = toRawPtrTuple(x);

    static_assert(std::tuple_size_v<decltype(y)> == 3);
    ASSERT_EQ(std::get<0>(x).get(), std::get<0>(y));
    ASSERT_EQ(std::get<1>(x).get(), std::get<1>(y));
    ASSERT_EQ(std::get<2>(x).get(), std::get<2>(y));

    ASSERT_EQ(3, *std::get<0>(y));
    ASSERT_EQ("kartoffel"s, *std::get<1>(y));
    ASSERT_EQ(false, *std::get<2>(y));
    static_assert(
        std::is_same_v<decltype(y), std::tuple<int*, std::string*, bool*>>);
  }
}

TEST(TupleHelpersTest, IsTuple) {
  std::tuple<int, bool> a;
  std::tuple<int, std::string> b;
  class X {};
  std::tuple<X, X, std::vector<X>> c;
  ASSERT_TRUE(is_tuple_v<decltype(a)>);
  ASSERT_TRUE(is_tuple_v<decltype(b)>);
  ASSERT_TRUE(is_tuple_v<decltype(c)>);

  int d;
  std::string e;
  std::array<bool, 5> f;
  ASSERT_FALSE(is_tuple_v<decltype(d)>);
  ASSERT_FALSE(is_tuple_v<decltype(e)>);
  ASSERT_FALSE(is_tuple_v<decltype(f)>);
}
