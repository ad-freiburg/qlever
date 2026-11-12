// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/hash/hash.h>
#include <absl/hash/hash_testing.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "backports/three_way_comparison.h"
#include "util/DefaultedHash.h"
#include "util/HashSet.h"

namespace {
// A simple class with several members of different types, some of them private.
class Person {
  std::string name_;
  int age_;

 public:
  std::vector<int> numbers_;

  Person(std::string name, int age, std::vector<int> numbers)
      : name_{std::move(name)}, age_{age}, numbers_{std::move(numbers)} {}

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(Person, name_, age_, numbers_)
  QL_DEFINE_DEFAULTED_HASH(Person, name_, age_, numbers_)
};

// A class template with a single member.
template <typename T>
struct Wrapper {
  T value_;

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(Wrapper, value_)
  QL_DEFINE_DEFAULTED_HASH(Wrapper, value_)
};

// A class for which only some of the members are hashed.
struct PartiallyHashed {
  int a_;
  int notHashed1_;
  std::string b_;
  int notHashed2_;

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(PartiallyHashed, a_, notHashed1_,
                                              b_, notHashed2_)
  QL_DEFINE_DEFAULTED_HASH_OF_SUBSET(PartiallyHashed, QL_HASHED(a_, b_),
                                     QL_NOT_HASHED(notHashed1_, notHashed2_))
};

// A class that is implicitly convertible to a `Wrapper<int>`, and a class that
// is derived from it. Both must not be hashable via the `Wrapper<int>`.
struct ConvertibleToWrapper {
  operator Wrapper<int>() const { return {0}; }
};
struct DerivedFromWrapper : Wrapper<int> {
  int additionalValue_;
};

// True iff `T` can be hashed by `absl::Hash`.
template <typename T, typename = void>
constexpr bool isHashable = false;
template <typename T>
constexpr bool isHashable<
    T, std::void_t<decltype(absl::Hash<T>{}(std::declval<const T&>()))>> = true;

// _____________________________________________________________________________
TEST(DefaultedHash, HashIsConsistentWithEquality) {
  // The following call checks that equal objects have equal hashes, and that
  // the hashes of unequal objects differ (with very high probability). The
  // objects differ in exactly one member, such that each member is covered.
  EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly({
      Person{"alice", 30, {1, 2}},
      Person{"alice", 30, {1, 2}},
      Person{"bob", 30, {1, 2}},
      Person{"alice", 31, {1, 2}},
      Person{"alice", 30, {1, 3}},
      Person{"", 0, {}},
  }));
  EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly({
      Wrapper<std::string>{"a"},
      Wrapper<std::string>{"a"},
      Wrapper<std::string>{"b"},
      Wrapper<std::string>{""},
  }));

  // The hash is exactly the combination of the hashes of the members.
  EXPECT_EQ(absl::HashOf(Person{"alice", 30, {1, 2}}),
            absl::HashOf(std::string{"alice"}, 30, std::vector<int>{1, 2}));
  EXPECT_EQ(absl::HashOf(Wrapper<int>{42}), absl::HashOf(42));
}

// _____________________________________________________________________________
TEST(DefaultedHash, HashOfSubset) {
  // The hash is exactly the combination of the hashes of the hashed members,
  // so objects that only differ in the other members have the same hash, but
  // are not equal.
  PartiallyHashed x{1, 2, "b", 3};
  PartiallyHashed y{1, 4, "b", 5};
  EXPECT_EQ(absl::HashOf(x), absl::HashOf(1, std::string{"b"}));
  EXPECT_EQ(absl::HashOf(x), absl::HashOf(y));
  EXPECT_NE(x, y);
  EXPECT_NE(absl::HashOf(x), absl::HashOf(PartiallyHashed{2, 2, "b", 3}));
  EXPECT_NE(absl::HashOf(x), absl::HashOf(PartiallyHashed{1, 2, "c", 3}));

  ad_utility::HashSet<PartiallyHashed> set{x, y, x};
  EXPECT_EQ(set.size(), 2u);
}

// _____________________________________________________________________________
TEST(DefaultedHash, UsableInHashSet) {
  ad_utility::HashSet<Person> set;
  set.insert(Person{"alice", 30, {1, 2}});
  set.insert(Person{"alice", 30, {1, 2}});
  set.insert(Person{"bob", 30, {1, 2}});
  EXPECT_EQ(set.size(), 2u);
  EXPECT_TRUE(set.contains(Person{"bob", 30, {1, 2}}));
  EXPECT_FALSE(set.contains(Person{"bob", 30, {}}));
}

// Test that a class is only hashable as itself.
TEST(DefaultedHash, NoImplicitConversions) {
  static_assert(isHashable<Person>);
  static_assert(isHashable<Wrapper<int>>);
  static_assert(isHashable<Wrapper<std::string>>);
  static_assert(!isHashable<ConvertibleToWrapper>);
  static_assert(!isHashable<DerivedFromWrapper>);
}

#ifdef QLEVER_HAS_REFLECTION
// Classes for which a `QL_DEFINE_DEFAULTED_HASH` would be invalid.
struct ThreeMembers {
  int a_;
  int b_;
  int c_;
};
struct Derived : ThreeMembers {
  int d_;
};

// Test the compile-time check of the listed members, which only exists with
// C++26 reflection.
TEST(DefaultedHash, CheckHashedMembers) {
  using ad_utility::defaultedHash::checkHashedMembers;
  using ::testing::AllOf;
  using ::testing::HasSubstr;
  static_assert(checkHashedMembers<ThreeMembers>("a_, b_, c_", "").empty());
  // The whitespace is irrelevant.
  static_assert(checkHashedMembers<ThreeMembers>("a_,b_ ,\n  c_", "").empty());
  static_assert(
      checkHashedMembers<Person>("name_, age_, numbers_", "").empty());

  auto expectInvalid = [](std::string_view message, std::string_view listed) {
    EXPECT_THAT(message,
                AllOf(HasSubstr("ThreeMembers` is invalid: The listed members "
                                "`"),
                      HasSubstr(listed),
                      HasSubstr("` are not exactly the data members of the "
                                "class in declaration order, which are `a_, "
                                "b_, c_`")));
  };
  // A missing member, a wrong order, and an additional member.
  expectInvalid(checkHashedMembers<ThreeMembers>("a_, c_", ""), "a_, c_");
  expectInvalid(checkHashedMembers<ThreeMembers>("a_, c_, b_", ""),
                "a_, c_, b_");
  expectInvalid(checkHashedMembers<ThreeMembers>("a_, b_, c_, d_", ""),
                "a_, b_, c_, d_");

  EXPECT_THAT(checkHashedMembers<Derived>("d_", ""),
              HasSubstr("The class has base classes, which is not supported"));

  // The same for `QL_DEFINE_DEFAULTED_HASH_OF_SUBSET`. All the ways to
  // distribute the members over the two lists are valid, as long as each list
  // is in declaration order.
  static_assert(checkHashedMembers<ThreeMembers>("a_, c_", "b_").empty());
  static_assert(checkHashedMembers<ThreeMembers>("b_", "a_,c_").empty());
  static_assert(checkHashedMembers<ThreeMembers>("a_", " b_ , c_ ").empty());
  static_assert(
      checkHashedMembers<PartiallyHashed>("a_, b_", "notHashed1_, notHashed2_")
          .empty());
  auto expectInvalidSubset = [](std::string_view message,
                                std::string_view hashed,
                                std::string_view notHashed) {
    EXPECT_THAT(
        message,
        AllOf(HasSubstr("ThreeMembers` is invalid: The hashed members "
                        "`"),
              HasSubstr(hashed), HasSubstr("` and the not hashed members `"),
              HasSubstr(notHashed),
              HasSubstr("` have to be in declaration order each, and "
                        "together have to be exactly the data members "
                        "of the class, which are `a_, b_, c_`")));
  };
  // A member that is in none of the lists (the typical case of a newly added
  // member), a member that is in both lists, a wrong order in one of the
  // lists, and a member that doesn't exist.
  expectInvalidSubset(checkHashedMembers<ThreeMembers>("a_", "b_"), "a_", "b_");
  expectInvalidSubset(checkHashedMembers<ThreeMembers>("a_, b_", "b_, c_"),
                      "a_, b_", "b_, c_");
  expectInvalidSubset(checkHashedMembers<ThreeMembers>("c_, a_", "b_"),
                      "c_, a_", "b_");
  expectInvalidSubset(checkHashedMembers<ThreeMembers>("a_, b_, c_", "d_"),
                      "a_, b_, c_", "d_");
}
#endif
}  // namespace
