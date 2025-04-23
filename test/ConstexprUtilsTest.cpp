//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <utility>

#include "gtest/gtest.h"
#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/GTestHelpers.h"
#include "util/TypeTraits.h"

using namespace ad_utility;

TEST(ConstexprUtils, pow) {
  static_assert(pow(0, 0) == 1);
  static_assert(pow(0.0, 0) == 1);
  static_assert(pow(0, 1) == 0);
  static_assert(pow(0.0, 1) == 0);
  static_assert(pow(0, 15) == 0);
  static_assert(pow(0.0, 15) == 0);
  static_assert(pow(1, 0) == 1);
  static_assert(pow(1.0, 0) == 1);
  static_assert(pow(15, 0) == 1);
  static_assert(pow(15.0, 0) == 1);

  static_assert(pow(1, 12) == 1);
  static_assert(pow(1.0, 12) == 1);

  static_assert(pow(2, 10) == 1024);
  static_assert(pow(1.5, 4) == 1.5 * 1.5 * 1.5 * 1.5);

  static_assert(pow(-1, 2) == 1);
  static_assert(pow(-1, 3) == -1);

  static_assert(pow(-2, 2) == 4);
  static_assert(pow(-2, 3) == -8);
}

// Easier testing for equality of `ValueSequence`. They must have the
// same underlying type and the same values.
// Note: we cannot call this `operator==` because the operator would only be
// found by argument-dependent lookup if it was in the `std::` namespace, and
// adding functions there is by default forbidden.
template <typename T, typename U, T... Ts, U... Us>
bool compare(const ValueSequence<T, Ts...>&, const ValueSequence<U, Us...>&) {
  if constexpr (std::is_same_v<U, T> && sizeof...(Us) == sizeof...(Ts)) {
    return std::array<T, sizeof...(Ts)>{Ts...} ==
           std::array<U, sizeof...(Us)>{Us...};
  } else {
    return false;
  }
}

TEST(ConstexprUtils, toIntegerSequence) {
  ASSERT_TRUE(compare(ValueSequence<int>{},
                      (toIntegerSequence<std::array<int, 0>{}>())));
  ASSERT_TRUE(compare(ValueSequence<int, 3, 2>{},
                      (toIntegerSequence<std::array{3, 2}>())));
  ASSERT_TRUE(compare(ValueSequence<int, -12>{},
                      (toIntegerSequence<std::array{-12}>())));
  ASSERT_TRUE(compare(ValueSequence<int, 5, 3, 3, 4, -1>{},
                      (toIntegerSequence<std::array{5, 3, 3, 4, -1}>())));

  // Mismatching types.
  ASSERT_FALSE(compare(ValueSequence<float>{},
                       (toIntegerSequence<std::array<int, 0>{}>())));
  ASSERT_FALSE(compare(ValueSequence<unsigned, 5, 4>{},
                       (toIntegerSequence<std::array{5, 4}>())));

  // Mismatching values
  ASSERT_FALSE(compare(ValueSequence<int, 3, 2>{},
                       (toIntegerSequence<std::array{3, 3}>())));
  ASSERT_FALSE(compare(ValueSequence<int, -12>{},
                       (toIntegerSequence<std::array{-12, 4}>())));
  ASSERT_FALSE(compare(ValueSequence<int, -12, 4>{},
                       (toIntegerSequence<std::array{-12}>())));
}

TEST(ConstexprUtils, cartesianPowerAsArray) {
  std::array<std::array<int, 1>, 4> a{std::array{0}, std::array{1},
                                      std::array{2}, std::array{3}};
  ASSERT_EQ(a, (cartesianPowerAsArray<4, 1>()));

  std::array<std::array<int, 2>, 4> b{std::array{0, 0}, std::array{0, 1},
                                      std::array{1, 0}, std::array{1, 1}};
  ASSERT_EQ(b, (cartesianPowerAsArray<2, 2>()));

  std::array<std::array<int, 3>, 8> c{std::array{0, 0, 0}, std::array{0, 0, 1},
                                      std::array{0, 1, 0}, std::array{0, 1, 1},
                                      std::array{1, 0, 0}, std::array{1, 0, 1},
                                      std::array{1, 1, 0}, std::array{1, 1, 1}};
  ASSERT_EQ(c, (cartesianPowerAsArray<2, 3>()));
}

TEST(ConstexprUtils, cartesianPowerAsIntegerArray) {
  ValueSequence<std::array<int, 1>, std::array{0}, std::array{1}, std::array{2},
                std::array{3}>
      a;
  ASSERT_TRUE(compare(a, (cartesianPowerAsIntegerArray<4, 1>())));

  ValueSequence<std::array<int, 2>, std::array{0, 0}, std::array{0, 1},
                std::array{1, 0}, std::array{1, 1}>
      b;
  ASSERT_TRUE(compare(b, (cartesianPowerAsIntegerArray<2, 2>())));

  ValueSequence<std::array<int, 3>, std::array{0, 0, 0}, std::array{0, 0, 1},
                std::array{0, 1, 0}, std::array{0, 1, 1}, std::array{1, 0, 0},
                std::array{1, 0, 1}, std::array{1, 1, 0}, std::array{1, 1, 1}>
      c;
  ASSERT_TRUE(compare(c, (cartesianPowerAsIntegerArray<2, 3>())));
}

TEST(ConstexprUtils, ConstexprForLoop) {
  size_t i{0};

  // Add `i` up to one hundred.
  ConstexprForLoop(std::make_index_sequence<100>{}, [&i]<size_t>() { i++; });
  ASSERT_EQ(i, 100);

  // Add up 2, 5, and 9 at run time.
  i = 0;
  ConstexprForLoop(std::index_sequence<2, 5, 9>{},
                   [&i]<size_t NumberToAdd>() { i += NumberToAdd; });
  ASSERT_EQ(i, 16);

  // Shouldn't do anything, because the index sequence is empty.
  i = 0;
  ConstexprForLoop(std::index_sequence<>{},
                   [&i]<size_t NumberToAdd>() { i += NumberToAdd; });
  ASSERT_EQ(i, 0);
}

TEST(ConstexprUtils, RuntimeValueToCompileTimeValue) {
  // Create one function, that sets `i` to x, for every possible
  // version of x in [0,100].
  size_t i = 1;
  auto setI = [&i]<size_t Number>() { i = Number; };
  for (size_t d = 0; d <= 100; d++) {
    RuntimeValueToCompileTimeValue<100>(d, setI);
    ASSERT_EQ(i, d);
  }

  // Should cause an exception, if the given value is bigger than the
  // `MaxValue`.
  ASSERT_ANY_THROW(RuntimeValueToCompileTimeValue<5>(10, setI));
}

// A helper struct for the following test.
struct F1 {
  CPP_template(int i)(requires(i == 0 || i == 1)) int operator()() const {
    return i + 1;
  }
};
// _____________________________________________________________
TEST(ConstexprUtils, ConstexprSwitch) {
  using namespace ad_utility;
  {
    auto f = []<int i> { return i * 2; };
    ASSERT_EQ((ConstexprSwitch<1, 2, 3, 5>{}(f, 2)), 4);
    ASSERT_EQ((ConstexprSwitch<1, 2, 3, 5>{}(f, 5)), 10);
    ASSERT_ANY_THROW((ConstexprSwitch<1, 2, 3, 5>{}(f, 4)));
  }
  {
    auto f = []<int i>(int j) { return i * j; };
    ASSERT_EQ((ConstexprSwitch<1, 2, 3, 5>{}(f, 2, 7)), 14);
    ASSERT_EQ((ConstexprSwitch<1, 2, 3, 5>{}(f, 5, 2)), 10);
    ASSERT_ANY_THROW((ConstexprSwitch<1, 2, 3, 5>{}(f, 4, 3)));
  }

  // F1 can only be called with 0 and 1 as template arguments.
  static_assert(std::invocable<decltype(ConstexprSwitch<0, 1>{}), F1, int>);
  static_assert(!std::invocable<decltype(ConstexprSwitch<0, 1, 2>{}), F1, int>);
}

struct PushToVector {
  std::vector<std::string>* typeToStringVector;

  template <typename T>
  void operator()() const {
    if constexpr (ad_utility::isSimilar<T, int>) {
      typeToStringVector->emplace_back("int");
    } else if constexpr (ad_utility::isSimilar<T, bool>) {
      typeToStringVector->emplace_back("bool");
    } else if constexpr (ad_utility::isSimilar<T, std::string>) {
      typeToStringVector->emplace_back("std::string");
    } else {
      AD_FAIL();
    }
  }
};

struct PushToVectorWithTI {
  std::vector<std::string>* typeToStringVector;

  void operator()(auto t) const {
    using T = typename decltype(t)::type;
    if constexpr (ad_utility::isSimilar<T, int>) {
      typeToStringVector->emplace_back("int");
    } else if constexpr (ad_utility::isSimilar<T, bool>) {
      typeToStringVector->emplace_back("bool");
    } else if constexpr (ad_utility::isSimilar<T, std::string>) {
      typeToStringVector->emplace_back("std::string");
    } else {
      AD_FAIL();
    }
  }
};

/*
@brief Create a lambda, that adds the string representation of a (supported)
type to a given vector.

@returns A lambda, that takes an explicit template type parameter and adds the
string representation at the end of `*typeToStringVector`.
*/
auto typeToStringFactory(std::vector<std::string>* typeToStringVector) {
  return PushToVector{typeToStringVector};
}

auto typeToStringFactoryWithTI(std::vector<std::string>* typeToStringVector) {
  return PushToVectorWithTI{typeToStringVector};
}

/*
@brief Test a normal call for a `constExprForEachType` function.

@param callToForEachWrapper A lambda wrapper, that takes an explicit template
parameter pack and a lambda function argument, which it passes to a
`constExprForEachType` function in the correct form.
*/
template <typename F>
void testConstExprForEachNormalCall(
    const F& callToForEachWrapper, auto callToTypeToStringFactory,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  // For generating better messages, when failing a test.
  auto trace{generateLocationTrace(l, "testConstExprForEachNormalCall")};

  std::vector<std::string> typeToStringVector{};
  auto typeToString = callToTypeToStringFactory(&typeToStringVector);

  // Normal call.
  callToForEachWrapper.template
  operator()<int, bool, std::string, bool, bool, int, int, int>(typeToString);

  ASSERT_STREQ(typeToStringVector.at(0).c_str(), "int");
  ASSERT_STREQ(typeToStringVector.at(1).c_str(), "bool");
  ASSERT_STREQ(typeToStringVector.at(2).c_str(), "std::string");
  ASSERT_STREQ(typeToStringVector.at(3).c_str(), "bool");
  ASSERT_STREQ(typeToStringVector.at(4).c_str(), "bool");
  ASSERT_STREQ(typeToStringVector.at(5).c_str(), "int");
  ASSERT_STREQ(typeToStringVector.at(6).c_str(), "int");
  ASSERT_STREQ(typeToStringVector.at(7).c_str(), "int");
}

struct TestForEachTypeInParameterPack {
  template <typename... Ts, typename F>
  void operator()(const F& func) const {
    forEachTypeInParameterPack<Ts...>(func);
  }
};

TEST(ConstexprUtils, ForEachTypeInParameterPack) {
  // Normal call.
  testConstExprForEachNormalCall(TestForEachTypeInParameterPack{},
                                 typeToStringFactory);

  // No types given should end in nothing happening.
  std::vector<std::string> typeToStringVector{};
  auto typeToString = typeToStringFactory(&typeToStringVector);
  forEachTypeInParameterPack<>(typeToString);
  ASSERT_TRUE(typeToStringVector.empty());
}

struct TestForEachTypeInParameterPackWithTI {
  template <typename... Ts, typename F>
  void operator()(const F& func) const {
    forEachTypeInParameterPackWithTI<Ts...>(func);
  }
};

TEST(ConstexprUtils, ForEachTypeInParameterPackWithTI) {
  // Normal call.
  testConstExprForEachNormalCall(TestForEachTypeInParameterPackWithTI{},
                                 typeToStringFactoryWithTI);

  // No types given should end in nothing happening.
  std::vector<std::string> typeToStringVector{};
  auto typeToString = typeToStringFactoryWithTI(&typeToStringVector);
  forEachTypeInParameterPackWithTI<>(typeToString);
  ASSERT_TRUE(typeToStringVector.empty());
}

struct TestForEachTypeInTemplateTypeOfVariant {
  template <typename... Ts, typename F>
  void operator()(const F& func) const {
    forEachTypeInTemplateType<std::variant<Ts...>>(func);
  }
};

struct TestForEachTypeInTemplateTypeOfTuple {
  template <typename... Ts, typename F>
  void operator()(const F& func) const {
    forEachTypeInTemplateType<std::tuple<Ts...>>(func);
  }
};

TEST(ConstexprUtils, forEachTypeInTemplateType) {
  // Normal call with `std::variant`.
  testConstExprForEachNormalCall(TestForEachTypeInTemplateTypeOfVariant{},
                                 typeToStringFactory);

  // Normal call with `std::tuple`.
  testConstExprForEachNormalCall(TestForEachTypeInTemplateTypeOfTuple{},
                                 typeToStringFactory);
}

struct TestForEachTypeInTemplateTypeWithTIOfVariant {
  template <typename... Ts, typename F>
  void operator()(const F& func) const {
    using use_type_identity::ti;
    forEachTypeInTemplateTypeWithTI(ti<std::variant<Ts...>>, func);
  }
};

struct TestForEachTypeInTemplateTypeWithTIOfTuple {
  template <typename... Ts, typename F>
  void operator()(const F& func) const {
    using use_type_identity::ti;
    forEachTypeInTemplateTypeWithTI(ti<std::tuple<Ts...>>, func);
  }
};

TEST(ConstexprUtils, forEachTypeInTemplateTypeWithTI) {
  // Normal call with `std::variant`.
  testConstExprForEachNormalCall(TestForEachTypeInTemplateTypeWithTIOfVariant{},
                                 typeToStringFactoryWithTI);

  // Normal call with `std::tuple`.
  testConstExprForEachNormalCall(TestForEachTypeInTemplateTypeWithTIOfTuple{},
                                 typeToStringFactoryWithTI);
}
