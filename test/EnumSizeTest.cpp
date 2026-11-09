// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <array>
#include <cstdint>

#include "util/EnumSize.h"

namespace enumSizeTest {
enum class Color : uint8_t { Red, Green, Blue };
QLEVER_ENUM_SIZE(Color, 3);

enum Plain { A };
QLEVER_ENUM_SIZE(Plain, 1);

struct WithNestedEnum {
  enum struct Kind { First, Second };
  QLEVER_NESTED_ENUM_SIZE(Kind, 2);
};

template <typename T>
struct TemplateWithNestedEnum {
  enum struct Kind { First, Second, Third, Fourth };
  QLEVER_NESTED_ENUM_SIZE(Kind, 4);
};

// Enums that are not regular. They can't be used with `QLEVER_ENUM_SIZE`,
// because that would fail to compile with reflection, see below.
enum class DoesNotStartAtZero { A = 1, B };
enum class HasGap { A, B = 2 };
enum class NotInDeclarationOrder { B = 1, A = 0 };
enum class WithAlias { A, B, C = B };
enum class Negative : int8_t { A = -1, B, C };
}  // namespace enumSizeTest

using namespace enumSizeTest;

// Test `qlever::enumSize` for an enum at namespace scope, a plain enum, a
// nested enum, and a nested enum of a class template.
TEST(EnumSize, enumSize) {
  static_assert(qlever::enumSize<Color> == 3);
  static_assert(qlever::enumSize<Plain> == 1);
  static_assert(qlever::enumSize<WithNestedEnum::Kind> == 2);
  static_assert(qlever::enumSize<TemplateWithNestedEnum<int>::Kind> == 4);
  EXPECT_EQ(qlever::enumSize<Color>, 3u);
}

// Test that `qlever::allEnumValues` lists all the values in increasing order.
TEST(EnumSize, allEnumValues) {
  using C = Color;
  EXPECT_EQ(qlever::allEnumValues<C>, (std::array{C::Red, C::Green, C::Blue}));
  EXPECT_EQ(qlever::allEnumValues<Plain>, std::array{A});
  using K = WithNestedEnum::Kind;
  EXPECT_EQ(qlever::allEnumValues<K>, (std::array{K::First, K::Second}));
}

#ifdef QLEVER_HAS_REFLECTION
// Test the check that `QLEVER_ENUM_SIZE` performs if reflection is supported.
TEST(EnumSize, reflectionCheck) {
  using qlever::enumSizeDetail::isRegularEnumWithSize;
  static_assert(isRegularEnumWithSize<Color, 3>());
  static_assert(isRegularEnumWithSize<WithNestedEnum::Kind, 2>());

  // Wrong sizes.
  static_assert(!isRegularEnumWithSize<Color, 2>());
  static_assert(!isRegularEnumWithSize<Color, 4>());

  // Enums that are not regular.
  static_assert(!isRegularEnumWithSize<DoesNotStartAtZero, 2>());
  static_assert(!isRegularEnumWithSize<HasGap, 2>());
  static_assert(!isRegularEnumWithSize<HasGap, 3>());
  static_assert(!isRegularEnumWithSize<NotInDeclarationOrder, 2>());
  static_assert(!isRegularEnumWithSize<WithAlias, 3>());
  static_assert(!isRegularEnumWithSize<WithAlias, 2>());
  static_assert(!isRegularEnumWithSize<Negative, 3>());
}
#endif
