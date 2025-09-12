// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <gmock/gmock.h>

#include "backports/three_way_comparison.h"

class TestClassWithComparison {
  int x;
  int y;

  QL_DEFINE_CLASS_MEMBERS_AS_TIE_CONSTEXPR(x, y)

 public:
  QL_DEFINE_EQUALITY_OPERATOR_CONSTEXPR(TestClassWithComparison)
  QL_DEFINE_THREEWAY_OPERATOR_CONSTEXPR(TestClassWithComparison)

  constexpr TestClassWithComparison(int x, int y) : x(x), y(y) {}
};

TEST(ThreeWayComparisonTest, RelationalOperators) {
  constexpr TestClassWithComparison a(1, 2);
  constexpr TestClassWithComparison b(2, 3);

  static_assert(a < b, "Operator < failed");
  static_assert(a <= b, "Operator <= failed");
  static_assert(b > a, "Operator > failed");
  static_assert(b >= a, "Operator >= failed");
}

TEST(ThreeWayComparisonTest, EqualityOperators) {
  constexpr TestClassWithComparison a(1, 2);
  constexpr TestClassWithComparison b(1, 2);
  constexpr TestClassWithComparison c(2, 3);

  static_assert(a == b, "Operator == failed");
  static_assert(a != c, "Operator != failed");
}

class TestClassWithCustomComparison {
  int x;
  int y;

 public:
  QL_DEFINE_THREEWAY_OPERATOR_CUSTOM_CONSTEXPR(
      TestClassWithCustomComparison,
      (const TestClassWithCustomComparison& lhs,
       const TestClassWithCustomComparison& rhs),
      {
        return (lhs.x == rhs.x) ? ql::compareThreeWay(lhs.y, rhs.y)
                                : ql::compareThreeWay(lhs.x, rhs.x);
      })

  constexpr bool operator==(const TestClassWithCustomComparison& other) const {
    return ql::compareThreeWay(*this, other) == 0;
  }

  constexpr TestClassWithCustomComparison(int x, int y) : x(x), y(y) {}
};

TEST(CustomThreeWayComparisonTest, RelationalOperators) {
  constexpr TestClassWithCustomComparison a(1, 2);
  constexpr TestClassWithCustomComparison b(2, 3);

  static_assert(a < b, "Operator < failed");
  static_assert(a <= b, "Operator <= failed");
  static_assert(b > a, "Operator > failed");
  static_assert(b >= a, "Operator >= failed");
}

TEST(CustomThreeWayComparisonTest, EqualityOperators) {
  constexpr TestClassWithCustomComparison a(1, 2);
  constexpr TestClassWithCustomComparison b(1, 2);
  constexpr TestClassWithCustomComparison c(2, 3);

  static_assert(a == b, "Operator == failed");
  static_assert(a != c, "Operator != failed");
}
