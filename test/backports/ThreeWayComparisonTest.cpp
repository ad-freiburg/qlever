// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <gmock/gmock.h>

#include "backports/three_way_comparison.h"

class TestClassWithComparison {
  int x;
  int y;

 public:
  QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_CONSTEXPR(TestClassWithComparison, x, y)

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
  friend constexpr auto compareThreeWay(
      const TestClassWithCustomComparison& lhs,
      const TestClassWithCustomComparison& rhs) {
    return (lhs.x == rhs.x) ? ql::compareThreeWay(lhs.y, rhs.y)
                            : ql::compareThreeWay(lhs.x, rhs.x);
  }
  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_CONSTEXPR(TestClassWithCustomComparison)

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

TEST(ThreeWayComparisonTest, FloatingPointComparison) {
  // Test NaN handling
  double nan_val = std::numeric_limits<double>::quiet_NaN();
  double normal_val = 1.0;

  auto result1 = ql::compareThreeWay(nan_val, normal_val);
  auto result2 = ql::compareThreeWay(normal_val, nan_val);
  auto result3 = ql::compareThreeWay(nan_val, nan_val);

  EXPECT_EQ(result1, ql::partial_ordering::unordered);
  EXPECT_EQ(result2, ql::partial_ordering::unordered);
  EXPECT_EQ(result3, ql::partial_ordering::unordered);

  // Test normal floating point comparison
  auto result4 = ql::compareThreeWay(1.0, 2.0);
  auto result5 = ql::compareThreeWay(2.0, 1.0);
  auto result6 = ql::compareThreeWay(1.0, 1.0);

  EXPECT_EQ(result4, ql::partial_ordering::less);
  EXPECT_EQ(result5, ql::partial_ordering::greater);
  EXPECT_EQ(result6, ql::partial_ordering::equivalent);
}

TEST(ThreeWayComparisonTest, IntegerComparison) {
  auto result1 = ql::compareThreeWay(1, 2);
  auto result2 = ql::compareThreeWay(2, 1);
  auto result3 = ql::compareThreeWay(1, 1);

  EXPECT_EQ(result1, ql::strong_ordering::less);
  EXPECT_EQ(result2, ql::strong_ordering::greater);
  EXPECT_EQ(result3, ql::strong_ordering::equal);
}

class TestClassWithMemberCompareThreeWay {
  int x;
  int y;

 public:
  QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL(
      TestClassWithMemberCompareThreeWay, x, y)

  TestClassWithMemberCompareThreeWay(int x, int y) : x(x), y(y) {}
};

TEST(ThreeWayComparisonTest, MemberCompareThreeWay) {
  TestClassWithMemberCompareThreeWay a(1, 2);
  TestClassWithMemberCompareThreeWay b(2, 3);
  TestClassWithMemberCompareThreeWay c(1, 2);

  EXPECT_TRUE(a < b);
  EXPECT_TRUE(a <= b);
  EXPECT_TRUE(b > a);
  EXPECT_TRUE(b >= a);
  EXPECT_TRUE(a == c);
  EXPECT_TRUE(a != b);
}

class TestClassWithExternalCompareThreeWay {
  int value;

 public:
  constexpr ql::strong_ordering compareThreeWay(
      const TestClassWithExternalCompareThreeWay&) const;
  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_CONSTEXPR(
      TestClassWithExternalCompareThreeWay)

  constexpr TestClassWithExternalCompareThreeWay(int val) : value(val) {}
  constexpr int getValue() const { return value; }
};

constexpr ql::strong_ordering
TestClassWithExternalCompareThreeWay::compareThreeWay(
    const TestClassWithExternalCompareThreeWay& other) const {
  return ql ::compareThreeWay(this->getValue(), other.getValue());
}

TEST(ThreeWayComparisonTest, ExternalCompareThreeWay) {
  constexpr TestClassWithExternalCompareThreeWay a(1);
  constexpr TestClassWithExternalCompareThreeWay b(2);

  constexpr auto result = ql::compareThreeWay(a, b);
  static_assert(result < 0, "External compareThreeWay failed");
}

template <typename T>
class TestTemplateClass {
  T value;

 public:
  template <typename U>
  static constexpr auto compareThreeWay(const TestTemplateClass<U>& lhs,
                                        const TestTemplateClass<U>& rhs) {
    return ql::compareThreeWay(lhs.value, rhs.value);
  }
  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_CONSTEXPR_TEMPLATE(template <typename U>,
                                                        TestTemplateClass<U>)

  constexpr bool operator==(const TestTemplateClass& other) const {
    return ql::compareThreeWay(*this, other) == 0;
  }

  constexpr TestTemplateClass(T val) : value(val) {}
};

TEST(ThreeWayComparisonTest, TemplateClass) {
  constexpr TestTemplateClass<int> a(1);
  constexpr TestTemplateClass<int> b(2);
  constexpr TestTemplateClass<int> c(1);

  static_assert(a < b, "Template comparison failed");
  static_assert(a == c, "Template equality failed");
  static_assert(a != b, "Template inequality failed");
}

class TestClassEqualityOnly {
  int x;
  int y;

 public:
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL_CONSTEXPR(TestClassEqualityOnly,
                                                        x, y)

  constexpr TestClassEqualityOnly(int x, int y) : x(x), y(y) {}
};

TEST(ThreeWayComparisonTest, EqualityOnlyOperators) {
  constexpr TestClassEqualityOnly a(1, 2);
  constexpr TestClassEqualityOnly b(1, 2);
  constexpr TestClassEqualityOnly c(2, 3);

  static_assert(a == b, "Equality operator failed");
  static_assert(a != c, "Inequality operator failed");
}

// The following classes test the `_DERIVED` variants of the macros. Those have
// to compare the base class subobjects first, and only then the members that
// were explicitly passed to the macro.
class TestBaseClass {
  int x_;

 public:
  QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL_CONSTEXPR(TestBaseClass, x_)

  constexpr explicit TestBaseClass(int x) : x_(x) {}
};

class TestDerivedEqualityConstexpr : public TestBaseClass {
  int y_;

 public:
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL_CONSTEXPR_DERIVED(
      TestDerivedEqualityConstexpr, TestBaseClass, y_)

  constexpr TestDerivedEqualityConstexpr(int x, int y)
      : TestBaseClass(x), y_(y) {}
};

class TestDerivedEquality : public TestBaseClass {
  int y_;

 public:
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL_DERIVED(TestDerivedEquality,
                                                      TestBaseClass, y_)

  TestDerivedEquality(int x, int y) : TestBaseClass(x), y_(y) {}
};

class TestDerivedThreeWayConstexpr : public TestBaseClass {
  int y_;

 public:
  QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL_CONSTEXPR_DERIVED(
      TestDerivedThreeWayConstexpr, TestBaseClass, y_)

  constexpr TestDerivedThreeWayConstexpr(int x, int y)
      : TestBaseClass(x), y_(y) {}
};

class TestDerivedThreeWay : public TestBaseClass {
  int y_;

 public:
  QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL_DERIVED(TestDerivedThreeWay,
                                                      TestBaseClass, y_)

  TestDerivedThreeWay(int x, int y) : TestBaseClass(x), y_(y) {}
};

// A class that adds nothing to its base class and doesn't declare any
// comparison operators of its own, such that a class derived from it has to
// use the inherited operators.
class TestIntermediateClass : public TestBaseClass {
 public:
  using TestBaseClass::TestBaseClass;
};

class TestDerivedFromIntermediate : public TestIntermediateClass {
  int y_;

 public:
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL_DERIVED(
      TestDerivedFromIntermediate, TestIntermediateClass, y_)

  TestDerivedFromIntermediate(int x, int y) : TestIntermediateClass(x), y_(y) {}
};

// A derived class that adds no members of its own, so that only the base class
// is compared.
class TestDerivedWithoutOwnMembers : public TestBaseClass {
 public:
  QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL_CONSTEXPR_DERIVED(
      TestDerivedWithoutOwnMembers, TestBaseClass)

  using TestBaseClass::TestBaseClass;
};

TEST(ThreeWayComparisonTest, DerivedEqualityOperators) {
  // Differences in the base class must not be ignored.
  static_assert(TestDerivedEqualityConstexpr(1, 2) ==
                TestDerivedEqualityConstexpr(1, 2));
  static_assert(TestDerivedEqualityConstexpr(1, 2) !=
                TestDerivedEqualityConstexpr(3, 2));
  static_assert(TestDerivedEqualityConstexpr(1, 2) !=
                TestDerivedEqualityConstexpr(1, 3));
  static_assert(!(TestDerivedEqualityConstexpr(1, 2) ==
                  TestDerivedEqualityConstexpr(3, 2)));

  EXPECT_EQ(TestDerivedEquality(1, 2), TestDerivedEquality(1, 2));
  EXPECT_NE(TestDerivedEquality(1, 2), TestDerivedEquality(3, 2));
  EXPECT_NE(TestDerivedEquality(1, 2), TestDerivedEquality(1, 3));

  // The same, but with an intermediate class that inherits its operators.
  EXPECT_EQ(TestDerivedFromIntermediate(1, 2),
            TestDerivedFromIntermediate(1, 2));
  EXPECT_NE(TestDerivedFromIntermediate(1, 2),
            TestDerivedFromIntermediate(3, 2));
  EXPECT_NE(TestDerivedFromIntermediate(1, 2),
            TestDerivedFromIntermediate(1, 3));
}

TEST(ThreeWayComparisonTest, DerivedRelationalOperators) {
  // The base class is compared first, so `(1, 7) < (2, 3)` although `7 > 3`.
  static_assert(TestDerivedThreeWayConstexpr(1, 7) <
                TestDerivedThreeWayConstexpr(2, 3));
  static_assert(TestDerivedThreeWayConstexpr(1, 7) <=
                TestDerivedThreeWayConstexpr(2, 3));
  static_assert(TestDerivedThreeWayConstexpr(2, 3) >
                TestDerivedThreeWayConstexpr(1, 7));
  static_assert(TestDerivedThreeWayConstexpr(2, 3) >=
                TestDerivedThreeWayConstexpr(1, 7));
  // Equal base classes, so the additional member decides.
  static_assert(TestDerivedThreeWayConstexpr(1, 3) <
                TestDerivedThreeWayConstexpr(1, 7));
  static_assert(TestDerivedThreeWayConstexpr(1, 7) >
                TestDerivedThreeWayConstexpr(1, 3));
  // Completely equal objects.
  static_assert(TestDerivedThreeWayConstexpr(1, 3) <=
                TestDerivedThreeWayConstexpr(1, 3));
  static_assert(TestDerivedThreeWayConstexpr(1, 3) >=
                TestDerivedThreeWayConstexpr(1, 3));
  static_assert(!(TestDerivedThreeWayConstexpr(1, 3) <
                  TestDerivedThreeWayConstexpr(1, 3)));
  static_assert(!(TestDerivedThreeWayConstexpr(1, 3) >
                  TestDerivedThreeWayConstexpr(1, 3)));
  // The equality operators that come with the three-way comparison also have to
  // consider the base class.
  static_assert(TestDerivedThreeWayConstexpr(1, 3) ==
                TestDerivedThreeWayConstexpr(1, 3));
  static_assert(TestDerivedThreeWayConstexpr(1, 3) !=
                TestDerivedThreeWayConstexpr(2, 3));

  EXPECT_LT(TestDerivedThreeWay(1, 7), TestDerivedThreeWay(2, 3));
  EXPECT_LE(TestDerivedThreeWay(1, 7), TestDerivedThreeWay(2, 3));
  EXPECT_GT(TestDerivedThreeWay(2, 3), TestDerivedThreeWay(1, 7));
  EXPECT_GE(TestDerivedThreeWay(2, 3), TestDerivedThreeWay(1, 7));
  EXPECT_LT(TestDerivedThreeWay(1, 3), TestDerivedThreeWay(1, 7));
  EXPECT_GT(TestDerivedThreeWay(1, 7), TestDerivedThreeWay(1, 3));
  EXPECT_LE(TestDerivedThreeWay(1, 3), TestDerivedThreeWay(1, 3));
  EXPECT_GE(TestDerivedThreeWay(1, 3), TestDerivedThreeWay(1, 3));
  EXPECT_EQ(TestDerivedThreeWay(1, 3), TestDerivedThreeWay(1, 3));
  EXPECT_NE(TestDerivedThreeWay(1, 3), TestDerivedThreeWay(2, 3));

  // A derived class without own members compares only its base class.
  static_assert(TestDerivedWithoutOwnMembers(1) <
                TestDerivedWithoutOwnMembers(2));
  static_assert(TestDerivedWithoutOwnMembers(2) >=
                TestDerivedWithoutOwnMembers(1));
  static_assert(TestDerivedWithoutOwnMembers(1) ==
                TestDerivedWithoutOwnMembers(1));
  static_assert(TestDerivedWithoutOwnMembers(1) !=
                TestDerivedWithoutOwnMembers(2));
}

TEST(ThreeWayComparisonTest, StrongOrderingWithIntegers) {
  auto ordering = ql::strong_ordering::less;

  // Test compareThreeWay with strong_ordering and integer
  auto result1 = ql::compareThreeWay(ordering, 0);
  auto result2 = ql::compareThreeWay(0, ordering);

  EXPECT_EQ(result1, ql::strong_ordering::less);
  EXPECT_EQ(result2, ql::strong_ordering::greater);
}

class TestClassWithDeclaredCompareThreeWay {
  int value;

 public:
  ql::strong_ordering compareThreeWay(
      const TestClassWithDeclaredCompareThreeWay& other) const;
  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL(TestClassWithDeclaredCompareThreeWay)

  TestClassWithDeclaredCompareThreeWay(int val) : value(val) {}
  int getValue() const { return value; }
};

ql::strong_ordering TestClassWithDeclaredCompareThreeWay::compareThreeWay(
    const TestClassWithDeclaredCompareThreeWay& other) const {
  return ql::compareThreeWay(this->value, other.value);
}

TEST(ThreeWayComparisonTest, DeclaredAndDefinedCompareThreeWay) {
  TestClassWithDeclaredCompareThreeWay a(1);
  TestClassWithDeclaredCompareThreeWay b(2);

  EXPECT_TRUE(a < b);
  EXPECT_TRUE(a <= b);
  EXPECT_TRUE(b > a);
  EXPECT_TRUE(b >= a);
}

TEST(ThreeWayComparisonTest, MixedTypeComparison) {
  // Test comparison between different arithmetic types
  auto result1 = ql::compareThreeWay(1, 2.0);
  auto result2 = ql::compareThreeWay(2.0f, 1);
  auto result3 = ql::compareThreeWay(5U, uint64_t{7});

  EXPECT_TRUE(result1 < 0);
  EXPECT_TRUE(result2 > 0);
  EXPECT_TRUE(result3 < 0);
}

#ifdef QLEVER_CPP_17
TEST(ThreeWayComparisonTest, TypeTraits) {
  // Test type trait functionality for C++17
  static_assert(ql::HasAllComparisonOperators_v<int, int>);
  static_assert(ql::HasAllComparisonOperators_v<double, double>);
  static_assert(
      ql::hasAnyCompareThreeWayV<TestClassWithExternalCompareThreeWay,
                                 TestClassWithExternalCompareThreeWay>);
}
#endif
