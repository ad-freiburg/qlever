//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <gtest/gtest.h>

#include <concepts>
#include <functional>
#include <type_traits>

#include "../test/util/TypeTraitsTestHelpers.h"
#include "util/ConstexprUtils.h"
#include "util/TypeTraits.h"

using namespace ad_utility;
TEST(TypeTraits, IsSimilar) {
  static_assert(isSimilar<int, const int&>);
  static_assert(isSimilar<int&, const int&>);
  static_assert(isSimilar<volatile int&, const int>);
  static_assert(!isSimilar<volatile int&, const int*>);
  ASSERT_TRUE(true);
}

/*
Call the given lambda with explicit types: `std::decay_t<T>`,`const
std::decay_t<T>`,`std::decay_t<T>&`,`const std::decay_t<T>&`.
*/
template <typename T>
constexpr void callLambdaWithAllVariationsOfType(const auto& lambda) {
  using decayedT = std::decay_t<T>;
  lambda.template operator()<decayedT>();
  lambda.template operator()<const decayedT>();
  lambda.template operator()<decayedT&>();
  lambda.template operator()<const decayedT&>();
  lambda.template operator()<decayedT&&>();
  lambda.template operator()<const decayedT&&>();
}

template <typename Tup>
struct TestSimiliarToAnyTypeIn {
  struct CheckSimilar {
    template <typename T>
    void operator()() const {
      static_assert(SimilarToAnyTypeIn<T, Tup>);
    }
  };

  template <typename TupType>
  void operator()() const {
    callLambdaWithAllVariationsOfType<TupType>(CheckSimilar{});
  }
};

template <typename Tup>
struct TestNotSimiliarToAnyTypeIn {
  struct CheckNotSimilar {
    template <typename T>
    void operator()() const {
      static_assert(!SimilarToAnyTypeIn<T, Tup>);
    }
  };

  template <typename WrongType>
  void operator()() const {
    callLambdaWithAllVariationsOfType<WrongType>(CheckNotSimilar{});
  }
};

TEST(TypeTraits, SimiliarToAnyTypeIn) {
  using tup = std::tuple<int, char>;
  using nested = std::tuple<tup>;

  // All the test, were the concept is supposed to return true.
  forEachTypeInTemplateType<tup>(TestSimiliarToAnyTypeIn<tup>{});
  static_assert(SimilarToAnyTypeIn<tup, nested>);

  // All the test, were the concept is supposed to return false.
  forEachTypeInParameterPack<unsigned int, float, bool, size_t, unsigned char>(
      TestNotSimiliarToAnyTypeIn<tup>{});
  static_assert(!SimilarToAnyTypeIn<tup, char>);
  static_assert(!SimilarToAnyTypeIn<int, int>);
  ASSERT_TRUE(true);
}

template <typename Tup>
struct TestSameType {
  template <typename TupType>
  void operator()() const {
    static_assert(SameAsAnyTypeIn<TupType, Tup>);
  }
};

template <typename Tup>
struct TestNotSameType {
  struct CheckNotSame {
    template <typename T>
    void operator()() const {
      static_assert(!SameAsAnyTypeIn<T, Tup>);
    }
  };

  template <typename WrongType>
  void operator()() const {
    callLambdaWithAllVariationsOfType<WrongType>(CheckNotSame{});
  }
};

// Unsuccessful comparison, where the underlying type is contained, but not
// with those qualifiers.
template <typename Tup>
struct TestNotIncludedWithThoseQualifiers {
  struct CheckCorrectType {
    template <typename CorrectType>
    struct CheckType {
      template <typename T>
      void operator()() const {
        if constexpr (!std::same_as<CorrectType, T>) {
          static_assert(!SameAsAnyTypeIn<T, Tup>);
        }
      }
    };

    template <typename CorrectType>
    void operator()() const {
      callLambdaWithAllVariationsOfType<CorrectType>(CheckType<CorrectType>{});
    }
  };

  template <typename TemplatedType>
  void operator()() const {
    forEachTypeInTemplateType<TemplatedType>(CheckCorrectType{});
  }
};

TEST(TypeTraits, SameAsAnyTypeIn) {
  using tup = std::tuple<int, const char, bool&, const float&>;
  using nested = std::tuple<tup>;

  // Successful comparison.
  forEachTypeInTemplateType<tup>(TestSameType<tup>{});
  static_assert(SameAsAnyTypeIn<tup, nested>);

  // Unsuccessful comparison, where the underlying type is wrong.
  forEachTypeInParameterPack<tup, size_t, double, unsigned char>(
      TestNotSameType<tup>{});

  TestNotIncludedWithThoseQualifiers<tup> testNotIncludedWithThoseQualifiers;
  testNotIncludedWithThoseQualifiers.template operator()<tup>();
  testNotIncludedWithThoseQualifiers.template operator()<nested>();

  // Should only works with templated types.
  static_assert(!SameAsAnyTypeIn<int, int>);
  ASSERT_TRUE(true);
}

TEST(TypeTraits, IsInstantiation) {
  static_assert(isVector<std::vector<int>>);
  static_assert(!isVector<std::tuple<int>>);
  static_assert(!isVector<int>);

  static_assert(isTuple<std::tuple<int>>);
  static_assert(isTuple<std::tuple<int, bool>>);
  static_assert(!isTuple<std::variant<int, bool>>);
  static_assert(!isTuple<int>);

  static_assert(isVariant<std::variant<int>>);
  static_assert(isVariant<std::variant<int, bool>>);
  static_assert(!isVariant<std::tuple<int, bool>>);
  static_assert(!isVariant<int>);
  ASSERT_TRUE(true);
}

template <typename>
struct TypeLifter {};

TEST(TypeTraits, Lift) {
  using T1 = std::tuple<int>;
  using LT = LiftedTuple<T1, TypeLifter>;
  static_assert(std::is_same_v<std::tuple<TypeLifter<int>>, LT>);

  using V = std::variant<int>;
  using LV = LiftedVariant<V, TypeLifter>;
  static_assert(std::is_same_v<std::variant<TypeLifter<int>>, LV>);

  using TT = std::tuple<int, bool, short>;
  using LTT = LiftedTuple<TT, TypeLifter>;
  static_assert(
      std::is_same_v<
          std::tuple<TypeLifter<int>, TypeLifter<bool>, TypeLifter<short>>,
          LTT>);

  using VV = std::variant<int, bool, short>;
  using LVV = LiftedVariant<VV, TypeLifter>;
  static_assert(
      std::is_same_v<
          std::variant<TypeLifter<int>, TypeLifter<bool>, TypeLifter<short>>,
          LVV>);
  ASSERT_TRUE(true);
}

TEST(TypeTraits, TupleToVariant) {
  using T = std::tuple<int>;
  using V = TupleToVariant<T>;
  static_assert(std::is_same_v<V, std::variant<int>>);

  using TT = std::tuple<int, short, bool>;
  using VV = TupleToVariant<TT>;
  static_assert(std::is_same_v<VV, std::variant<int, short, bool>>);
  ASSERT_TRUE(true);
}

TEST(TypeTraits, TupleCat) {
  using T1 = std::tuple<int, short>;
  using T2 = std::tuple<bool, long, size_t>;
  using T3 = std::tuple<>;

  static_assert(std::is_same_v<T1, TupleCat<T1>>);
  static_assert(std::is_same_v<T2, TupleCat<T2>>);
  static_assert(std::is_same_v<T3, TupleCat<T3>>);

  static_assert(std::is_same_v<T1, TupleCat<T1, T3>>);
  static_assert(std::is_same_v<T2, TupleCat<T2, T3>>);

  static_assert(std::is_same_v<std::tuple<int, short, bool, long, size_t>,
                               TupleCat<T1, T2>>);
  static_assert(std::is_same_v<std::tuple<int, short, bool, long, size_t>,
                               TupleCat<T1, T3, T2>>);
  ASSERT_TRUE(true);
}

struct BothInvocableWithExactReturnType {
  template <typename Fn, typename Ret, typename... Args>
  constexpr bool operator()() const {
    return InvocableWithExactReturnType<Fn, Ret, Args...> &&
           RegularInvocableWithExactReturnType<Fn, Ret, Args...>;
  }
};

struct BothInvocableWithSimilarReturnType {
  template <typename Fn, typename Ret, typename... Args>
  constexpr bool operator()() const {
    return InvocableWithSimilarReturnType<Fn, Ret, Args...> &&
           RegularInvocableWithSimilarReturnType<Fn, Ret, Args...>;
  }
};

/*
Call the given template function with every type in the parameter type list
`Type, Type&, const Type&, Type&&, const Type&&` as template parameter.
*/
struct CallWithEveryVariantOfType {
  template <typename Type>
  constexpr void operator()(auto func) const {
    using pureT = std::decay_t<Type>;
    passListOfTypesToLambda<pureT, pureT&, const pureT&, pureT&&,
                            const pureT&&>(std::move(func));
  }
};

/*
Call the given template function with every type combination in the cartesian
product of the parameter type list `Type, Type&, const Type&, Type&&, const
Type&&`, as template parameter.
*/
struct CallWithCartesianProductOfEveryVariantOfType {
  template <typename Type>
  constexpr void operator()(auto func) const {
    using pureT = std::decay_t<Type>;
    passCartesianPorductToLambda<decltype(func), pureT, pureT&, const pureT&,
                                 pureT&&, const pureT&&>(std::move(func));
  }
};

struct CheckNotInvocableWithIntParameter {
  template <typename ParameterType>
  constexpr void operator()() const {
    static_assert(BothInvocableWithExactReturnType{}.template
                  operator()<std::negate<int>, int, ParameterType>());
    static_assert(BothInvocableWithSimilarReturnType{}.template
                  operator()<std::negate<int>, int, ParameterType>());
  }
};

struct CheckInvocableWithBoolReturnType {
  template <typename FirstParameterType, typename SecondParameterType>
  constexpr void operator()() const {
    static_assert(
        BothInvocableWithExactReturnType{}
            .template operator()<std::equal_to<int>, bool, FirstParameterType,
                                 SecondParameterType>());
    static_assert(
        BothInvocableWithSimilarReturnType{}
            .template operator()<std::equal_to<int>, bool, FirstParameterType,
                                 SecondParameterType>());
  }
};

template <typename SingleParameter>
struct CheckInvocableWithIntParameter {
  template <typename ReturnType>
  constexpr void operator()() const {
    if constexpr (std::same_as<ReturnType, int>) {
      static_assert(BothInvocableWithExactReturnType{}.template
                    operator()<SingleParameter, ReturnType, int&>());
    }
    static_assert(BothInvocableWithSimilarReturnType{}.template
                  operator()<SingleParameter, ReturnType, int&>());
  }
};

template <typename DoubleParameter>
struct CheckInvocableWithBoolParameter {
  template <typename ReturnType>
  constexpr void operator()() const {
    if constexpr (std::same_as<ReturnType, bool>) {
      static_assert(BothInvocableWithExactReturnType{}.template
                    operator()<DoubleParameter, ReturnType, bool&, bool&>());
    }
    static_assert(BothInvocableWithSimilarReturnType{}.template
                  operator()<DoubleParameter, ReturnType, bool&, bool&>());
  }
};

template <typename SingleParameter>
struct CheckNotInvocableWithIntReturnType {
  template <typename ParameterType>
  constexpr void operator()() const {
    if constexpr (!std::same_as<ParameterType, int&>) {
      static_assert(!BothInvocableWithExactReturnType{}.template
                     operator()<SingleParameter, int, ParameterType>());
      static_assert(!BothInvocableWithSimilarReturnType{}.template
                     operator()<SingleParameter, int, ParameterType>());
    }
  }
};

template <typename DoubleParameter>
struct CheckNotInvocableWithBoolReturnType {
  template <typename FirstParameterType, typename SecondParameterType>
  constexpr void operator()() const {
    if constexpr (!std::same_as<FirstParameterType, bool&> ||
                  !std::same_as<SecondParameterType, bool&>) {
      static_assert(
          !BothInvocableWithExactReturnType{}
               .template operator()<DoubleParameter, bool, FirstParameterType,
                                    SecondParameterType>());
      static_assert(
          !BothInvocableWithSimilarReturnType{}
               .template operator()<DoubleParameter, bool, FirstParameterType,
                                    SecondParameterType>());
    }
  }
};

template <typename SingleParameter>
struct CheckReturnTypeNotSameAsInt {
  template <typename ReturnType>
  constexpr void operator()() const {
    if constexpr (!std::same_as<ReturnType, int>) {
      static_assert(!BothInvocableWithExactReturnType{}.template
                     operator()<SingleParameter, ReturnType, int&>());
    }
  }
};

template <typename DoubleParameter>
struct CheckReturnTypeNotSameAsBool {
  template <typename ReturnType>
  constexpr void operator()() const {
    if constexpr (!std::same_as<ReturnType, bool>) {
      static_assert(!BothInvocableWithExactReturnType{}.template
                     operator()<DoubleParameter, ReturnType, bool&, bool&>());
    }
  }
};

template <typename SingleParameter>
struct CheckBothNotInvocableWithIntReturnAndParameterType {
  template <typename ReturnType, typename ParameterType>
  constexpr void operator()() const {
    if constexpr (!std::same_as<ReturnType, int> &&
                  !std::same_as<ParameterType, int&>) {
      static_assert(!BothInvocableWithExactReturnType{}.template
                     operator()<SingleParameter, ReturnType, ParameterType>());
    }
  }
};

template <typename DoubleParameter>
struct CheckNotInvocableWithBoolReturnAndParameterType {
  template <typename FirstParameterType, typename SecondParameterType>
  struct DoCheck {
    template <typename ReturnType>
    constexpr void operator()() {
      if constexpr ((!std::same_as<FirstParameterType, bool&> ||
                     !std::same_as<SecondParameterType,
                                   bool&>)&&!std::same_as<ReturnType, bool>) {
        static_assert(!BothInvocableWithExactReturnType{}
                           .template operator()<DoubleParameter, ReturnType,
                                                FirstParameterType,
                                                SecondParameterType>());
      }
    }
  };

  template <typename FirstParameterType, typename SecondParameterType>
  constexpr void operator()() const {
    // For the return type.
    CallWithEveryVariantOfType{}.template operator()<bool>(
        DoCheck<FirstParameterType, SecondParameterType>{});
  }
};

// There is a lot of overlap between the concepts.
TEST(TypeTraits, InvocableWithConvertibleReturnType) {
  /*
  Currently, `std::invocable` and `std::regular_invocable` are the same.
  Therefore, having separate tests would be an unnecessary code increase.
  */
  constexpr BothInvocableWithExactReturnType bothInvocableWithExactReturnType;
  constexpr BothInvocableWithSimilarReturnType
      bothInvocableWithSimilarReturnType;

  constexpr CallWithEveryVariantOfType callWithEveryVariantOfType;

  constexpr CallWithCartesianProductOfEveryVariantOfType
      callWithCartesianProductOfEveryVariantOfType;

  // Invocable types for checking.
  auto singleParameterLambda = [](int&) -> int { return 4; };
  using SingleParameter = decltype(singleParameterLambda);
  auto doubleParameterLambda = [](bool&, bool&) -> bool { return true; };
  using DoubleParameter = decltype(doubleParameterLambda);

  /*
  Make sure, that the parameter types and the types, with which the function is
  called, DON'T have to be the exact same in cases, where they don't need to.

  For example: A function with the single parameter `int` can take any version
  of `int`. `int`, `const int`, `const int&`, etc.
  */
  callWithEveryVariantOfType.template operator()<int>(
      CheckNotInvocableWithIntParameter{});
  callWithCartesianProductOfEveryVariantOfType.template operator()<int>(
      CheckInvocableWithBoolReturnType{});

  // Not an invocable.
  static_assert(!bothInvocableWithExactReturnType
                     .template operator()<bool, bool, bool>());
  static_assert(!bothInvocableWithSimilarReturnType
                     .template operator()<bool, bool, bool>());

  // Valid function.
  callWithEveryVariantOfType.template operator()<int>(
      CheckInvocableWithIntParameter<SingleParameter>{});
  callWithEveryVariantOfType.template operator()<bool>(
      CheckInvocableWithBoolParameter<DoubleParameter>{});

  // The number of parameter types is wrong.
  static_assert(!bothInvocableWithExactReturnType
                     .template operator()<SingleParameter, int>());
  static_assert(!bothInvocableWithExactReturnType
                     .template operator()<SingleParameter, int, int&, int&>());
  static_assert(!bothInvocableWithExactReturnType
                     .template operator()<DoubleParameter, bool>());
  static_assert(!bothInvocableWithSimilarReturnType
                     .template operator()<DoubleParameter, bool, bool&>());
  static_assert(!bothInvocableWithSimilarReturnType.template
                 operator()<DoubleParameter, bool, bool&, bool&, bool&>());

  // The parameter types  are wrong, but the return type is correct.
  callWithEveryVariantOfType.template operator()<int>(
      CheckNotInvocableWithIntReturnType<SingleParameter>{});
  callWithCartesianProductOfEveryVariantOfType.template operator()<bool>(
      CheckNotInvocableWithBoolReturnType<DoubleParameter>{});

  // Parameter types are correct, but return type is wrong.
  callWithEveryVariantOfType.template operator()<int>(
      CheckReturnTypeNotSameAsInt<SingleParameter>{});
  callWithEveryVariantOfType.template operator()<bool>(
      CheckReturnTypeNotSameAsBool<DoubleParameter>{});

  // Both the parameter types and the return type is wrong.
  // TODO Is there a cleaner way of iterating multiple parameter packs?
  callWithCartesianProductOfEveryVariantOfType.template operator()<int>(
      CheckBothNotInvocableWithIntReturnAndParameterType<SingleParameter>{});
  callWithCartesianProductOfEveryVariantOfType.template operator()<bool>(
      CheckNotInvocableWithBoolReturnAndParameterType<DoubleParameter>{});
}

TEST(TypeTraits, Rvalue) {
  static_assert(Rvalue<int>);
  static_assert(Rvalue<int&&>);
  static_assert(Rvalue<const int>);
  static_assert(!Rvalue<const int&>);
  static_assert(!Rvalue<int&>);
}

// _____________________________________________________________________________
TEST(TypeTraits, InvokeResultSfinaeFriendly) {
  using namespace ad_utility;
  using namespace ad_utility::invokeResultSfinaeFriendly::detail;
  [[maybe_unused]] auto x = [](int) -> bool { return false; };

  static_assert(
      std::is_same_v<InvokeResultSfinaeFriendly<decltype(x), int>, bool>);
  [[maybe_unused]] auto tp = getInvokeResultImpl<decltype(x), int>();
  EXPECT_TRUE((std::is_same_v<typename decltype(tp)::type, bool>));

  static_assert(
      std::is_same_v<InvokeResultSfinaeFriendly<decltype(x), const char*>,
                     InvalidInvokeResult<decltype(x), const char*>>);
  [[maybe_unused]] auto tp2 = getInvokeResultImpl<decltype(x), const char*>();
  EXPECT_TRUE((std::is_same_v<typename decltype(tp2)::type,
                              InvalidInvokeResult<decltype(x), const char*>>));
}
