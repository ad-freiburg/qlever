//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

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

TEST(TypeTraits, SimiliarToAnyTypeIn) {
  using tup = std::tuple<int, char>;
  using nested = std::tuple<tup>;

  // All the test, were the concept is supposed to return true.
  forEachTypeInTemplateType<tup>([]<typename TupType>() {
    callLambdaWithAllVariationsOfType<TupType>(
        []<typename T>() { static_assert(SimilarToAnyTypeIn<T, tup>); });
  });
  static_assert(SimilarToAnyTypeIn<tup, nested>);

  // All the test, were the concept is supposed to return false.
  forEachTypeInParameterPack<unsigned int, float, bool, size_t, unsigned char>(
      []<typename WrongType>() {
        callLambdaWithAllVariationsOfType<WrongType>(
            []<typename T>() { static_assert(!SimilarToAnyTypeIn<T, tup>); });
      });
  static_assert(!SimilarToAnyTypeIn<tup, char>);
  static_assert(!SimilarToAnyTypeIn<int, int>);
  ASSERT_TRUE(true);
}

TEST(TypeTraits, SameAsAnyTypeIn) {
  using tup = std::tuple<int, const char, bool&, const float&>;
  using nested = std::tuple<tup>;

  // Successful comparison.
  forEachTypeInTemplateType<tup>(
      []<typename TupType>() { static_assert(SameAsAnyTypeIn<TupType, tup>); });
  static_assert(SameAsAnyTypeIn<tup, nested>);

  // Unsuccessful comparison, where the underlying type is wrong.
  forEachTypeInParameterPack<tup, size_t, double, unsigned char>(
      []<typename WrongType>() {
        callLambdaWithAllVariationsOfType<WrongType>([]<typename T>() {
          static_assert(!SameAsAnyTypeIn<WrongType, tup>);
        });
      });

  // Unsuccessful comparison, where the underlying type is contained, but not
  // with those qualifiers.
  auto testNotIncludedWithThoseQualifiers = []<typename TemplatedType>() {
    forEachTypeInTemplateType<TemplatedType>([]<typename CorrectType>() {
      callLambdaWithAllVariationsOfType<CorrectType>([]<typename T>() {
        if constexpr (!std::same_as<CorrectType, T>) {
          static_assert(!SameAsAnyTypeIn<T, tup>);
        }
      });
    });
  };
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

// There is a lot of overlap between the concepts.
TEST(TypeTraits, InvocableWithConvertibleReturnType) {
  /*
  Currently, `std::invocable` and `std::regular_invocable` are the same.
  Therefore, having separate tests would be an unnecessary code increase.
  */
  constexpr auto bothInvocableWithExactReturnType =
      []<typename Fn, typename Ret, typename... Args>() {
        return InvocableWithExactReturnType<Fn, Ret, Args...> &&
               RegularInvocableWithExactReturnType<Fn, Ret, Args...>;
      };
  constexpr auto bothInvocableWithSimilarReturnType =
      []<typename Fn, typename Ret, typename... Args>() {
        return InvocableWithSimilarReturnType<Fn, Ret, Args...> &&
               RegularInvocableWithSimilarReturnType<Fn, Ret, Args...>;
      };

  /*
  Call the given template function with every type in the parameter type list
  `Type, Type&, const Type&, Type&&, const Type&&` as template parameter.
  */
  constexpr auto callWithEveryVariantOfType = []<typename Type>(auto func) {
    using pureT = std::decay_t<Type>;
    passListOfTypesToLambda<pureT, pureT&, const pureT&, pureT&&,
                            const pureT&&>(std::move(func));
  };

  /*
  Call the given template function with every type combination in the cartesian
  product of the parameter type list `Type, Type&, const Type&, Type&&, const
  Type&&`, as template parameter.
  */
  constexpr auto callWithCartesianProductOfEveryVariantOfType =
      []<typename Type>(auto func) {
        using pureT = std::decay_t<Type>;
        passCartesianPorductToLambda<decltype(func), pureT, pureT&,
                                     const pureT&, pureT&&, const pureT&&>(
            std::move(func));
      };

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
      [&bothInvocableWithExactReturnType,
       &bothInvocableWithSimilarReturnType]<typename ParameterType>() {
        static_assert(bothInvocableWithExactReturnType.template
                      operator()<std::negate<int>, int, ParameterType>());
        static_assert(bothInvocableWithSimilarReturnType.template
                      operator()<std::negate<int>, int, ParameterType>());
      });
  callWithCartesianProductOfEveryVariantOfType.template operator()<int>(
      [&bothInvocableWithExactReturnType,
       &bothInvocableWithSimilarReturnType]<typename FirstParameterType,
                                            typename SecondParameterType>() {
        static_assert(bothInvocableWithExactReturnType.template
                      operator()<std::equal_to<int>, bool, FirstParameterType,
                                 SecondParameterType>());
        static_assert(bothInvocableWithSimilarReturnType.template
                      operator()<std::equal_to<int>, bool, FirstParameterType,
                                 SecondParameterType>());
      });

  // Not an invocable.
  static_assert(!bothInvocableWithExactReturnType
                     .template operator()<bool, bool, bool>());
  static_assert(!bothInvocableWithSimilarReturnType
                     .template operator()<bool, bool, bool>());

  // Valid function.
  callWithEveryVariantOfType.template operator()<int>(
      [&bothInvocableWithExactReturnType,
       &bothInvocableWithSimilarReturnType]<typename ReturnType>() {
        if constexpr (std::same_as<ReturnType, int>) {
          static_assert(bothInvocableWithExactReturnType.template
                        operator()<SingleParameter, ReturnType, int&>());
        }
        static_assert(bothInvocableWithSimilarReturnType.template
                      operator()<SingleParameter, ReturnType, int&>());
      });
  callWithEveryVariantOfType.template operator()<bool>(
      [&bothInvocableWithSimilarReturnType,
       &bothInvocableWithExactReturnType]<typename ReturnType>() {
        if constexpr (std::same_as<ReturnType, bool>) {
          static_assert(
              bothInvocableWithExactReturnType.template
              operator()<DoubleParameter, ReturnType, bool&, bool&>());
        }
        static_assert(bothInvocableWithSimilarReturnType.template
                      operator()<DoubleParameter, ReturnType, bool&, bool&>());
      });

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
      [&bothInvocableWithExactReturnType,
       &bothInvocableWithSimilarReturnType]<typename ParameterType>() {
        if constexpr (!std::same_as<ParameterType, int&>) {
          static_assert(!bothInvocableWithExactReturnType.template
                         operator()<SingleParameter, int, ParameterType>());
          static_assert(!bothInvocableWithSimilarReturnType.template
                         operator()<SingleParameter, int, ParameterType>());
        }
      });
  callWithCartesianProductOfEveryVariantOfType.template operator()<bool>(
      [&bothInvocableWithExactReturnType,
       &bothInvocableWithSimilarReturnType]<typename FirstParameterType,
                                            typename SecondParameterType>() {
        if constexpr (!std::same_as<FirstParameterType, bool&> ||
                      !std::same_as<SecondParameterType, bool&>) {
          static_assert(!bothInvocableWithExactReturnType.template
                         operator()<DoubleParameter, bool, FirstParameterType,
                                    SecondParameterType>());
          static_assert(!bothInvocableWithSimilarReturnType.template
                         operator()<DoubleParameter, bool, FirstParameterType,
                                    SecondParameterType>());
        }
      });

  // Parameter types are correct, but return type is wrong.
  callWithEveryVariantOfType.template operator()<int>(
      [&bothInvocableWithExactReturnType]<typename ReturnType>() {
        if constexpr (!std::same_as<ReturnType, int>) {
          static_assert(!bothInvocableWithExactReturnType.template
                         operator()<SingleParameter, ReturnType, int&>());
        }
      });
  callWithEveryVariantOfType.template operator()<bool>(
      [&bothInvocableWithExactReturnType]<typename ReturnType>() {
        if constexpr (!std::same_as<ReturnType, bool>) {
          static_assert(
              !bothInvocableWithExactReturnType.template
               operator()<DoubleParameter, ReturnType, bool&, bool&>());
        }
      });

  // Both the parameter types and the return type is wrong.
  // TODO Is there a cleaner way of iterating multiple parameter packs?
  callWithCartesianProductOfEveryVariantOfType.template operator()<int>(
      [&bothInvocableWithExactReturnType]<typename ReturnType,
                                          typename ParameterType>() {
        if constexpr (!std::same_as<ReturnType, int> &&
                      !std::same_as<ParameterType, int&>) {
          static_assert(
              !bothInvocableWithExactReturnType.template
               operator()<SingleParameter, ReturnType, ParameterType>());
        }
      });
  callWithCartesianProductOfEveryVariantOfType.template operator()<bool>(
      [&bothInvocableWithExactReturnType,
       &callWithEveryVariantOfType]<typename FirstParameterType,
                                    typename SecondParameterType>() {
        // For the return type.
        callWithEveryVariantOfType.template operator()<bool>(
            [&bothInvocableWithExactReturnType]<typename ReturnType>() {
              if constexpr ((!std::same_as<FirstParameterType, bool&> ||
                             !std::same_as<SecondParameterType, bool&>)&&!std::
                                same_as<ReturnType, bool>) {
                static_assert(
                    !bothInvocableWithExactReturnType.template
                     operator()<DoubleParameter, ReturnType, FirstParameterType,
                                SecondParameterType>());
              }
            });
      });
}

TEST(TypeTraits, Rvalue) {
  static_assert(Rvalue<int>);
  static_assert(Rvalue<int&&>);
  static_assert(Rvalue<const int>);
  static_assert(!Rvalue<const int&>);
  static_assert(!Rvalue<int&>);
}
