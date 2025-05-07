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
using use_type_identity::ti;

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
template <typename T, typename F>
constexpr void callLambdaWithAllVariationsOfType(const F& lambda) {
  using decayedT = std::decay_t<T>;
  lambda(ti<decayedT>);
  lambda(ti<const decayedT>);
  lambda(ti<decayedT&>);
  lambda(ti<const decayedT&>);
  lambda(ti<decayedT&&>);
  lambda(ti<const decayedT&&>);
}

TEST(TypeTraits, SimiliarToAnyTypeIn) {
  using tup = std::tuple<int, char>;
  using nested = std::tuple<tup>;

  // All the test, were the concept is supposed to return true.
  forEachTypeInTemplateTypeWithTI(ti<tup>, [](auto t) {
    using TupType = typename decltype(t)::type;
    callLambdaWithAllVariationsOfType<TupType>([](auto t) {
      using T = typename decltype(t)::type;
      static_assert(SimilarToAnyTypeIn<T, tup>);
    });
  });
  static_assert(SimilarToAnyTypeIn<tup, nested>);

  // All the test, were the concept is supposed to return false.
  forEachTypeInParameterPackWithTI<unsigned int, float, bool, size_t,
                                   unsigned char>([](auto t) {
    using WrongType = typename decltype(t)::type;
    callLambdaWithAllVariationsOfType<WrongType>([](auto t) {
      using T = typename decltype(t)::type;
      static_assert(!SimilarToAnyTypeIn<T, tup>);
    });
  });
  static_assert(!SimilarToAnyTypeIn<tup, char>);
  static_assert(!SimilarToAnyTypeIn<int, int>);
  ASSERT_TRUE(true);
}

TEST(TypeTraits, SameAsAnyTypeIn) {
  using tup = std::tuple<int, const char, bool&, const float&>;
  using nested = std::tuple<tup>;

  // Successful comparison.
  forEachTypeInTemplateTypeWithTI(ti<tup>, [](auto t) {
    using TupType = typename decltype(t)::type;
    static_assert(SameAsAnyTypeIn<TupType, tup>);
  });
  static_assert(SameAsAnyTypeIn<tup, nested>);

  // Unsuccessful comparison, where the underlying type is wrong.
  forEachTypeInParameterPackWithTI<tup, size_t, double, unsigned char>(
      [](auto t) {
        using WrongType = typename decltype(t)::type;
        callLambdaWithAllVariationsOfType<WrongType>(
            [](auto) { static_assert(!SameAsAnyTypeIn<WrongType, tup>); });
      });

  // Unsuccessful comparison, where the underlying type is contained, but not
  // with those qualifiers.
  auto testNotIncludedWithThoseQualifiers = [](auto t) {
    forEachTypeInTemplateTypeWithTI(t, [](auto t2) {
      using CorrectType = typename decltype(t2)::type;
      callLambdaWithAllVariationsOfType<CorrectType>([&t2](auto t3) {
        // Note: This redundant repetition is required to work around a bug in
        // GCC.
        using CorrectT = typename std::decay_t<decltype(t2)>::type;
        using T = typename decltype(t3)::type;
        (void)t2;
        if constexpr (!std::same_as<CorrectT, T>) {
          static_assert(!SameAsAnyTypeIn<T, tup>);
        }
      });
    });
  };
  testNotIncludedWithThoseQualifiers(ti<tup>);
  testNotIncludedWithThoseQualifiers(ti<nested>);

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

// There is a lot of overlap between the concepts.
TEST(TypeTraits, InvocableWithConvertibleReturnType) {
  /*
  Currently, `std::invocable` and `std::regular_invocable` are the same.
  Therefore, having separate tests would be an unnecessary code increase.
  */
  constexpr auto bothInvocableWithExactReturnType =
      BothInvocableWithExactReturnType{};
  constexpr auto bothInvocableWithSimilarReturnType =
      BothInvocableWithSimilarReturnType{};

  /*
  Call the given template function with every type in the parameter type list
  `Type, Type&, const Type&, Type&&, const Type&&` as template parameter.
  */
  constexpr auto callWithEveryVariantOfType = [](auto t, auto func) {
    using T = typename decltype(t)::type;
    passListOfTypesToLambda<T, T&, const T&, T&&, const T&&>(std::move(func));
  };

  /*
  Call the given template function with every type combination in the cartesian
  product of the parameter type list `Type, Type&, const Type&, Type&&, const
  Type&&`, as template parameter.
  */
  constexpr auto callWithCartesianProductOfEveryVariantOfType = [](auto t,
                                                                   auto func) {
    using T = typename decltype(t)::type;
    passCartesianPorductToLambda<T, T&, const T&, T&&, const T&&>(
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
  callWithEveryVariantOfType(
      ti<int>, [&bothInvocableWithExactReturnType,
                &bothInvocableWithSimilarReturnType](auto t) {
        using ParameterType = typename decltype(t)::type;
        static_assert(bothInvocableWithExactReturnType.template
                      operator()<std::negate<int>, int, ParameterType>());
        static_assert(bothInvocableWithSimilarReturnType.template
                      operator()<std::negate<int>, int, ParameterType>());
      });
  callWithCartesianProductOfEveryVariantOfType(
      ti<int>, [&bothInvocableWithExactReturnType,
                &bothInvocableWithSimilarReturnType](auto t1, auto t2) {
        using FirstParameterType = typename decltype(t1)::type;
        using SecondParameterType = typename decltype(t2)::type;
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
  callWithEveryVariantOfType(
      ti<int>, [&bothInvocableWithExactReturnType,
                &bothInvocableWithSimilarReturnType](auto t) {
        using ReturnType = typename decltype(t)::type;
        if constexpr (std::same_as<ReturnType, int>) {
          static_assert(bothInvocableWithExactReturnType.template
                        operator()<SingleParameter, ReturnType, int&>());
        }
        static_assert(bothInvocableWithSimilarReturnType.template
                      operator()<SingleParameter, ReturnType, int&>());
      });
  callWithEveryVariantOfType(ti<bool>, [&bothInvocableWithSimilarReturnType,
                                        &bothInvocableWithExactReturnType](
                                           auto t) {
    using ReturnType = typename decltype(t)::type;
    if constexpr (std::same_as<ReturnType, bool>) {
      static_assert(bothInvocableWithExactReturnType.template
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
  callWithEveryVariantOfType(
      ti<int>, [&bothInvocableWithExactReturnType,
                &bothInvocableWithSimilarReturnType](auto t) {
        using ParameterType = typename decltype(t)::type;
        if constexpr (!std::same_as<ParameterType, int&>) {
          static_assert(!bothInvocableWithExactReturnType.template
                         operator()<SingleParameter, int, ParameterType>());
          static_assert(!bothInvocableWithSimilarReturnType.template
                         operator()<SingleParameter, int, ParameterType>());
        }
      });
  callWithCartesianProductOfEveryVariantOfType(
      ti<bool>, [&bothInvocableWithExactReturnType,
                 &bothInvocableWithSimilarReturnType](auto t1, auto t2) {
        using FirstParameterType = typename decltype(t1)::type;
        using SecondParameterType = typename decltype(t2)::type;
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
  callWithEveryVariantOfType(
      ti<int>, [&bothInvocableWithExactReturnType](auto t) {
        using ReturnType = typename decltype(t)::type;
        if constexpr (!std::same_as<ReturnType, int>) {
          static_assert(!bothInvocableWithExactReturnType.template
                         operator()<SingleParameter, ReturnType, int&>());
        }
      });
  callWithEveryVariantOfType(ti<bool>, [&bothInvocableWithExactReturnType](
                                           auto t) {
    using ReturnType = typename decltype(t)::type;
    if constexpr (!std::same_as<ReturnType, bool>) {
      static_assert(!bothInvocableWithExactReturnType.template
                     operator()<DoubleParameter, ReturnType, bool&, bool&>());
    }
  });

  // Both the parameter types and the return type is wrong.
  // TODO Is there a cleaner way of iterating multiple parameter packs?
  callWithCartesianProductOfEveryVariantOfType(
      ti<int>, [&bothInvocableWithExactReturnType](auto t1, auto t2) {
        using ReturnType = typename decltype(t1)::type;
        using ParameterType = typename decltype(t2)::type;
        if constexpr (!std::same_as<ReturnType, int> &&
                      !std::same_as<ParameterType, int&>) {
          static_assert(
              !bothInvocableWithExactReturnType.template
               operator()<SingleParameter, ReturnType, ParameterType>());
        }
      });
  callWithCartesianProductOfEveryVariantOfType(
      ti<bool>, [&bothInvocableWithExactReturnType,
                 &callWithEveryVariantOfType](auto t1, auto t2) {
        using FirstParameterType = typename decltype(t1)::type;
        using SecondParameterType = typename decltype(t2)::type;
        // For the return type.
        callWithEveryVariantOfType(
            ti<bool>, [&bothInvocableWithExactReturnType](auto t) {
              using ReturnType = typename decltype(t)::type;
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
