// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (August of 2023, schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "util/ConfigManager/ValidatorConcept.h"

namespace ad_utility {
/*
@brief Call the given template function with the cartesian product of the
parameter type list with itself, as template parameters. For example: If given
`<int, const int>`, then the function will be called as `Func<int, int>`,
`Func<int, const int>`, `Func<const int, int>` and `Func<const int, const int>`.
*/
template <typename Func, typename... Parameter>
constexpr void passCartesianPorductToLambda(Func func) {
  (
      [&func]<typename T>() {
        (func.template operator()<T, Parameter>(), ...);
      }.template operator()<Parameter>(),
      ...);
}

TEST(ValidatorConceptTest, ValidatorConcept) {
  // Lambda function types for easier test creation.
  using SingleIntValidatorFunction = decltype([](const int&) { return true; });
  using DoubleIntValidatorFunction =
      decltype([](const int&, const int&) { return true; });

  // Valid function.
  static_assert(ad_utility::Validator<SingleIntValidatorFunction, int>);
  static_assert(ad_utility::Validator<DoubleIntValidatorFunction, int, int>);

  // The number of parameter types is wrong.
  static_assert(!ad_utility::Validator<SingleIntValidatorFunction>);
  static_assert(!ad_utility::Validator<SingleIntValidatorFunction, int, int>);
  static_assert(!ad_utility::Validator<DoubleIntValidatorFunction>);
  static_assert(
      !ad_utility::Validator<DoubleIntValidatorFunction, int, int, int, int>);

  // Function is valid, but the parameter types are of the wrong object type.
  static_assert(
      !ad_utility::Validator<SingleIntValidatorFunction, std::vector<bool>>);
  static_assert(
      !ad_utility::Validator<SingleIntValidatorFunction, std::string>);
  static_assert(!ad_utility::Validator<DoubleIntValidatorFunction,
                                       std::vector<bool>, int>);
  static_assert(!ad_utility::Validator<DoubleIntValidatorFunction, int,
                                       std::vector<bool>>);
  static_assert(!ad_utility::Validator<DoubleIntValidatorFunction,
                                       std::vector<bool>, std::vector<bool>>);
  static_assert(
      !ad_utility::Validator<DoubleIntValidatorFunction, std::string, int>);
  static_assert(
      !ad_utility::Validator<DoubleIntValidatorFunction, int, std::string>);
  static_assert(!ad_utility::Validator<DoubleIntValidatorFunction, std::string,
                                       std::string>);

  // The given function is not valid.

  // The parameter types of the function are wrong, but the return type is
  // correct.
  static_assert(
      !ad_utility::Validator<decltype([](int&) { return true; }), int>);
  static_assert(
      !ad_utility::Validator<decltype([](int&&) { return true; }), int>);
  static_assert(
      !ad_utility::Validator<decltype([](const int&&) { return true; }), int>);

  auto validParameterButFunctionParameterWrongAndReturnTypeRightTestHelper =
      []<typename FirstParameter, typename SecondParameter>() {
        static_assert(
            !ad_utility::Validator<
                decltype([](FirstParameter, SecondParameter) { return true; }),
                int, int>);
      };
  passCartesianPorductToLambda<
      decltype(validParameterButFunctionParameterWrongAndReturnTypeRightTestHelper),
      int&, int&&, const int&&>(
      validParameterButFunctionParameterWrongAndReturnTypeRightTestHelper);

  // Parameter types are correct, but return type is wrong.
  static_assert(!ad_utility::Validator<decltype([](int n) { return n; }), int>);
  static_assert(
      !ad_utility::Validator<decltype([](const int n) { return n; }), int>);
  static_assert(
      !ad_utility::Validator<decltype([](const int& n) { return n; }), int>);

  auto validParameterButFunctionParameterRightAndReturnTypeWrongTestHelper =
      []<typename FirstParameter, typename SecondParameter>() {
        static_assert(
            !ad_utility::Validator<decltype([](FirstParameter n,
                                               SecondParameter) { return n; }),
                                   int, int>);
      };
  passCartesianPorductToLambda<
      decltype(validParameterButFunctionParameterRightAndReturnTypeWrongTestHelper),
      int, const int, const int&>(
      validParameterButFunctionParameterRightAndReturnTypeWrongTestHelper);

  // Both the parameter types and the return type is wrong.
  static_assert(
      !ad_utility::Validator<decltype([](int& n) { return n; }), int>);
  static_assert(
      !ad_utility::Validator<decltype([](int&& n) { return n; }), int>);
  static_assert(
      !ad_utility::Validator<decltype([](const int&& n) { return n; }), int>);

  auto validParameterButFunctionParameterWrongAndReturnTypeWrongTestHelper =
      []<typename FirstParameter, typename SecondParameter>() {
        static_assert(
            !ad_utility::Validator<decltype([](FirstParameter n,
                                               SecondParameter) { return n; }),
                                   int, int>);
      };
  passCartesianPorductToLambda<
      decltype(validParameterButFunctionParameterWrongAndReturnTypeWrongTestHelper),
      int&, int&&, const int&&>(
      validParameterButFunctionParameterWrongAndReturnTypeWrongTestHelper);
}

/*
@brief Does the tests for `ValidatorWithSingleParameterTypeOutOfListConcept`
with the given lambda function.

@tparam ConceptFunction A constexpr lambda function, with the signature
`template<typename T, typename... Ts>`, no arguments and a return type of bool.
*/
template <typename ConceptFunction>
void doValidatorWithSingleParameterTypeOutOfListConceptTest(
    ConceptFunction conceptFunction) {
  // Lambda function types for easier test creation.
  using SingleIntValidatorFunction = decltype([](const int&) { return true; });

  // Valid function with list, which contains its parameter.
  static_assert(
      conceptFunction.template operator()<SingleIntValidatorFunction, int>());
  static_assert(conceptFunction.template
                operator()<SingleIntValidatorFunction, int, int, int>());
  static_assert(
      conceptFunction.template operator()<
          SingleIntValidatorFunction, std::string, std::vector<bool>, int>());
  static_assert(
      conceptFunction.template operator()<
          SingleIntValidatorFunction, std::string, int, std::vector<bool>>());

  // Empty list shouldn't work.
  static_assert(
      !conceptFunction.template operator()<SingleIntValidatorFunction>());

  // Correct function, but no valid parameter type.
  static_assert(!conceptFunction.template
                 operator()<SingleIntValidatorFunction, std::string>());
  static_assert(!conceptFunction.template
                 operator()<SingleIntValidatorFunction, std::string,
                            std::vector<bool>, int*, const int*>());

  // Valid types in the list, but the given function is not valid `Validator`.

  // The parameter types of the function are wrong, but the return type is
  // correct.
  auto wrongParameterButRightReturnTest =
      [&conceptFunction]<typename FirstParameter, typename SecondParameter>() {
        static_assert(
            !conceptFunction.template
             operator()<decltype([](int&) { return true; }), FirstParameter>());
        static_assert(!conceptFunction.template operator()<
                       decltype([](int&&) { return true; }), FirstParameter>());
        static_assert(
            !conceptFunction.template operator()<
                decltype([](const int&&) { return true; }), FirstParameter>());

        static_assert(!conceptFunction.template
                       operator()<decltype([](int&) { return true; }),
                                  FirstParameter, SecondParameter>());
        static_assert(!conceptFunction.template
                       operator()<decltype([](int&&) { return true; }),
                                  FirstParameter, SecondParameter>());
        static_assert(!conceptFunction.template
                       operator()<decltype([](const int&&) { return true; }),
                                  FirstParameter, SecondParameter>());
      };
  passCartesianPorductToLambda<decltype(wrongParameterButRightReturnTest), int,
                               std::string>(wrongParameterButRightReturnTest);

  // Parameter types are correct, but return type is wrong.
  auto rightParameterButWrongReturnTest =
      [&conceptFunction]<typename FirstParameter, typename SecondParameter>() {
        static_assert(
            !conceptFunction.template
             operator()<decltype([](int n) { return n; }), FirstParameter>());
        static_assert(
            !conceptFunction.template operator()<
                decltype([](const int n) { return n; }), FirstParameter>());
        static_assert(
            !conceptFunction.template operator()<
                decltype([](const int& n) { return n; }), FirstParameter>());

        static_assert(!conceptFunction.template
                       operator()<decltype([](int n) { return n; }),
                                  FirstParameter, SecondParameter>());
        static_assert(!conceptFunction.template
                       operator()<decltype([](const int n) { return n; }),
                                  FirstParameter, SecondParameter>());
        static_assert(!conceptFunction.template
                       operator()<decltype([](const int& n) { return n; }),
                                  FirstParameter, SecondParameter>());
      };
  passCartesianPorductToLambda<decltype(rightParameterButWrongReturnTest), int,
                               std::string>(rightParameterButWrongReturnTest);

  // Both the parameter types and the return type is wrong.
  auto wrongParameterAndWrongReturnTest =
      [&conceptFunction]<typename FirstParameter, typename SecondParameter>() {
        static_assert(
            !conceptFunction.template
             operator()<decltype([](int& n) { return n; }), FirstParameter>());
        static_assert(
            !conceptFunction.template
             operator()<decltype([](int&& n) { return n; }), FirstParameter>());
        static_assert(
            !conceptFunction.template operator()<
                decltype([](const int&& n) { return n; }), FirstParameter>());

        static_assert(!conceptFunction.template
                       operator()<decltype([](int& n) { return n; }),
                                  FirstParameter, SecondParameter>());
        static_assert(!conceptFunction.template
                       operator()<decltype([](int&& n) { return n; }),
                                  FirstParameter, SecondParameter>());
        static_assert(!conceptFunction.template
                       operator()<decltype([](const int&& n) { return n; }),
                                  FirstParameter, SecondParameter>());
      };
  passCartesianPorductToLambda<decltype(wrongParameterAndWrongReturnTest), int,
                               std::string>(wrongParameterAndWrongReturnTest);
}

TEST(ValidatorConceptTest, ValidatorWithSingleParameterTypeOutOfListConcept) {
  // We moved the test into it's own function to reduce code duplication.
  doValidatorWithSingleParameterTypeOutOfListConceptTest([]<typename Func,
                                                            typename... Ts>() {
    return ad_utility::ValidatorWithSingleParameterTypeOutOfList<Func, Ts...>;
  });
}

TEST(ValidatorConceptTest, isValidatorWithSingleParameterTypeOutOfVariant) {
  // Do all the tests for `ValidatorWithSingleParameterTypeOutOfList`.
  doValidatorWithSingleParameterTypeOutOfListConceptTest(
      []<typename Func, typename... Ts>() {
        return ad_utility::isValidatorWithSingleParameterTypeOutOfVariant<
            Func, std::variant<Ts...>>;
      });

  // Lambda function types for easier test creation.
  using SingleIntValidatorFunction = decltype([](const int&) { return true; });

  // The function is valid, but the passed `VariantType` isn't a `std::variant`.
  static_assert(!ad_utility::isValidatorWithSingleParameterTypeOutOfVariant<
                SingleIntValidatorFunction, std::vector<int>>);
  static_assert(!ad_utility::isValidatorWithSingleParameterTypeOutOfVariant<
                SingleIntValidatorFunction, std::tuple<int>>);
  static_assert(!ad_utility::isValidatorWithSingleParameterTypeOutOfVariant<
                SingleIntValidatorFunction, std::tuple<int, int, int, int>>);
}
}  // namespace ad_utility
