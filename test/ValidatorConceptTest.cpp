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

}  // namespace ad_utility
