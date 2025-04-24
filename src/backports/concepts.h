// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_BACKPORTS_CONCEPTS_H
#define QLEVER_SRC_BACKPORTS_CONCEPTS_H

#include <concepts/concepts.hpp>

#include "backports/cppTemplate2.h"
#ifndef QLEVER_CPP_17
#include <concepts>
#endif

// Define the following macros:
//
// `QL_CONCEPT_OR_NOTHING(arg)`: expands to `arg` in C++20 mode, and to
// nothing in C++17 mode. It can be used to easily opt out of concepts that are
// only used for documentation and increased safety and not for overload
// resolution.
//
// `QL_CONCEPT_OR_TYPENAME(arg)`: expands to `arg` in C++20 mode, and to
// `typename` in C++17 mode. Example usage:
//
// `CPP_lambda(capture)(arg)(requires ...)`: Expands lambda to use
//  `requires` in C++20 mode and `std::enable_if_t` in C++17 mode.
//
// `CPP_lambda_mut(capture)(arg)(requires ...)`: Same as
//   `CPP_lambda` but for mutable lambdas.
//
// `CPP_template_lambda(capture)(typenames...)(arg)(requires ...)`: Expands
//   lambda with (C++20) explicit template parameters  to use
//  `requires` in C++20 mode and `std::enable_if_t` in C++17 mode.
//
// `CPP_template_lambda_mut(capture)(typenames...)(arg)(requires ...)`: Same as
//  `CPP_template_lambda` but for mutable lambdas.
//
// `CPP_variadic_template`
// Similar to CPP_template(_2), but works for variadic functions.
//
// `CPP_class_template`, Similar to `CPP_variadic_template`, but
// preferred for classes. The differences between the two are as follows:
// `CPP_variadic_template` supports overloading of a function (which is
// irrelevant for classes) whereas in `CPP_variadic_class_template` the
// constraints are implemented as types, not as values. It can be beneficial in
// generic metaprogramming to have a template with only type parameters, e.g. to
// match it via `template<template<typename...> typename T>`. For the same
// reasons, `CPP_variadic_template` is preferable for classes that only have
// non-type template parameters, although those are much rarer.
//
// Example usages:
//
// `QL_CONCEPT_OR_NOTHING(std::view) auto x = someFunction();`
//
// `QL_CONCEPT_OR_NOTHING(SameAsAny<int, float>)`
//
// `void f(QL_CONCEPT_OR_NOTHING(std::view) auto x) {...}`
//
// `template <QL_CONCEPT_OR_TYPENAME(ql::same_as<int>) T> void f(){...}`
//
// `auto myLambda = CPP_lambda(someCapture)(someArg)(requires
// ranges::same_as<decltype(someArg), int>) {...}`
//
// `auto myLambda2 = CPP_lambda(someCapture)(typename F)(F arg)(requires
// ranges::same_as<F, int>) {...}`
//
// NOTE: The macros are variadic to allow for commas in the argument, like in
// the second example above.

// Additionally define the macros `CPP_template_2` and `CPP_and_2` that can
// be used to constrain member functions of classes where the outer class
// has already been constrained with `CPP_template`. For a detailed example, see
// the `test/backports/ConceptsTest.cpp` file.

#ifdef QLEVER_CPP_17
#define QL_CONCEPT_OR_NOTHING(...)
#define QL_CONCEPT_OR_TYPENAME(...) typename
#define CPP_template_2 CPP_template_2_SFINAE
#define CPP_and_2 CPP_and_2_sfinae
#define CPP_and_def CPP_and_sfinae_def
#define CPP_and_2_def CPP_and_2_def_sfinae
#define CPP_variadic_template CPP_template_NO_DEFAULT_SFINAE
#define CPP_class_template CPP_template_VARIADIC_CLASS_SFINAE
#define CPP_member_def CPP_member_def_sfinae
#define CPP_lambda CPP_lambda_sfinae
#define CPP_template_lambda CPP_template_lambda_sfinae
#define CPP_lambda_mut CPP_lambda_mut_sfinae
#define CPP_template_lambda_mut CPP_template_lambda_mut_sfinae
#else
#define QL_CONCEPT_OR_NOTHING(...) __VA_ARGS__
#define QL_CONCEPT_OR_TYPENAME(...) __VA_ARGS__
#define CPP_template_2 CPP_template
#define CPP_and_2 CPP_and
#define CPP_and_def CPP_and
#define CPP_and_2_def CPP_and
#define CPP_variadic_template CPP_template
#define CPP_class_template CPP_template
#define CPP_member_def CPP_member
#define CPP_lambda CPP_LAMBDA_20
#define CPP_template_lambda CPP_TEMPLATE_LAMBDA_20
#define CPP_lambda_mut CPP_lambda_mut_20
#define CPP_template_lambda_mut CPP_TEMPLATE_LAMBDA_MUT_20
#endif

// The namespace `ql::concepts` includes concepts that are contained in the
// C++20 standard as well as in `range-v3`.
namespace ql::concepts {

#ifdef QLEVER_CPP_17
using namespace ::concepts;
#else
using namespace std;
#endif

}  // namespace ql::concepts

#endif  // QLEVER_SRC_BACKPORTS_CONCEPTS_H
