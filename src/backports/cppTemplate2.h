// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_BACKPORTS_CPPTEMPLATE2_H
#define QLEVER_SRC_BACKPORTS_CPPTEMPLATE2_H

#include <concepts/concepts.hpp>

// The internal implementation of a `CPP_variadic_class_template` macro,
// That can be used for variadic template classes.
#define CPP_TEMPLATE_VARIADIC_CLASS_SFINAE_AUX_0(...)                 \
  , typename = std::enable_if_t <                                     \
               CPP_PP_CAT(CPP_TEMPLATE_SFINAE_AUX_3_, __VA_ARGS__) >> \
               CPP_PP_IGNORE_CXX2A_COMPAT_END

#define CPP_TEMPLATE_VARIADIC_CLASS_SFINAE_AUX_3_requires

#define CPP_TEMPLATE_VARIADIC_CLASS_SFINAE_AUX_WHICH_(FIRST, ...) \
  CPP_PP_EVAL(CPP_PP_CHECK,                                       \
              CPP_PP_CAT(CPP_TEMPLATE_SFINAE_PROBE_CONCEPT_, FIRST))

#define CPP_TEMPLATE_VARIADIC_CLASS_SFINAE_AUX_(...)                       \
  CPP_PP_CAT(CPP_TEMPLATE_VARIADIC_CLASS_SFINAE_AUX_,                      \
             CPP_TEMPLATE_VARIADIC_CLASS_SFINAE_AUX_WHICH_(__VA_ARGS__, )) \
  (__VA_ARGS__)

#define CPP_template_VARIADIC_CLASS_SFINAE(...) \
  CPP_PP_IGNORE_CXX2A_COMPAT_BEGIN              \
  template <__VA_ARGS__ CPP_TEMPLATE_VARIADIC_CLASS_SFINAE_AUX_

// The internal reimplementation of a `CPP_variadic_template` macro
// that can be used for variadic template functions.
#define CPP_TEMPLATE_NO_DEFAULT_SFINAE_AUX_0(...)                         \
  , std::enable_if_t<CPP_PP_CAT(CPP_TEMPLATE_SFINAE_AUX_3_, __VA_ARGS__), \
                     int> = 0 > CPP_PP_IGNORE_CXX2A_COMPAT_END

#define CPP_TEMPLATE_NO_DEFAULT_SFINAE_AUX_3_requires

#define CPP_TEMPLATE_NO_DEFAULT_SFINAE_AUX_WHICH_(FIRST, ...) \
  CPP_PP_EVAL(CPP_PP_CHECK,                                   \
              CPP_PP_CAT(CPP_TEMPLATE_SFINAE_PROBE_CONCEPT_, FIRST))

#define CPP_TEMPLATE_NO_DEFAULT_SFINAE_AUX_(...)                       \
  CPP_PP_CAT(CPP_TEMPLATE_NO_DEFAULT_SFINAE_AUX_,                      \
             CPP_TEMPLATE_NO_DEFAULT_SFINAE_AUX_WHICH_(__VA_ARGS__, )) \
  (__VA_ARGS__)

#define CPP_template_NO_DEFAULT_SFINAE(...) \
  CPP_PP_IGNORE_CXX2A_COMPAT_BEGIN          \
  template <__VA_ARGS__ CPP_TEMPLATE_NO_DEFAULT_SFINAE_AUX_

// The internal reimplementation of a `CPP_template_2` and `CPP_and_2` macro
// that can be used for class members when the outer class has already been
// constrained using `CPP_template`.
#define CPP_TEMPLATE_2_SFINAE_AUX_0(...)                                \
  , bool CPP_true_2 = true,                                             \
         std::enable_if_t <                                             \
                 CPP_PP_CAT(CPP_TEMPLATE_SFINAE_AUX_3_, __VA_ARGS__) && \
             CPP_BOOL(CPP_true_2),                                      \
         int > = 0 > CPP_PP_IGNORE_CXX2A_COMPAT_END

#define CPP_TEMPLATE_2_SFINAE_AUX_3_requires

#define CPP_TEMPLATE_2_SFINAE_AUX_WHICH_(FIRST, ...) \
  CPP_PP_EVAL(CPP_PP_CHECK,                          \
              CPP_PP_CAT(CPP_TEMPLATE_SFINAE_PROBE_CONCEPT_, FIRST))

#define CPP_TEMPLATE_2_SFINAE_AUX_(...)                       \
  CPP_PP_CAT(CPP_TEMPLATE_2_SFINAE_AUX_,                      \
             CPP_TEMPLATE_2_SFINAE_AUX_WHICH_(__VA_ARGS__, )) \
  (__VA_ARGS__)

#define CPP_template_2_SFINAE(...) \
  CPP_PP_IGNORE_CXX2A_COMPAT_BEGIN \
  template <__VA_ARGS__ CPP_TEMPLATE_2_SFINAE_AUX_

/// INTERNAL ONLY
#define CPP_and_def_sfinae &&CPP_BOOL(CPP_true), int >, std::enable_if_t <
#define CPP_and_2_sfinae &&CPP_BOOL(CPP_true_2), int > = 0, std::enable_if_t <
#define CPP_and_2_def_sfinae &&CPP_BOOL(CPP_true_2), int >, std::enable_if_t <

#define CPP_member_def_sfinae \
  template <bool (&CPP_true_fn)(::concepts::detail::xNil)>

#define CPP_LAMBDA_20(...) [__VA_ARGS__] CPP_LAMBDA_ARGS

#define CPP_TEMPLATE_LAMBDA_20(...) [__VA_ARGS__] CPP_TEMPLATE_LAMBDA_ARGS

#define CPP_TEMPLATE_LAMBDA_MUT_20(...) \
  [__VA_ARGS__] CPP_TEMPLATE_LAMBDA_MUT_ARGS

// The internals of the `CPP_lambda` template
#define CPP_LAMBDA_SFINAE_ARGS(...) \
(__VA_ARGS__ CPP_LAMBDA_SFINAE_AUX_

#define CPP_LAMBDA_SFINAE_AUX_WHICH_(FIRST, ...) \
  CPP_PP_EVAL(CPP_PP_CHECK, FIRST)
//  CPP_PP_EVAL(CPP_PP_CHECK, CPP_PP_CAT(CPP_LAMBDA_SFINAE_PROBE_CONCEPT_,
//  FIRST))

#define CPP_LAMBDA_SFINAE_AUX_(...)                       \
  CPP_PP_CAT(CPP_LAMBDA_SFINAE_AUX_,                      \
             CPP_LAMBDA_SFINAE_AUX_WHICH_(__VA_ARGS__, )) \
  (__VA_ARGS__)

#define CPP_LAMBDA_SFINAE_AUX_0(...) ,                           \
std::enable_if_t<                                            \
CPP_PP_CAT(CPP_LAMBDA_SFINAE_AUX_3_, __VA_ARGS__)        \
>* = nullptr)

#define CPP_lambda_sfinae(...)     \
  CPP_PP_IGNORE_CXX2A_COMPAT_BEGIN \
  [__VA_ARGS__] CPP_LAMBDA_SFINAE_ARGS

#define CPP_TEMPLATE_LAMBDA_ARGS_sfinae(...) \
  <__VA_ARGS__> CPP_LAMBDA_SFINAE_ARGS

#define CPP_TEMPLATE_LAMBDA_MUT_ARGS_sfinae(...) \
  <__VA_ARGS__> CPP_LAMBDA_MUT_SFINAE_ARGS

#define CPP_template_lambda_sfinae(...) \
  [__VA_ARGS__] CPP_TEMPLATE_LAMBDA_ARGS_sfinae

#define CPP_template_lambda_mut_sfinae(...) \
  [__VA_ARGS__] CPP_TEMPLATE_LAMBDA_MUT_ARGS_sfinae

#define CPP_LAMBDA_SFINAE_AUX_3_requires

#define CPP_LAMBDA_ARGS(...) (__VA_ARGS__) CPP_LAMBDA_AUX_

#define CPP_LAMBDA_AUX_(...) \
  CPP_PP_CAT(CPP_LAMBDA_AUX_, CPP_LAMBDA_AUX_WHICH_(__VA_ARGS__, ))(__VA_ARGS__)

#define CPP_LAMBDA_AUX_WHICH_(FIRST, ...) CPP_PP_EVAL(CPP_PP_CHECK, FIRST)

#define CPP_LAMBDA_AUX_0(...) __VA_ARGS__

#define CPP_TEMPLATE_LAMBDA_ARGS(...) <__VA_ARGS__> CPP_LAMBDA_ARGS

#define CPP_TEMPLATE_LAMBDA_MUT_ARGS(...) <__VA_ARGS__> CPP_LAMBDA_ARGS_MUT

#define CPP_lambda_mut_sfinae(...) \
  CPP_PP_IGNORE_CXX2A_COMPAT_BEGIN \
  [__VA_ARGS__] CPP_LAMBDA_MUT_SFINAE_ARGS

#define CPP_LAMBDA_MUT_SFINAE_ARGS(...) \
(__VA_ARGS__ CPP_LAMBDA_MUT_SFINAE_AUX_

#define CPP_LAMBDA_MUT_SFINAE_AUX_(...)                   \
  CPP_PP_CAT(CPP_LAMBDA_SFINAE_AUX_,                      \
             CPP_LAMBDA_SFINAE_AUX_WHICH_(__VA_ARGS__, )) \
  (__VA_ARGS__) mutable

#define CPP_LAMBDA_ARGS_MUT(...) (__VA_ARGS__) mutable CPP_LAMBDA_AUX_
#define CPP_lambda_mut_20(...) [__VA_ARGS__] CPP_LAMBDA_ARGS_MUT

#endif  // QLEVER_SRC_BACKPORTS_CPPTEMPLATE2_H
