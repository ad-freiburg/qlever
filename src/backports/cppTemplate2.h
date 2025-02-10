//
// Created by kalmbacj on 2/10/25.
//

#pragma once
#include <concepts/concepts.hpp>
// A template with a requires clause
/// INTERNAL ONLY
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

#define CPP_template_2(...)        \
  CPP_PP_IGNORE_CXX2A_COMPAT_BEGIN \
  template <__VA_ARGS__ CPP_TEMPLATE_2_SFINAE_AUX_
