// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#pragma once

// The macros in this file provide automatic generation of if clauses that
// enable transformation of runtime variables to a limited range of compile
// time values and a default case. Currently this means that
// CALL_FIXED_SIZE_1(i, func, ...) calls func with i as the first template
// parameter if i is in the range [1;5] and with 0 as the first template
// parameter otherwise. Any further template parameters can currently only be
// set by the compiler using template argument deduction.
// This feature is implemented using macros as passing templated but not
// instantiated functions as function or template parameters is not possible
// in C++ 17.

// =============================================================================
// One Variable
// =============================================================================
#define CALL_FIXED_SIZE_1(i, func, ...) \
  if (i == 1) {                         \
    func<1>(__VA_ARGS__);               \
  } else if (i == 2) {                  \
    func<2>(__VA_ARGS__);               \
  } else if (i == 3) {                  \
    func<3>(__VA_ARGS__);               \
  } else if (i == 4) {                  \
    func<4>(__VA_ARGS__);               \
  } else if (i == 5) {                  \
    func<5>(__VA_ARGS__);               \
  } else if (i == 6) {                  \
    func<6>(__VA_ARGS__);               \
  } else if (i == 7) {                  \
    func<7>(__VA_ARGS__);               \
  } else if (i == 8) {                  \
    func<8>(__VA_ARGS__);               \
  } else if (i == 9) {                  \
    func<9>(__VA_ARGS__);               \
  } else if (i == 10) {                 \
    func<10>(__VA_ARGS__);              \
  } else {                              \
    func<0>(__VA_ARGS__);               \
  }

// =============================================================================
// Two Variables
// =============================================================================
#define _CALL_FIXED_SIZE_2_i(i, j, func, ...) \
  if (j == 1) {                               \
    func<i, 1>(__VA_ARGS__);                  \
  } else if (j == 2) {                        \
    func<i, 2>(__VA_ARGS__);                  \
  } else if (j == 3) {                        \
    func<i, 3>(__VA_ARGS__);                  \
  } else if (j == 4) {                        \
    func<i, 4>(__VA_ARGS__);                  \
  } else if (j == 5) {                        \
    func<i, 5>(__VA_ARGS__);                  \
  } else {                                    \
    func<i, 0>(__VA_ARGS__);                  \
  }

#define CALL_FIXED_SIZE_2(i, j, func, ...)        \
  if (i == 1) {                                   \
    _CALL_FIXED_SIZE_2_i(1, j, func, __VA_ARGS__) \
  } else if (i == 2) {                            \
    _CALL_FIXED_SIZE_2_i(2, j, func, __VA_ARGS__) \
  } else if (i == 3) {                            \
    _CALL_FIXED_SIZE_2_i(3, j, func, __VA_ARGS__) \
  } else if (i == 4) {                            \
    _CALL_FIXED_SIZE_2_i(4, j, func, __VA_ARGS__) \
  } else if (i == 5) {                            \
    _CALL_FIXED_SIZE_2_i(5, j, func, __VA_ARGS__) \
  } else {                                        \
    _CALL_FIXED_SIZE_2_i(0, j, func, __VA_ARGS__) \
  }

// =============================================================================
// Three Variables
// =============================================================================
#define _CALL_FIXED_SIZE_3_i_j(i, j, k, func, ...) \
  if (k == 1) {                                    \
    func<i, j, 1>(__VA_ARGS__);                    \
  } else if (k == 2) {                             \
    func<i, j, 2>(__VA_ARGS__);                    \
  } else if (k == 3) {                             \
    func<i, j, 3>(__VA_ARGS__);                    \
  } else if (k == 4) {                             \
    func<i, j, 4>(__VA_ARGS__);                    \
  } else if (k == 5) {                             \
    func<i, j, 5>(__VA_ARGS__);                    \
  } else {                                         \
    func<i, j, 0>(__VA_ARGS__);                    \
  }

#define _CALL_FIXED_SIZE_3_i(i, j, k, func, ...)       \
  if (j == 1) {                                        \
    _CALL_FIXED_SIZE_3_i_j(i, 1, k, func, __VA_ARGS__) \
  } else if (j == 2) {                                 \
    _CALL_FIXED_SIZE_3_i_j(i, 2, k, func, __VA_ARGS__) \
  } else if (j == 3) {                                 \
    _CALL_FIXED_SIZE_3_i_j(i, 3, k, func, __VA_ARGS__) \
  } else if (j == 4) {                                 \
    _CALL_FIXED_SIZE_3_i_j(i, 4, k, func, __VA_ARGS__) \
  } else if (j == 5) {                                 \
    _CALL_FIXED_SIZE_3_i_j(i, 5, k, func, __VA_ARGS__) \
  } else {                                             \
    _CALL_FIXED_SIZE_3_i_j(i, 0, k, func, __VA_ARGS__) \
  }

#define CALL_FIXED_SIZE_3(i, j, k, func, ...)        \
  if (i == 1) {                                      \
    _CALL_FIXED_SIZE_3_i(1, j, k, func, __VA_ARGS__) \
  } else if (i == 2) {                               \
    _CALL_FIXED_SIZE_3_i(2, j, k, func, __VA_ARGS__) \
  } else if (i == 3) {                               \
    _CALL_FIXED_SIZE_3_i(3, j, k, func, __VA_ARGS__) \
  } else if (i == 4) {                               \
    _CALL_FIXED_SIZE_3_i(4, j, k, func, __VA_ARGS__) \
  } else if (i == 5) {                               \
    _CALL_FIXED_SIZE_3_i(5, j, k, func, __VA_ARGS__) \
  } else {                                           \
    _CALL_FIXED_SIZE_3_i(0, j, k, func, __VA_ARGS__) \
  }
