// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

// Clang up to version 13 does not link again libstdc++,
// because coroutines are still expected in the std::experimental
// namespace. This is a helper file that overcomes this limitation,
// simply include this file as a replacement of <coroutine>

// TODO<LLVM14> Remove this file as soon as we have a stable
// clang version that doesn't require this hack.
#ifndef QLEVER_COROUTINES_H
#define QLEVER_COROUTINES_H

#include <coroutine>

#endif  // QLEVER_COROUTINES_H
