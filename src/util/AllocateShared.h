// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ALLOCATESHARED_H
#define QLEVER_SRC_UTIL_ALLOCATESHARED_H

#include <memory>

#include "util/Forward.h"

// Define a member function template `makeShared<T>(args...)` which behaves
// exactly like `std::make_shared<T>(args...)`, but allocates the object
// (together with the control block of the `shared_ptr`) using the allocator
// that the expression `allocatorSnippet` evaluates to. That way the memory
// limit of a query also applies to the objects that are allocated on the heap
// while the query is planned and executed.
//
// The `allocatorSnippet` is evaluated in the scope of the enclosing class, so
// it typically refers to one of its members, for example
// `DEFINE_MAKE_SHARED_MEMBER(qec_->getAllocator())`.
#define DEFINE_MAKE_SHARED_MEMBER(allocatorSnippet)                    \
  template <typename T, typename... Args>                              \
  std::shared_ptr<T> makeShared(Args&&... args) const {                \
    return std::allocate_shared<T>(allocatorSnippet, AD_FWD(args)...); \
  }

#endif  // QLEVER_SRC_UTIL_ALLOCATESHARED_H
