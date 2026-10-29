// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_BACKPORTS_STRING_H
#define QLEVER_SRC_BACKPORTS_STRING_H

#include <cstddef>
#include <string>
#include <utility>
#if __has_include(<version>)
#include <version>
#endif

#include "backports/concepts.h"
#include "util/Exception.h"
#include "util/TypeTraits.h"

namespace ql {

// Backport of C++23 `std::basic_string::resize_and_overwrite` as a free
// function (C++17 and C++20). `op` is called once as an rvalue with
// `(data, count)`, must not throw, and returns the new size (at most `count`,
// checked after the call; on a violation the string keeps size `count` with
// the written contents). Treat the buffer as uninitialized and use the pointer
// only during the call. Before C++23 the fallback `resize` value-initializes
// the characters first, so there is no performance benefit there.
CPP_template(typename CharT, typename Traits, typename Allocator,
             typename Operation)(
    requires ad_utility::InvocableWithConvertibleReturnType<Operation, size_t,
                                                            CharT*, size_t>)
void resize_and_overwrite(std::basic_string<CharT, Traits, Allocator>& str,
                          size_t count, Operation op) {
  // Feature-test macro from P1072R10 (`[version.syn]`).
#if defined(__cpp_lib_string_resize_and_overwrite) && \
    __cpp_lib_string_resize_and_overwrite >= 202110L
  // The result reported to the standard member is clamped to `n` (an
  // oversize result would violate its contract); the raw value is checked
  // after the call, because throwing inside the member's operation is UB.
  size_t newSize = 0;
  str.resize_and_overwrite(
      count, [&newSize, op = std::move(op)](CharT* data, size_t n) mutable {
        newSize = std::move(op)(data, n);
        return newSize <= n ? newSize : n;
      });
  AD_CONTRACT_CHECK(newSize <= count);
#else
  str.resize(count);
  const size_t newSize = std::move(op)(str.data(), count);
  AD_CONTRACT_CHECK(newSize <= count);
  str.resize(newSize);
#endif
}

}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_STRING_H
