// Copyright 2026, The QLever Authors, in particular:
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

#include "backports/concepts.h"
#include "util/Exception.h"

namespace ql {

// C++17-compatible backport of C++23's
// `std::basic_string::resize_and_overwrite` as a free function taking the
// string first. Unlike the standard (oversize result is UB), both branches
// enforce `newSize <= count` via `AD_CONTRACT_CHECK`.
CPP_template(typename CharT, typename Traits, typename Allocator,
             typename Operation)(
    requires ql::concepts::invocable<Operation, CharT*, size_t>&&
        ql::concepts::convertible_to<
            decltype(std::declval<Operation>()(std::declval<CharT*>(),
                                               std::declval<size_t>())),
            size_t>) void resize_and_overwrite(std::basic_string<CharT, Traits,
                                                                 Allocator>&
                                                   str,
                                               size_t count, Operation&& op) {
  // Forward to the standard member when `__cpp_lib_string_resize_and_overwrite
  // >= 202110L` (C++23, P1072R10); otherwise `resize` (zero-fills), overwrite,
  // then shrink. Same end state; only the fallback pays for the fill.
  // P1072R10 (basic_string::resize_and_overwrite):
  // https://web.archive.org/web/20260102024635/https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2021/p1072r10.html
  // Feature-test macro value 202110L in [version.syn] of the C++ draft:
  // https://web.archive.org/web/20251223023007/http://eel.is/c++draft/version.syn
#if defined(__cpp_lib_string_resize_and_overwrite) && \
    __cpp_lib_string_resize_and_overwrite >= 202110L
  // Move `op` into the lambda (standard takes it by value as
  // `std::move(op)(p, count)`); `mutable` keeps mutable callables working.
  str.resize_and_overwrite(count, [op = std::forward<Operation>(op), count](
                                      CharT* data, size_t n) mutable {
    const size_t newSize = std::move(op)(data, n);
    AD_CONTRACT_CHECK(newSize <= count);
    return newSize;
  });
#else
  str.resize(count);
  const size_t newSize = std::forward<Operation>(op)(str.data(), count);
  AD_CONTRACT_CHECK(newSize <= count);
  str.resize(newSize);
#endif
}

}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_STRING_H
