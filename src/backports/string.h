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
#include "util/TypeTraits.h"

namespace ql {

// C++17-compatible backport of C++23's
// `std::basic_string::resize_and_overwrite` as a free function taking the
// string first. Like the standard, `op` is taken by value, called once as an
// rvalue with `(data, count)`, must write at most `count` bytes, must not
// throw, and returns the new size. Unlike the standard (a result larger than
// `count` is UB), the result is checked with `AD_CONTRACT_CHECK` after the
// call. Before C++23 there is no performance benefit: the fallback
// zero-fills all `count` bytes with `resize` first.
CPP_template(typename CharT, typename Traits, typename Allocator,
             typename Operation)(
    requires ad_utility::InvocableWithConvertibleReturnType<
        Operation, size_t, CharT*,
        size_t>) void resize_and_overwrite(std::basic_string<CharT, Traits,
                                                             Allocator>& str,
                                           size_t count, Operation op) {
  // Forward to the standard member when `__cpp_lib_string_resize_and_overwrite
  // >= 202110L` (C++23, P1072R10); otherwise `resize` (zero-fills), overwrite,
  // then shrink.
  // P1072R10 (basic_string::resize_and_overwrite):
  // https://web.archive.org/web/20260102024635/https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2021/p1072r10.html
  // Feature-test macro value 202110L in [version.syn] of the C++ draft:
  // https://web.archive.org/web/20251223023007/http://eel.is/c++draft/version.syn
#if defined(__cpp_lib_string_resize_and_overwrite) && \
    __cpp_lib_string_resize_and_overwrite >= 202110L
  // Throwing inside the operation passed to the standard member is UB, so the
  // result is only recorded there (and clamped to a valid size) and checked
  // after the call returns.
  size_t newSize = 0;
  str.resize_and_overwrite(
      count, [&newSize, op = std::move(op)](CharT* data, size_t n) mutable {
        newSize = std::move(op)(data, n);
        return newSize <= n ? newSize : size_t{0};
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
