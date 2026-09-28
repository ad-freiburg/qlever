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

// Provide a C++17-compatible backport of C++23's
// `std::basic_string::resize_and_overwrite` as a free function that takes the
// string as the first parameter. Deliberate deviation: the standard leaves a
// returned size above `count` as undefined behavior, while this backport
// enforces the same bound with `AD_CONTRACT_CHECK` on both branches, so a
// violating operation fails loudly instead of corrupting the string.
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
#if defined(__cpp_lib_string_resize_and_overwrite) && \
    __cpp_lib_string_resize_and_overwrite >= 202110L
  // Move `op` into the lambda like the standard, which takes its operation
  // by value and invokes it as `std::move(op)(p, count)`. Capturing the
  // forwarding reference by reference would dangle for move-only rvalue
  // callables. The lambda is `mutable` so mutable callables keep working.
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
