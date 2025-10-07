// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_BACKPORTS_STARTSWITHANDENDSWITH_H
#define QLEVER_SRC_BACKPORTS_STARTSWITHANDENDSWITH_H

#include <string>
#include <string_view>

#include "backports/concepts.h"

namespace ql {

// Backport of C++20's std::string::starts_with and
// std::string_view::starts_with as free functions that take the
// string/string_view as the first parameter.

// Overload 1: Does a `string_view` start with a `string_view`.
CPP_template(typename CharT, typename Traits, typename Other)(
    requires ql::concepts::convertible_to<
        const Other&,
        std::basic_string_view<
            CharT, Traits>>) constexpr bool starts_with(const std::
                                                            basic_string_view<
                                                                CharT, Traits>&
                                                                sv,
                                                        const Other&
                                                            prefixIn) noexcept {
  std::basic_string_view<CharT, Traits> prefix(prefixIn);
  return sv.size() >= prefix.size() &&
         Traits::compare(sv.data(), prefix.data(), prefix.size()) == 0;
}

// Overload 2: Does a `string_view` start with a `char`.
CPP_template(typename CharT, typename Traits, typename Other)(
    requires ql::concepts::convertible_to<
        const Other&,
        CharT>) constexpr bool starts_with(std::basic_string_view<CharT, Traits>
                                               sv,
                                           const Other& prefixIn) noexcept {
  CharT prefix(prefixIn);
  return !sv.empty() && Traits::eq(sv.front(), prefix);
}

// Overload 3: Does a `string` start with a `string_view`.
CPP_template(typename CharT, typename Traits, typename Allocator,
             typename Other)(
    requires ql::concepts::convertible_to<
        const Other&,
        std::basic_string_view<
            CharT, Traits>>) constexpr bool starts_with(const std::
                                                            basic_string<
                                                                CharT, Traits,
                                                                Allocator>& str,
                                                        const Other&
                                                            prefix) noexcept {
  return starts_with(std::basic_string_view<CharT, Traits>(str), prefix);
}

// Overload 3: Does a `string` start with a `char`.
CPP_template(typename CharT, typename Traits, typename Allocator,
             typename Other)(
    requires ql::concepts::convertible_to<
        const Other&,
        CharT>) constexpr bool starts_with(const std::basic_string<CharT,
                                                                   Traits,
                                                                   Allocator>&
                                               str,
                                           const Other& prefix) noexcept {
  return starts_with(std::basic_string_view<CharT, Traits>(str), prefix);
}

// Backport of C++20's std::string::ends_with and
// std::string_view::ends_with as free functions that take the
// string/string_view as the first parameter.

// Overload 1: Does a `string_view` end with a `string_view`.
CPP_template(typename CharT, typename Traits, typename Other)(
    requires ql::concepts::convertible_to<
        const Other&,
        std::basic_string_view<
            CharT, Traits>>) constexpr bool ends_with(const std::
                                                          basic_string_view<
                                                              CharT, Traits>&
                                                              sv,
                                                      const Other&
                                                          suffixIn) noexcept {
  std::basic_string_view<CharT, Traits> suffix(suffixIn);
  return sv.size() >= suffix.size() &&
         Traits::compare(sv.data() + (sv.size() - suffix.size()), suffix.data(),
                         suffix.size()) == 0;
}

// Overload 2: Does a `string_view` end with a `char`.
CPP_template(typename CharT, typename Traits, typename Other)(
    requires ql::concepts::convertible_to<
        const Other&,
        CharT>) constexpr bool ends_with(std::basic_string_view<CharT, Traits>
                                             sv,
                                         const Other& suffixIn) noexcept {
  CharT suffix(suffixIn);
  return !sv.empty() && Traits::eq(sv.back(), suffix);
}

// Overload 3: Does a `string` end with a `string_view`.
CPP_template(typename CharT, typename Traits, typename Allocator,
             typename Other)(
    requires ql::concepts::convertible_to<
        const Other&,
        std::basic_string_view<
            CharT,
            Traits>>) constexpr bool ends_with(const std::
                                                   basic_string<CharT, Traits,
                                                                Allocator>& str,
                                               const Other& suffix) noexcept {
  return ends_with(std::basic_string_view<CharT, Traits>(str), suffix);
}

// Overload 4: Does a `string` end with a `char`.
CPP_template(typename CharT, typename Traits, typename Allocator,
             typename Other)(
    requires ql::concepts::convertible_to<
        const Other&,
        CharT>) constexpr bool ends_with(const std::basic_string<CharT, Traits,
                                                                 Allocator>&
                                             str,
                                         const Other& suffix) noexcept {
  return ends_with(std::basic_string_view<CharT, Traits>(str), suffix);
}

}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_STARTSWITHANDENDSWITH_H
