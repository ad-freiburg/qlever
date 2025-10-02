//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_BACKPORTS_STARTSWITH_H
#define QLEVER_SRC_BACKPORTS_STARTSWITH_H

#include <string>
#include <string_view>

#include "backports/concepts.h"

namespace ql {

// Backport of C++20's std::string::starts_with and
// std::string_view::starts_with as free functions that take the
// string/string_view as the first parameter.

// Overload 1: starts_with(string_view, string_view)
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

// Overload 2: starts_with(string_view, CharT)
template <typename CharT, typename Traits>
constexpr bool starts_with(std::basic_string_view<CharT, Traits> sv,
                           CharT prefix) noexcept {
  return !sv.empty() && Traits::eq(sv.front(), prefix);
}

// Overload 3: starts_with(string_view, const CharT*)
template <typename CharT, typename Traits>
constexpr bool starts_with(std::basic_string_view<CharT, Traits> sv,
                           const CharT* prefix) {
  return starts_with(sv, std::basic_string_view<CharT, Traits>(prefix));
}

// Overload 4: starts_with(string, string_view)
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

// Overload 5: starts_with(string, CharT)
template <typename CharT, typename Traits, typename Allocator>
constexpr bool starts_with(
    const std::basic_string<CharT, Traits, Allocator>& str,
    CharT prefix) noexcept {
  return starts_with(std::basic_string_view<CharT, Traits>(str), prefix);
}

// Overload 6: starts_with(string, const CharT*)
template <typename CharT, typename Traits, typename Allocator>
constexpr bool starts_with(
    const std::basic_string<CharT, Traits, Allocator>& str,
    const CharT* prefix) {
  return starts_with(std::basic_string_view<CharT, Traits>(str), prefix);
}

}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_STARTSWITH_H
