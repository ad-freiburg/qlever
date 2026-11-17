// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_STRINGWITHALLOCATOR_H
#define QLEVER_SRC_UTIL_STRINGWITHALLOCATOR_H

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <array>
#include <functional>
#include <ios>
#include <range/v3/numeric/accumulate.hpp>
#include <sstream>
#include <string>
#include <string_view>

#include "util/ContainersWithAllocator.h"
#include "util/Forward.h"

// Drop-in replacements for `std::string`, `std::ostringstream`,
// `absl::StrCat`, `absl::StrAppend` and `absl::StrJoin` that allocate via a
// `qlm::Allocator` (see `util/ContainersWithAllocator.h`).
namespace qlm {

using string = std::basic_string<char, std::char_traits<char>, Allocator<char>>;

namespace detail {
// Append all `pieces` to `target` with a single reallocation.
template <size_t N>
void appendPieces(string& target,
                  const std::array<absl::string_view, N>& pieces) {
  target.reserve(::ranges::accumulate(
      pieces, target.size(), std::plus<>{},
      [](absl::string_view piece) { return piece.size(); }));
  for (auto piece : pieces) {
    target.append(piece.data(), piece.size());
  }
}
}  // namespace detail

// Like `absl::StrCat`, but the result uses `allocator`.
template <typename... Args>
string StrCat(Allocator<char> allocator, const Args&... args) {
  string result{std::move(allocator)};
  // The `AlphaNum` temporaries (which may own the formatted digits) live until
  // the end of the full-expression, so the pieces stay valid.
  detail::appendPieces(result, std::array<absl::string_view, sizeof...(Args)>{
                                   absl::AlphaNum(args).Piece()...});
  return result;
}

// Like `absl::StrAppend`, but for a `qlm::string`.
template <typename... Args>
void StrAppend(string* target, const Args&... args) {
  detail::appendPieces(*target, std::array<absl::string_view, sizeof...(Args)>{
                                    absl::AlphaNum(args).Piece()...});
}

namespace detail {
// Join the elements of `range` with `separator` into a `qlm::string` that uses
// `allocator`. Each element is appended to the result via
// `appendElement(result, element)`.
template <typename Range, typename AppendElement>
string joinImpl(Allocator<char> allocator, Range&& range,
                std::string_view separator, AppendElement appendElement) {
  string result{std::move(allocator)};
  bool first = true;
  for (const auto& value : range) {
    if (!first) {
      result.append(separator);
    }
    first = false;
    appendElement(result, value);
  }
  return result;
}
}  // namespace detail

// Like `absl::StrJoin`, but the result uses `allocator`. The `formatter` has
// the same signature as for `absl::StrJoin`.
template <typename Range, typename Formatter>
string StrJoin(Allocator<char> allocator, Range&& range,
               std::string_view separator, Formatter&& formatter) {
  // The `formatter` writes to a `std::string`, which is reused for all
  // elements.
  return detail::joinImpl(std::move(allocator), AD_FWD(range), separator,
                          [&formatter, element = std::string{}](
                              string& result, const auto& value) mutable {
                            element.clear();
                            formatter(&element, value);
                            result.append(element);
                          });
}

// Like `absl::StrJoin` with the default formatter.
template <typename Range>
string StrJoin(Allocator<char> allocator, Range&& range,
               std::string_view separator) {
  return detail::joinImpl(
      std::move(allocator), AD_FWD(range), separator,
      [](string& result, const auto& value) { StrAppend(&result, value); });
}

// Like `std::ostringstream`, but the result uses `allocator`. Unlike for a
// `std::ostringstream`, allocation failures (e.g. when the memory limit is
// exceeded) throw instead of only setting the `badbit`. With libstdc++, the
// exception of the allocator is rethrown. With libc++, whose `std::stringbuf`
// swallows that exception, a `std::ios_base::failure` is thrown instead.
//
// NOTE: In C++17 mode, `std::move(os).str()` copies the string, because the
// overload of `str()` for rvalues was only added in C++20.
class ostringstream
    : public std::basic_ostringstream<char, std::char_traits<char>,
                                      Allocator<char>> {
  using Base =
      std::basic_ostringstream<char, std::char_traits<char>, Allocator<char>>;

 public:
  // NOTE: The constructor of `Base` that only takes an allocator was only added
  // in C++20, so we pass an empty string with the `allocator` instead.
  explicit ostringstream(Allocator<char> allocator)
      : Base{string{std::move(allocator)}} {
    exceptions(std::ios_base::badbit);
  }
};

}  // namespace qlm

#endif  // QLEVER_SRC_UTIL_STRINGWITHALLOCATOR_H
