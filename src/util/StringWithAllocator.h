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

#include <algorithm>
#include <array>
#include <functional>
#include <ios>
#include <ostream>
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

namespace detail {
// A `std::basic_stringbuf` that writes to a `qlm::string` and grows the string
// itself, s.t. exceptions of the allocator (e.g. an
// `AllocationExceedsLimitException` when the memory limit is exceeded)
// propagate to the caller.
//
// NOTE: libc++ catches all exceptions while growing the string in `overflow`
// and only reports a failure, so the original exception would otherwise be
// lost.
class StringBuf : public std::basic_stringbuf<char, std::char_traits<char>,
                                              Allocator<char>> {
  using Base =
      std::basic_stringbuf<char, std::char_traits<char>, Allocator<char>>;

 public:
  // NOTE: The `ate` mode makes `str(...)` (see `grow` below) place the output
  // position at the end of the string instead of at its beginning.
  explicit StringBuf(Allocator<char> allocator)
      : Base{string{std::move(allocator)},
             std::ios_base::out | std::ios_base::ate} {}

  // In C++17 mode, the plain `overflow` of `Base` is used, which on libstdc++
  // (the only standard library supported for C++17) propagates the exceptions
  // of the allocator anyway.
  //
  // NOTE 1: `view()`, `get_allocator()` and `str(string&&)` were only added in
  // C++20, without them `grow` would copy the string twice.
  //
  // NOTE 2: The check is on the language version and not on `QLEVER_CPP_17`,
  // which is also defined for some C++20 builds (see `CMakeLists.txt`).
#if __cplusplus >= 202002L
 protected:
  int_type overflow(int_type ch) override {
    if (!traits_type::eq_int_type(ch, traits_type::eof()) &&
        this->pptr() == this->epptr()) {
      grow();
    }
    // There now is space for `ch`, so `Base::overflow` doesn't allocate.
    return Base::overflow(ch);
  }

 private:
  // Replace the underlying string by a copy with twice the capacity, like a
  // `std::string` does when it grows. `Base` uses the full capacity of the
  // string as its output buffer. If the allocation fails, `Base` is unchanged.
  //
  // NOTE: We don't use `std::move(*this).str()` to reuse the string, because
  // libc++ before version 18 default-constructs the allocator there.
  void grow() {
    static constexpr size_t minCapacity = 32;
    auto content = this->view();
    string grown{this->get_allocator()};
    grown.reserve(std::max(
        2 * static_cast<size_t>(this->epptr() - this->pbase()), minCapacity));
    grown.append(content.data(), content.size());
    this->str(std::move(grown));
  }
#endif
};
}  // namespace detail

// Like `std::ostringstream`, but the result uses `allocator`. Unlike for a
// `std::ostringstream`, allocation failures (e.g. when the memory limit is
// exceeded) rethrow the exception of the allocator instead of only setting the
// `badbit` (see `detail::StringBuf` above).
//
// NOTE: In C++17 mode and with libc++ before version 18, `std::move(os).str()`
// copies the string, because the overload of `str()` for rvalues was only added
// in C++20, and libc++ before version 18 default-constructs the allocator in
// it.
class ostringstream : public std::ostream {
  detail::StringBuf buffer_;

 public:
  explicit ostringstream(Allocator<char> allocator)
      : std::ostream{nullptr}, buffer_{std::move(allocator)} {
    rdbuf(&buffer_);
    exceptions(std::ios_base::badbit);
  }

  // The base class stores a pointer to `buffer_`.
  ostringstream(const ostringstream&) = delete;
  ostringstream& operator=(const ostringstream&) = delete;

  string str() const& { return buffer_.str(); }
  string str() && {
#if defined(_LIBCPP_VERSION) && _LIBCPP_VERSION < 180000
    return buffer_.str();
#else
    return std::move(buffer_).str();
#endif
  }
};

}  // namespace qlm

#endif  // QLEVER_SRC_UTIL_STRINGWITHALLOCATOR_H
