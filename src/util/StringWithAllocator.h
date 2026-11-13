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
#include <ios>
#include <ostream>
#include <streambuf>
#include <string>
#include <string_view>

#include "util/ContainersWithAllocator.h"

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
  size_t totalSize = target.size();
  for (auto piece : pieces) {
    totalSize += piece.size();
  }
  target.reserve(totalSize);
  for (auto piece : pieces) {
    target.append(piece.data(), piece.size());
  }
}

// A `std::streambuf` that writes to a `qlm::string`. Like the buffer of a
// `std::ostringstream`, it supports seeking the output position backwards, in
// which case subsequent writes overwrite the existing characters.
class StringStreamBuf : public std::streambuf {
  string str_;
  // The current output position.
  size_t pos_ = 0;

 public:
  explicit StringStreamBuf(const Allocator<char>& allocator)
      : str_{allocator} {}

  string& str() { return str_; }
  const string& str() const { return str_; }

 protected:
  int_type overflow(int_type ch) override {
    if (traits_type::eq_int_type(ch, traits_type::eof())) {
      return traits_type::not_eof(ch);
    }
    char c = traits_type::to_char_type(ch);
    xsputn(&c, 1);
    return ch;
  }

  std::streamsize xsputn(const char* s, std::streamsize n) override {
    auto count = static_cast<size_t>(n);
    size_t numOverwritten = std::min(count, str_.size() - pos_);
    str_.replace(pos_, numOverwritten, s, numOverwritten);
    str_.append(s + numOverwritten, count - numOverwritten);
    pos_ += count;
    return n;
  }

  pos_type seekoff(off_type off, std::ios_base::seekdir dir,
                   std::ios_base::openmode which) override {
    const auto failure = pos_type(off_type(-1));
    if (!(which & std::ios_base::out)) {
      return failure;
    }
    off_type base = dir == std::ios_base::beg ? 0
                    : dir == std::ios_base::cur
                        ? static_cast<off_type>(pos_)
                        : static_cast<off_type>(str_.size());
    off_type newPos = base + off;
    if (newPos < 0 || newPos > static_cast<off_type>(str_.size())) {
      return failure;
    }
    pos_ = static_cast<size_t>(newPos);
    return pos_type(newPos);
  }

  pos_type seekpos(pos_type pos, std::ios_base::openmode which) override {
    return seekoff(off_type(pos), std::ios_base::beg, which);
  }
};
}  // namespace detail

// Like `absl::StrCat`, but the result uses `allocator`.
template <typename... Args>
string StrCat(const Allocator<char>& allocator, const Args&... args) {
  string result{allocator};
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

// Like `absl::StrJoin`, but the result uses `allocator`. The `formatter` has
// the same signature as for `absl::StrJoin`.
template <typename Range, typename Formatter>
string StrJoin(const Allocator<char>& allocator, Range&& range,
               std::string_view separator, Formatter&& formatter) {
  string result{allocator};
  std::string element;
  bool first = true;
  for (const auto& value : range) {
    if (!first) {
      result.append(separator);
    }
    first = false;
    element.clear();
    formatter(&element, value);
    result.append(element);
  }
  return result;
}

// Like `absl::StrJoin` with the default formatter.
template <typename Range>
string StrJoin(const Allocator<char>& allocator, Range&& range,
               std::string_view separator) {
  string result{allocator};
  bool first = true;
  for (const auto& value : range) {
    if (!first) {
      result.append(separator);
    }
    first = false;
    StrAppend(&result, value);
  }
  return result;
}

// Like `std::ostringstream`, but the result uses `allocator`. Unlike for a
// `std::ostringstream`, exceptions from the allocator (e.g. when the memory
// limit is exceeded) are rethrown instead of only setting the `badbit`.
class ostringstream : public std::ostream {
  detail::StringStreamBuf buffer_;

 public:
  explicit ostringstream(const Allocator<char>& allocator)
      : std::ostream{nullptr}, buffer_{allocator} {
    rdbuf(&buffer_);
    exceptions(std::ios_base::badbit);
  }

  // The base class stores a pointer to `buffer_`.
  ostringstream(const ostringstream&) = delete;
  ostringstream& operator=(const ostringstream&) = delete;

  string str() const& { return buffer_.str(); }
  string str() && { return std::move(buffer_.str()); }
};

}  // namespace qlm

#endif  // QLEVER_SRC_UTIL_STRINGWITHALLOCATOR_H
