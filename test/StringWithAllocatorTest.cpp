// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "util/MemoryLimitTracker.h"
#include "util/StringWithAllocator.h"

using namespace ad_utility::memory_literals;
using ad_utility::detail::AllocationExceedsLimitException;

namespace {
auto unlimited() { return qlever::makeUnlimitedAllocator<char>(); }

// An allocator that can't hold more than a few bytes.
auto tiny() { return qlever::makeAllocatorWithLimit<char>(8_B); }

// `qlm::string` and `std::string` cannot be compared directly.
std::string toStd(const qlm::string& s) { return std::string{s}; }

constexpr std::string_view longString =
    "a string that is too long for the tiny allocator";
}  // namespace

// _____________________________________________________________________________
TEST(StringWithAllocator, strCat) {
  auto alloc = unlimited();
  qlm::string q{"qlm", alloc};
  std::string s = "std";
  auto result =
      qlm::StrCat(alloc, "a", 42, -3, 1.5, s, q, std::string_view{"v"});
  EXPECT_EQ(toStd(result), absl::StrCat("a", 42, -3, 1.5, s, q, "v"));
  EXPECT_EQ(result.get_allocator(), alloc);
  EXPECT_EQ(qlm::StrCat(alloc), "");

  EXPECT_THROW(qlm::StrCat(tiny(), longString),
               AllocationExceedsLimitException);
}

// _____________________________________________________________________________
TEST(StringWithAllocator, strAppend) {
  qlm::string result{"start", unlimited()};
  qlm::StrAppend(&result, " ", 7, " ", "end");
  EXPECT_EQ(result, "start 7 end");
  qlm::StrAppend(&result);
  EXPECT_EQ(result, "start 7 end");

  qlm::string small{tiny()};
  EXPECT_THROW(qlm::StrAppend(&small, longString),
               AllocationExceedsLimitException);
}

// _____________________________________________________________________________
TEST(StringWithAllocator, strJoin) {
  auto alloc = unlimited();
  std::vector<int> ints{1, 2, 3};
  EXPECT_EQ(toStd(qlm::StrJoin(alloc, ints, ", ")), absl::StrJoin(ints, ", "));
  EXPECT_EQ(qlm::StrJoin(alloc, std::vector<int>{}, ", "), "");

  auto formatter = [](std::string* out, int i) {
    absl::StrAppend(out, "<", i, ">");
  };
  EXPECT_EQ(toStd(qlm::StrJoin(alloc, ints, "-", formatter)),
            absl::StrJoin(ints, "-", formatter));
  EXPECT_EQ(qlm::StrJoin(alloc, std::vector<int>{}, "-", formatter), "");

  // Ranges that are not `const`-iterable work as well.
  auto view = ql::views::transform(ints, [](int i) { return i * 2; });
  EXPECT_EQ(qlm::StrJoin(alloc, view, ","), "2,4,6");

  std::vector<std::string_view> strings{longString, longString};
  EXPECT_THROW(qlm::StrJoin(tiny(), strings, ","),
               AllocationExceedsLimitException);
  EXPECT_THROW(qlm::StrJoin(tiny(), ints, ",",
                            [](std::string* out, int) {
                              absl::StrAppend(out, longString);
                            }),
               AllocationExceedsLimitException);
}

// _____________________________________________________________________________
TEST(StringWithAllocator, ostringstream) {
  auto alloc = unlimited();
  qlm::ostringstream os{alloc};
  std::ostringstream expected;
  auto write = [](std::ostream& stream) {
    stream << "text " << 42 << ' ' << 1.25 << ' ' << std::string{"str"} << '\n';
  };
  write(os);
  write(expected);
  EXPECT_EQ(toStd(os.str()), expected.str());
  EXPECT_EQ(os.str().get_allocator(), alloc);
  auto moved = std::move(os).str();
  EXPECT_EQ(toStd(moved), expected.str());

  // Allocation failures are not swallowed by the stream.
  qlm::ostringstream small{tiny()};
  EXPECT_THROW(small << longString, AllocationExceedsLimitException);
  qlm::ostringstream smallNumbers{tiny()};
  EXPECT_THROW(
      for (int i = 0; i < 100; ++i) { smallNumbers << 123456789; },
      AllocationExceedsLimitException);
}

// Seeking backwards overwrites characters, like for a `std::ostringstream`.
TEST(StringWithAllocator, ostringstreamSeek) {
  auto check = [](auto&& operations) {
    qlm::ostringstream os{unlimited()};
    std::ostringstream expected;
    operations(os);
    operations(expected);
    EXPECT_EQ(toStd(os.str()), expected.str());
    EXPECT_EQ(os.good(), expected.good());
  };
  check([](std::ostream& os) {
    os << "1, 2, ";
    os.seekp(-2, std::ios_base::cur);
    os << "}";
  });
  check([](std::ostream& os) {
    os << "abcdef";
    os.seekp(1, std::ios_base::beg);
    os << 'X';
    os.seekp(-1, std::ios_base::end);
    os << "YZ";
  });
  check([](std::ostream& os) {
    os << "abc";
    os.seekp(std::streampos{1});
    EXPECT_EQ(os.tellp(), std::streampos{1});
    os << "XYZW";
  });
  // Seeking out of bounds fails.
  check([](std::ostream& os) {
    os << "ab";
    os.seekp(-3, std::ios_base::cur);
  });
  check([](std::ostream& os) {
    os << "ab";
    os.seekp(1, std::ios_base::end);
  });
  // Seeking the input position fails, as the buffer is output-only.
  check([](std::ostream& os) {
    os << "ab";
    EXPECT_EQ(os.rdbuf()->pubseekoff(0, std::ios_base::cur, std::ios_base::in),
              std::streampos(std::streamoff(-1)));
  });
}

// `overflow(eof)` succeeds without writing anything.
TEST(StringWithAllocator, overflowWithEof) {
  struct Buf : qlm::detail::StringStreamBuf {
    using StringStreamBuf::overflow;
    using StringStreamBuf::StringStreamBuf;
  };
  using Traits = std::char_traits<char>;
  Buf buf{unlimited()};
  EXPECT_EQ(buf.overflow('a'), 'a');
  EXPECT_FALSE(Traits::eq_int_type(buf.overflow(Traits::eof()), Traits::eof()));
  EXPECT_EQ(buf.str(), "a");
}
