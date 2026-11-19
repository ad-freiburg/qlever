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

// Test `qlm::StrCat` against `absl::StrCat`, and that it throws when the
// allocator's limit is exceeded.
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

// Test `qlm::StrAppend` against `absl::StrAppend`, and that it throws when
// the allocator's limit is exceeded.
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

// Test both overloads of `qlm::StrJoin` against `absl::StrJoin`, and that
// they throw when the allocator's limit is exceeded.
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

// Test `qlm::ostringstream` against `std::ostringstream`, and that it rethrows
// the exception of the allocator when the limit is exceeded.
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

  // Allocation failures are not swallowed by the stream, but the exception of
  // the allocator is rethrown.
  qlm::ostringstream small{tiny()};
  EXPECT_THROW(small << longString, AllocationExceedsLimitException);
  EXPECT_TRUE(small.bad());
  qlm::ostringstream smallNumbers{tiny()};
  EXPECT_THROW(
      for (int i = 0; i < 100; ++i) { smallNumbers << 123456789; },
      AllocationExceedsLimitException);
  EXPECT_TRUE(smallNumbers.bad());
}

// Test that writing a lot of data (which grows the buffer several times) and
// seeking behave exactly like for a `std::ostringstream`.
TEST(StringWithAllocator, ostringstreamGrowAndSeek) {
  qlm::ostringstream os{unlimited()};
  std::ostringstream expected;
  auto write = [](std::ostream& stream) {
    for (int i = 0; i < 1000; ++i) {
      stream << i << ", ";
    }
    stream.seekp(-2, std::ios_base::cur);
    stream << '!';
    stream.seekp(0, std::ios_base::beg);
    stream << "start";
  };
  write(os);
  write(expected);
  EXPECT_EQ(toStd(os.str()), expected.str());
  EXPECT_EQ(toStd(std::move(os).str()), expected.str());
}

// Test that the previously written content is kept when growing the buffer
// fails.
TEST(StringWithAllocator, ostringstreamKeepsContentOnFailure) {
  auto allocator = qlever::makeAllocatorWithLimit<char>(1_kB);
  qlm::ostringstream os{allocator};
  std::string written;
  EXPECT_THROW(
      for (;;) {
        os << longString;
        written += longString;
      },
      AllocationExceedsLimitException);
  EXPECT_FALSE(written.empty());
  // Raise the limit, s.t. the content can also be copied out of the stream.
  allocator.increaseMemoryLimit(1_kB);
  EXPECT_THAT(toStd(os.str()), ::testing::StartsWith(written));
  EXPECT_THAT(toStd(std::move(os).str()), ::testing::StartsWith(written));
}
