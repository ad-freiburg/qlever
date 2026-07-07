//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <cstdint>
#include <initializer_list>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "util/GenericCharTraits.h"

namespace {
using ad_utility::GenericCharTraits;

// A minimal user-defined single-byte character type that wraps a `char`, to
// verify that `GenericCharTraits` also works for non-integral types.
struct WrappedChar {
  char c_;
  bool operator==(const WrappedChar& rhs) const { return c_ == rhs.c_; }
};

using ByteTraits = GenericCharTraits<uint8_t>;
using WrappedTraits = GenericCharTraits<WrappedChar>;

// Build an array of `char_type` from a byte sequence for convenient testing.
template <typename Traits>
auto makeArray(std::initializer_list<int> bytes) {
  std::vector<typename Traits::char_type> result;
  for (int b : bytes) {
    result.push_back(Traits::to_char_type(b));
  }
  return result;
}
}  // namespace

// The traits must expose the standard member types.
TEST(GenericCharTraits, MemberTypes) {
  static_assert(std::is_same_v<ByteTraits::char_type, uint8_t>);
  static_assert(std::is_same_v<WrappedTraits::char_type, WrappedChar>);
  static_assert(std::is_same_v<ByteTraits::int_type, int>);
  static_assert(std::is_same_v<ByteTraits::off_type, std::streamoff>);
  static_assert(std::is_same_v<ByteTraits::pos_type, std::streampos>);
  static_assert(std::is_same_v<ByteTraits::state_type, std::mbstate_t>);
}

// `to_char_type`/`to_int_type` must round-trip the whole byte range for both a
// plain integral and a user-defined wrapper character type.
TEST(GenericCharTraits, ByteRoundTrip) {
  for (int i = 0; i <= 255; ++i) {
    EXPECT_EQ(ByteTraits::to_int_type(ByteTraits::to_char_type(i)), i);
    EXPECT_EQ(WrappedTraits::to_int_type(WrappedTraits::to_char_type(i)), i);
  }
}

// _____________________________________________________________________________
TEST(GenericCharTraits, AssignEqLt) {
  WrappedChar c{};
  WrappedTraits::assign(c, WrappedChar{'x'});
  EXPECT_EQ(c.c_, 'x');

  EXPECT_TRUE(WrappedTraits::eq(WrappedChar{'a'}, WrappedChar{'a'}));
  EXPECT_FALSE(WrappedTraits::eq(WrappedChar{'a'}, WrappedChar{'b'}));

  EXPECT_TRUE(WrappedTraits::lt(WrappedChar{'a'}, WrappedChar{'b'}));
  EXPECT_FALSE(WrappedTraits::lt(WrappedChar{'b'}, WrappedChar{'a'}));

  // Comparisons are unsigned, i.e. bytes >= 0x80 compare greater than ASCII,
  // even though the underlying `char` may be signed.
  EXPECT_TRUE(
      WrappedTraits::lt(WrappedChar{'a'}, WrappedTraits::to_char_type(0x80)));
  EXPECT_TRUE(ByteTraits::lt(uint8_t{'a'}, uint8_t{0x80}));
}

// _____________________________________________________________________________
TEST(GenericCharTraits, Compare) {
  auto a = makeArray<ByteTraits>({'a', 'b', 'c'});
  auto b = makeArray<ByteTraits>({'a', 'b', 'c'});
  auto c = makeArray<ByteTraits>({'a', 'b', 'd'});
  auto high = makeArray<ByteTraits>({0x80});
  auto low = makeArray<ByteTraits>({'a'});

  EXPECT_EQ(ByteTraits::compare(a.data(), b.data(), 3), 0);
  EXPECT_LT(ByteTraits::compare(a.data(), c.data(), 3), 0);
  EXPECT_GT(ByteTraits::compare(c.data(), a.data(), 3), 0);
  // A length of zero always compares equal.
  EXPECT_EQ(ByteTraits::compare(a.data(), c.data(), 0), 0);
  // Unsigned comparison: 0x80 > 'a'.
  EXPECT_GT(ByteTraits::compare(high.data(), low.data(), 1), 0);
}

// _____________________________________________________________________________
TEST(GenericCharTraits, Length) {
  auto s = makeArray<ByteTraits>({'a', 'b', 'c', '\0'});
  EXPECT_EQ(ByteTraits::length(s.data()), 3u);

  auto empty = makeArray<ByteTraits>({'\0'});
  EXPECT_EQ(ByteTraits::length(empty.data()), 0u);
}

// _____________________________________________________________________________
TEST(GenericCharTraits, Find) {
  auto s = makeArray<ByteTraits>({'a', 'b', 'c'});
  EXPECT_EQ(ByteTraits::find(s.data(), 3, uint8_t{'b'}), s.data() + 1);
  EXPECT_EQ(ByteTraits::find(s.data(), 3, uint8_t{'z'}), nullptr);
  // A length of zero never finds anything.
  EXPECT_EQ(ByteTraits::find(s.data(), 0, uint8_t{'a'}), nullptr);
}

// _____________________________________________________________________________
TEST(GenericCharTraits, MoveCopy) {
  auto src = makeArray<ByteTraits>({'a', 'b', 'c', 'd'});

  std::vector<uint8_t> dst(4, 0);
  EXPECT_EQ(ByteTraits::copy(dst.data(), src.data(), 4), dst.data());
  EXPECT_EQ(ByteTraits::compare(dst.data(), src.data(), 4), 0);

  // `copy`/`move` with `n == 0` must be a no-op that still returns the
  // destination, even when the pointers are null.
  EXPECT_EQ(ByteTraits::copy(nullptr, nullptr, 0), nullptr);
  EXPECT_EQ(ByteTraits::move(nullptr, nullptr, 0), nullptr);

  // `move` must handle overlapping ranges (shift left by one).
  auto overlap = makeArray<ByteTraits>({'a', 'b', 'c', 'd'});
  EXPECT_EQ(ByteTraits::move(overlap.data(), overlap.data() + 1, 3),
            overlap.data());
  auto expected = makeArray<ByteTraits>({'b', 'c', 'd'});
  EXPECT_EQ(ByteTraits::compare(overlap.data(), expected.data(), 3), 0);
}

// _____________________________________________________________________________
TEST(GenericCharTraits, AssignFill) {
  std::vector<uint8_t> buf(4, 0);
  EXPECT_EQ(ByteTraits::assign(buf.data(), 4, uint8_t{'z'}), buf.data());
  auto expected = makeArray<ByteTraits>({'z', 'z', 'z', 'z'});
  EXPECT_EQ(ByteTraits::compare(buf.data(), expected.data(), 4), 0);
}

// _____________________________________________________________________________
TEST(GenericCharTraits, IntType) {
  // `to_int_type` must yield the unsigned byte value (0..255) and round-trip
  // through `to_char_type`.
  for (int i = 0; i <= 255; ++i) {
    auto c = ByteTraits::to_char_type(i);
    EXPECT_EQ(ByteTraits::to_int_type(c), i);
  }
  // The high byte must be non-negative, i.e. the traits treat it as unsigned.
  EXPECT_EQ(ByteTraits::to_int_type(ByteTraits::to_char_type(0xFF)), 0xFF);

  EXPECT_TRUE(ByteTraits::eq_int_type(42, 42));
  EXPECT_FALSE(ByteTraits::eq_int_type(42, 43));
}

// _____________________________________________________________________________
TEST(GenericCharTraits, Eof) {
  EXPECT_EQ(ByteTraits::eof(), -1);
  // `not_eof(eof())` must map to a value that is distinct from `eof()`.
  EXPECT_EQ(ByteTraits::not_eof(ByteTraits::eof()), 0);
  // For any non-eof value, `not_eof` is the identity.
  EXPECT_EQ(ByteTraits::not_eof(42), 42);
}

// End-to-end: the traits must plug into `std::basic_string` /
// `std::basic_string_view` and produce byte-wise (unsigned) ordering.
TEST(GenericCharTraits, IntegrationWithBasicString) {
  using String = std::basic_string<uint8_t, ByteTraits>;
  using StringView = std::basic_string_view<uint8_t, ByteTraits>;

  String s;
  for (char c : std::string_view{"hello"}) {
    s.push_back(static_cast<uint8_t>(c));
  }
  EXPECT_EQ(s.size(), 5u);

  StringView view{s};
  EXPECT_EQ(view.size(), 5u);
  EXPECT_TRUE(view == StringView{s});

  // Ordering is unsigned byte order: a string starting with 0x80 sorts after
  // one starting with an ASCII letter.
  String ascii;
  ascii.push_back(static_cast<uint8_t>('z'));
  String high;
  high.push_back(static_cast<uint8_t>(0x80));
  EXPECT_LT(StringView{ascii}.compare(StringView{high}), 0);
}
