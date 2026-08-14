// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Marvin Stoetzel <marvin.stoetzel@mailbox.org>

#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "util/SimdUtils.h"

namespace {

using namespace ad_utility::simd;

// Call all implementations that are available on this platform and check that
// they agree with the scalar reference implementation. Returns the common
// result. This is the core of all tests below: the SIMD variants are only
// correct if they are indistinguishable from the reference for every input.
template <char... SpecialChars>
bool checkAllImplementationsAgree(std::string_view sv) {
  const bool expected = detail::containsAnyByteScalar<SpecialChars...>(sv);
#ifdef QLEVER_SIMD_X86
  EXPECT_EQ(detail::containsAnyByteSSE2<SpecialChars...>(sv.data(), sv.size()),
            expected);
  // Exercise both branches of the dispatcher explicitly: the AVX2 path (if
  // the CPU supports it) and the SSE2 fallback. The public `containsAnyByte`
  // only ever takes the AVX2 branch on AVX2-capable machines, so without the
  // explicit `useAvx2 = false` call the fallback would not be covered by the
  // test suite on such machines.
  EXPECT_EQ(detail::containsAnyByteImpl<SpecialChars...>(sv, false), expected);
  if (detail::hasAvx2()) {
    EXPECT_EQ(
        detail::containsAnyByteAVX2<SpecialChars...>(sv.data(), sv.size()),
        expected);
    EXPECT_EQ(detail::containsAnyByteImpl<SpecialChars...>(sv, true), expected);
  }
#endif
  // The dispatching entry point must of course also agree.
  EXPECT_EQ(containsAnyByte<SpecialChars...>(sv), expected);
  return expected;
}

// Sizes 0 to 70 cover all interesting cases for both the 16-byte (SSE2) and the
// 32-byte (AVX2) chunk size: below one chunk, exactly one chunk, several full
// chunks, and every possible tail length (which is handled by an overlapping
// load).
constexpr size_t maxSize = 70;

// _____________________________________________________________________________
TEST(SimdUtils, emptyInput) {
  ASSERT_FALSE((checkAllImplementationsAgree<'"', '\\', '\n', '\r'>("")));
  // Also check that a default-constructed (nullptr) view is not dereferenced.
  ASSERT_FALSE(checkAllImplementationsAgree<'a'>(std::string_view{}));
}

// _____________________________________________________________________________
TEST(SimdUtils, noSpecialCharForAnySize) {
  std::string input;
  for (size_t size = 0; size <= maxSize; ++size) {
    ASSERT_FALSE((checkAllImplementationsAgree<'"', '\\', '\n', '\r'>(input)))
        << "size " << size;
    input.push_back('x');
  }
}

// _____________________________________________________________________________
TEST(SimdUtils, specialCharAtEveryPosition) {
  // For every size and every position within that size, place each of the
  // special characters there and require that it is found. This exercises the
  // chunk loop, the overlapping tail load, and the scalar path alike.
  for (size_t size = 1; size <= maxSize; ++size) {
    for (size_t pos = 0; pos < size; ++pos) {
      for (char specialChar : {'"', '\\', '\n', '\r'}) {
        std::string input(size, 'x');
        input[pos] = specialChar;
        ASSERT_TRUE(
            (checkAllImplementationsAgree<'"', '\\', '\n', '\r'>(input)))
            << "size " << size << ", pos " << pos << ", char "
            << static_cast<int>(specialChar);
      }
    }
  }
}

// _____________________________________________________________________________
TEST(SimdUtils, characterNotInTheSetIsNotFound) {
  // A character that is *close* to the set (e.g. `'` next to `"`) must not
  // trigger a match, and a match must not be reported for a byte with the high
  // bit set (the comparison is on signed chars).
  for (size_t size = 1; size <= maxSize; ++size) {
    for (size_t pos = 0; pos < size; ++pos) {
      for (char otherChar : {'\'', '/', 'n', '\x80', '\xff'}) {
        std::string input(size, 'x');
        input[pos] = otherChar;
        ASSERT_FALSE(
            (checkAllImplementationsAgree<'"', '\\', '\n', '\r'>(input)))
            << "size " << size << ", pos " << pos;
      }
    }
  }
}

// _____________________________________________________________________________
TEST(SimdUtils, highBitBytesInTheSet) {
  // The set itself may contain bytes with the high bit set; `_mm_set1_epi8`
  // and the scalar comparison must treat them identically.
  for (size_t size = 1; size <= maxSize; ++size) {
    for (size_t pos = 0; pos < size; ++pos) {
      std::string input(size, 'x');
      input[pos] = '\xc3';
      ASSERT_TRUE((checkAllImplementationsAgree<'\xc3', '\xa4'>(input)))
          << "size " << size << ", pos " << pos;
      input[pos] = '\xc4';
      ASSERT_FALSE((checkAllImplementationsAgree<'\xc3', '\xa4'>(input)))
          << "size " << size << ", pos " << pos;
    }
  }
}

// _____________________________________________________________________________
TEST(SimdUtils, singleCharacterSet) {
  for (size_t size = 0; size <= maxSize; ++size) {
    std::string input(size, 'x');
    ASSERT_FALSE(checkAllImplementationsAgree<'y'>(input));
    if (size > 0) {
      input.back() = 'y';
      ASSERT_TRUE(checkAllImplementationsAgree<'y'>(input));
    }
  }
}

// _____________________________________________________________________________
TEST(SimdUtils, allCharacterSetsUsedInTheCodebase) {
  // The four character sets that replaced the CTRE character classes.
  for (size_t size = 1; size <= maxSize; ++size) {
    for (size_t pos = 0; pos < size; ++pos) {
      std::string input(size, 'x');
      input[pos] = '\n';
      ASSERT_TRUE((checkAllImplementationsAgree<'\r', '\n', '"', ','>(input)));
      ASSERT_TRUE((checkAllImplementationsAgree<'\n', '\t'>(input)));
      ASSERT_FALSE(
          (checkAllImplementationsAgree<'&', '"', '<', '>', '\''>(input)));
      ASSERT_TRUE((checkAllImplementationsAgree<'"', '\\', '\n', '\r'>(input)));
    }
  }
}

}  // namespace
