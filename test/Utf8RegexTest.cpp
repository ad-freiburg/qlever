// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(johannes.kalmbach@gmail.com)

// Regexes in CTRE that match Turtle Prefix declarations
// CTRE currently doesn't support UTF-8 properly so these have to be converted
// to Explicit ascii bytes pattern.
// These are currently not used in production because of compile times being to
// long but might be useful in the future

#include <ctre/ctre.h>
#include <gtest/gtest.h>
#include <codecvt>
#include <locale>
#include <string>
#include "../src/parser/Tokenizer.h"

using namespace std::literals::string_literals;

std::string codePointToUtf8(char32_t codepoint) {
  std::wstring_convert<std::codecvt_utf8<char32_t>, char32_t> convert;
  auto res = convert.to_bytes(&codepoint, (&codepoint) + 1);
  return res;
}
/*

TEST(UTF8Test, Foo) { ASSERT_EQ(u8"Ä", codePointToUtf8(0xc4)); }

constexpr ctll::fixed_string r1(R"(\xc3([\x80-\x96]|[\x98-\xb6]|[\xB8-\xBF]))");
constexpr ctll::fixed_string r2(R"([\xc4-\xcb][\x80-\xBF])");
constexpr ctll::fixed_string r3(
    R"((\xcd[\xb0-\xbd\xbf])|([\xce-\xdf][\x80-\xbf]))");
constexpr ctll::fixed_string r4(R"(([\xe0\xe1][\x80-\xbf][\x80-\xbf]))");
constexpr ctll::fixed_string r5(
    R"((\xe2((\x80[\x8c-\x8d])|(\x81[\xb0-\xbf])|([\x82-\x85][\x80-\xbf])|(\x86[\x80-\x8f])|([\xb0-\xbe][\x80-\xbf])|(\xbf[\x80-\xaf]))))");
constexpr ctll::fixed_string r6(
    R"(\xe3((\x80[\x81-\xbf])|([\x81-\xbf][\x80-\xbf])))");
constexpr ctll::fixed_string r7(R"([\xe4-\xec][\x80-\xbf][\x80-\xbf])");
constexpr ctll::fixed_string r8(R"(\xed[\x80-\x9f][\x80-\xbf])");
constexpr ctll::fixed_string r9(
    R"(\xef([\xa4-\xb6\xb8-\xbe][\x80-\xbf]|\xb7[\x80-\x8f\xb0-\xbf]|\xbf[\x80-\xbd]))");
constexpr ctll::fixed_string r10(
    R"([\xf0-\xf2]...)");  // a little relaxed, invalid utf-8 also recognized
constexpr ctll::fixed_string r11(
    R"(\xf3[\x80-\xaf][\x80-\xbf][\x80-\xbf])");  // a little relaxed, invalid
                                                  // utf-8 also recognized
constexpr auto r = grp(r1) + "|" + grp(r2) + "|" + grp(r3) + "|" + grp(r4) +
                   "|" + grp(r5) + "|" + grp(r6) + "|" + grp(r7) + "|" +
                   grp(r8) + "|" + grp(r9) + "|" + grp(r10) + "|" + grp(r11);
TEST(UTF8Test, FirstReges) {
  for (char32_t i = 0x0; i < 0xc0; ++i) {
    ASSERT_FALSE(ctre::match<r>(codePointToUtf8(i)));
  }
  for (char32_t i = 0xc0; i <= 0xD6; ++i) {
    ASSERT_TRUE(ctre::match<r>(codePointToUtf8(i)));
  }
  ASSERT_FALSE(ctre::match<r>(codePointToUtf8(0xd7)));
  for (char32_t i = 0xd8; i <= 0xf6; ++i) {
    ASSERT_TRUE(ctre::match<r>(codePointToUtf8(i)));
  }
  ASSERT_FALSE(ctre::match<r>(codePointToUtf8(0xf7)));
  for (char32_t i = 0xf8; i <= 0x2ff; ++i) {
    ASSERT_TRUE(ctre::match<r>(codePointToUtf8(i)));
  }
  for (char32_t i = 0x300; i < 0x370; ++i) {
    ASSERT_FALSE(ctre::match<r>(codePointToUtf8(i)));
  }
  for (char32_t i = 0x370; i <= 0x37d; ++i) {
    ASSERT_TRUE(ctre::match<r3>(codePointToUtf8(i)));
  }

  ASSERT_FALSE(ctre::match<r>(codePointToUtf8(0x37e)));

  for (char32_t i = 0x37f; i <= 0x1fff; ++i) {
    ASSERT_TRUE(ctre::match<r>(codePointToUtf8(i)));
  }

  for (char32_t i = 0x2000; i <= 0x200b; ++i) {
    // std::cout << std::hex << i << std::endl;
    ASSERT_FALSE(ctre::match<r>(codePointToUtf8(i)));
  }

  for (char32_t i = 0x37f; i <= 0x1fff; ++i) {
    ASSERT_TRUE(ctre::match<r>(codePointToUtf8(i)));
  }

  for (char32_t i = 0x2000; i <= 0x200b; ++i) {
    ASSERT_FALSE(ctre::match<r>(codePointToUtf8(i)));
  }

  for (char32_t i = 0x200C; i <= 0x200d; ++i) {
    ASSERT_TRUE(ctre::match<r>(codePointToUtf8(i)));
  }

  for (char32_t i = 0x200e; i < 0x2070; ++i) {
    ASSERT_FALSE(ctre::match<r>(codePointToUtf8(i)));
  }

  for (char32_t i = 0x2070; i <= 0x218f; ++i) {
    // std::cout << std::hex << i << std::endl;
    ASSERT_TRUE(ctre::match<r>(codePointToUtf8(i)));
  }

  for (char32_t i = 0x2190; i < 0x2c00; ++i) {
    ASSERT_FALSE(ctre::match<r>(codePointToUtf8(i)));
  }

  for (char32_t i = 0x2c00; i <= 0x2fef; ++i) {
    // std::cout << std::hex << i << std::endl;
    ASSERT_TRUE(ctre::match<r>(codePointToUtf8(i)));
  }

  for (char32_t i = 0x2ff0; i < 0x3001; ++i) {
    ASSERT_FALSE(ctre::match<r>(codePointToUtf8(i)));
  }

  for (char32_t i = 0x3001; i <= 0xd7ff; ++i) {
    // std::cout << std::hex << i << std::endl;
    ASSERT_TRUE(ctre::match<r>(codePointToUtf8(i)));
  }

  for (char32_t i = 0xd7ff + 1; i < 0xF900; ++i) {
    ASSERT_FALSE(ctre::match<r>(codePointToUtf8(i)));
  }

  for (char32_t i = 0xF900; i <= 0xFDCF; ++i) {
    // std::cout << std::hex << i << std::endl;
    ASSERT_TRUE(ctre::match<r>(codePointToUtf8(i)));
  }

  for (char32_t i = 0xFDCF + 1; i < 0xFDF0; ++i) {
    ASSERT_FALSE(ctre::match<r>(codePointToUtf8(i)));
  }

  for (char32_t i = 0xFDF0; i <= 0xfffd; ++i) {
    // std::cout << std::hex << i << std::endl;
    ASSERT_TRUE(ctre::match<r>(codePointToUtf8(i)));
  }

  for (char32_t i = 0xFFFD + 1; i < 0x10000; ++i) {
    ASSERT_FALSE(ctre::match<r>(codePointToUtf8(i)));
  }

  for (char32_t i = 0x10000; i <= 0xEFFFF; ++i) {
    if (!(ctre::match<r>(codePointToUtf8(i)))) {
      std::cout << std::hex << i << std::endl;
      auto x = codePointToUtf8(i);
      for (auto& c : x) {
        std::cout << std::hex << static_cast<u_char>(c);
      }
      std::cout << std::endl;
    }
    ASSERT_TRUE(ctre::match<r>(codePointToUtf8(i)));
  }

  for (char32_t i = 0xEFFFF + 1; i <= 0x10FFFF; ++i) {
    ASSERT_FALSE(ctre::match<r>(codePointToUtf8(i)));
  }
}
 */
