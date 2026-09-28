// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include "util/SwarDelimiterPacker.h"

namespace {
using namespace ad_utility;

// _____________________________________________________________________________
TEST(SwarDelimiterPackerTest, PackDelimPatternBasic) {
  EXPECT_EQ(packDelimPattern(""), 0ULL);
  EXPECT_EQ(packDelimPattern("\t"), 0x09ULL);
  EXPECT_EQ(packDelimPattern("\n"), 0x0AULL);
  EXPECT_EQ(packDelimPattern(" "), 0x20ULL);
  EXPECT_EQ(packDelimPattern("<"), 0x3CULL);
  EXPECT_EQ(packDelimPattern(">"), 0x3EULL);
  EXPECT_EQ(packDelimPattern("\""), 0x22ULL);
  EXPECT_EQ(packDelimPattern(","), 0x2CULL);

  // 2-byte patterns
  EXPECT_EQ(packDelimPattern("\r\n"), 0x0A0DULL);
  EXPECT_EQ(packDelimPattern("> "), 0x203EULL);
  EXPECT_EQ(packDelimPattern(".\n"), 0x0A2EULL);

  // 3-byte patterns
  EXPECT_EQ(packDelimPattern("> <"), 0x3C203EULL);
  EXPECT_EQ(packDelimPattern("> \""), 0x22203EULL);
  EXPECT_EQ(packDelimPattern(" .\n"), 0x0A2E20ULL);
  EXPECT_EQ(packDelimPattern("\",\""), 0x222C22ULL);

  // 4-byte patterns
  EXPECT_EQ(packDelimPattern("> .\n"), 0x0A2E203EULL);
  EXPECT_EQ(packDelimPattern("\" .\n"), 0x0A2E2022ULL);
  EXPECT_EQ(packDelimPattern(" .\r\n"), 0x0A0D2E20ULL);

  // 8-byte full pattern
  EXPECT_EQ(packDelimPattern("12345678"), 0x3837363534333231ULL);

  // Strings longer than 8 bytes truncate to first 8 bytes
  EXPECT_EQ(packDelimPattern("1234567890"), 0x3837363534333231ULL);
}

// _____________________________________________________________________________
TEST(SwarDelimiterPackerTest, PackDelimPattern32And16) {
  EXPECT_EQ(packDelimPattern32("\t"), 0x09U);
  EXPECT_EQ(packDelimPattern32("\r\n"), 0x0A0DU);
  EXPECT_EQ(packDelimPattern32(" .\n"), 0x0A2E20U);
  EXPECT_EQ(packDelimPattern32("> .\n"), 0x0A2E203EU);

  EXPECT_EQ(packDelimPattern16("\t"), 0x09U);
  EXPECT_EQ(packDelimPattern16("\r\n"), 0x0A0DU);
  EXPECT_EQ(packDelimPattern16("> "), 0x203EU);
}

// _____________________________________________________________________________
TEST(SwarDelimiterPackerTest, PackedDelimiterStruct) {
  PackedDelimiter delim("> <");
  EXPECT_EQ(delim.pattern(), 0x3C203EULL);
  EXPECT_EQ(delim.len(), 3U);
  EXPECT_EQ(delim.size(), 3U);
  EXPECT_FALSE(delim.empty());
  EXPECT_EQ(delim.toString(), "> <");

  PackedDelimiter emptyDelim("");
  EXPECT_EQ(emptyDelim.pattern(), 0ULL);
  EXPECT_EQ(emptyDelim.len(), 0U);
  EXPECT_TRUE(emptyDelim.empty());
  EXPECT_EQ(emptyDelim.toString(), "");

  PackedDelimiter defaultDelim;
  EXPECT_EQ(defaultDelim.pattern(), 0ULL);
  EXPECT_EQ(defaultDelim.len(), 0U);
  EXPECT_TRUE(defaultDelim.empty());

  PackedDelimiter customDelim(0x0A2E203EULL, 4);
  EXPECT_EQ(customDelim.pattern(), 0x0A2E203EULL);
  EXPECT_EQ(customDelim.len(), 4U);
  EXPECT_EQ(customDelim.toString(), "> .\n");

  EXPECT_EQ(delim, PackedDelimiter("> <"));
  EXPECT_NE(delim, customDelim);
}

// _____________________________________________________________________________
TEST(SwarDelimiterPackerTest, WriteDelim64Dynamic) {
  std::array<char, 32> buffer;
  buffer.fill('X');

  // Test writing 3-byte delimiter
  char* next = SwarDelimiterPacker::writeDelim64(
      buffer.data(), SwarDelimiterPacker::TRIPLE_S_TO_P_IRI.pattern(),
      SwarDelimiterPacker::TRIPLE_S_TO_P_IRI.len());
  EXPECT_EQ(next, buffer.data() + 3);
  EXPECT_EQ(buffer[0], '>');
  EXPECT_EQ(buffer[1], ' ');
  EXPECT_EQ(buffer[2], '<');
  EXPECT_EQ(std::string_view(buffer.data(), 3), "> <");

  // Test writing 4-byte delimiter
  buffer.fill('Z');
  next = SwarDelimiterPacker::writeDelim64(
      buffer.data(), SwarDelimiterPacker::TRIPLE_O_IRI_END.pattern(),
      SwarDelimiterPacker::TRIPLE_O_IRI_END.len());
  EXPECT_EQ(next, buffer.data() + 4);
  EXPECT_EQ(std::string_view(buffer.data(), 4), "> .\n");

  // Test writing 1-byte delimiter
  buffer.fill('Y');
  next = SwarDelimiterPacker::writeDelim64(buffer.data(),
                                           SwarDelimiterPacker::TSV_TAB, 1);
  EXPECT_EQ(next, buffer.data() + 1);
  EXPECT_EQ(buffer[0], '\t');

  // Test writing 0-byte delimiter (no pointer advance)
  buffer.fill('W');
  next = SwarDelimiterPacker::writeDelim64(buffer.data(), 0, 0);
  EXPECT_EQ(next, buffer.data());

  // Test writing full 8-byte pattern
  buffer.fill('A');
  uint64_t pat8 = SwarDelimiterPacker::pack("12345678");
  next = SwarDelimiterPacker::writeDelim64(buffer.data(), pat8, 8);
  EXPECT_EQ(next, buffer.data() + 8);
  EXPECT_EQ(std::string_view(buffer.data(), 8), "12345678");
}

// _____________________________________________________________________________
TEST(SwarDelimiterPackerTest, ViolatedPreconditionsThrow) {
  // The store functions check their preconditions with `AD_CONTRACT_CHECK`,
  // so a violation must surface as an exception, not as `std::terminate`.
  std::array<char, 8> buffer{};
  EXPECT_ANY_THROW(
      static_cast<void>(SwarDelimiterPacker::writeDelim64(nullptr, 0, 1)));
  EXPECT_ANY_THROW(static_cast<void>(
      SwarDelimiterPacker::writeDelim64(buffer.data(), 0, 9)));
  EXPECT_ANY_THROW(
      static_cast<void>(SwarDelimiterPacker::writeDelim64<1>(nullptr, 0)));
  EXPECT_ANY_THROW(static_cast<void>(
      SwarDelimiterPacker::writeDelim32(buffer.data(), 0, 5)));
  EXPECT_ANY_THROW(static_cast<void>(
      SwarDelimiterPacker::writeDelim16(buffer.data(), 0, 3)));
  EXPECT_ANY_THROW(static_cast<void>(SwarDelimiterPacker::writeDelim(
      nullptr, SwarDelimiterPacker::TRIPLE_S_TO_P_IRI)));
}

// _____________________________________________________________________________
TEST(SwarDelimiterPackerTest, WriteDelim64Template) {
  std::array<char, 32> buffer;
  buffer.fill('X');

  char* next = SwarDelimiterPacker::writeDelim64<3>(
      buffer.data(), SwarDelimiterPacker::CSV_QUOTE_COMMA_QUOTE);
  EXPECT_EQ(next, buffer.data() + 3);
  EXPECT_EQ(std::string_view(buffer.data(), 3), "\",\"");

  next = SwarDelimiterPacker::writeDelim64<2>(
      next, SwarDelimiterPacker::CSV_QUOTE_NEWLINE);
  EXPECT_EQ(next, buffer.data() + 5);
  EXPECT_EQ(std::string_view(buffer.data(), 5), "\",\"\"\n");
}

// _____________________________________________________________________________
TEST(SwarDelimiterPackerTest, WriteDelimPackedDescriptor) {
  std::array<char, 32> buffer;
  buffer.fill('\0');

  char* p = buffer.data();
  p = SwarDelimiterPacker::writeDelim(p,
                                      SwarDelimiterPacker::TRIPLE_S_TO_P_IRI);
  p = SwarDelimiterPacker::writeDelim(p,
                                      SwarDelimiterPacker::TRIPLE_P_TO_O_LIT);
  p = SwarDelimiterPacker::writeDelim(p, SwarDelimiterPacker::TRIPLE_O_LIT_END);

  EXPECT_EQ(p, buffer.data() + 10);
  EXPECT_EQ(std::string_view(buffer.data(), 10), "> <> \x22\x22 .\n");
}

// _____________________________________________________________________________
TEST(SwarDelimiterPackerTest, WriteDelim32And16) {
  std::array<char, 16> buffer;
  buffer.fill('0');

  char* next32 = SwarDelimiterPacker::writeDelim32(
      buffer.data(), packDelimPattern32("> .\n"), 4);
  EXPECT_EQ(next32, buffer.data() + 4);
  EXPECT_EQ(std::string_view(buffer.data(), 4), "> .\n");

  char* next16 = SwarDelimiterPacker::writeDelim16(
      buffer.data(), packDelimPattern16("\r\n"), 2);
  EXPECT_EQ(next16, buffer.data() + 2);
  EXPECT_EQ(std::string_view(buffer.data(), 2), "\r\n");
}

// _____________________________________________________________________________
TEST(SwarDelimiterPackerTest, AllPredefinedDelimitersMatchStringViews) {
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::TSV_TAB, 1).toString(), "\t");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::TSV_NEWLINE, 1).toString(),
            "\n");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::TSV_CRLF, 2).toString(),
            "\r\n");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::TSV_TAB_TAB, 2).toString(),
            "\t\t");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::TSV_TAB_NEWLINE, 2).toString(),
            "\t\n");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::TSV_TAB_CRLF, 3).toString(),
            "\t\r\n");

  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::CSV_COMMA, 1).toString(), ",");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::CSV_NEWLINE, 1).toString(),
            "\n");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::CSV_CRLF, 2).toString(),
            "\r\n");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::CSV_QUOTE, 1).toString(),
            "\"");
  EXPECT_EQ(
      PackedDelimiter(SwarDelimiterPacker::CSV_QUOTE_COMMA_QUOTE, 3).toString(),
      "\",\"");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::CSV_COMMA_QUOTE, 2).toString(),
            ",\"");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::CSV_QUOTE_COMMA, 2).toString(),
            "\",");
  EXPECT_EQ(
      PackedDelimiter(SwarDelimiterPacker::CSV_QUOTE_NEWLINE, 2).toString(),
      "\"\n");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::CSV_QUOTE_CRLF, 3).toString(),
            "\"\r\n");
  EXPECT_EQ(
      PackedDelimiter(SwarDelimiterPacker::CSV_NEWLINE_QUOTE, 2).toString(),
      "\n\"");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::CSV_CRLF_QUOTE, 3).toString(),
            "\r\n\"");

  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::SPACE, 1).toString(), " ");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::DOT_NEWLINE, 3).toString(),
            " .\n");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::DOT_CRLF, 4).toString(),
            " .\r\n");
  EXPECT_EQ(
      PackedDelimiter(SwarDelimiterPacker::SHORT_DOT_NEWLINE, 2).toString(),
      ".\n");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::SHORT_DOT_CRLF, 3).toString(),
            ".\r\n");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::IRI_OPEN, 1).toString(), "<");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::IRI_CLOSE, 1).toString(), ">");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::IRI_CLOSE_SPACE, 2).toString(),
            "> ");
  EXPECT_EQ(
      PackedDelimiter(SwarDelimiterPacker::IRI_CLOSE_SPACE_OPEN, 3).toString(),
      "> <");
  EXPECT_EQ(
      PackedDelimiter(SwarDelimiterPacker::IRI_CLOSE_SPACE_QUOTE, 3).toString(),
      "> \"");
  EXPECT_EQ(
      PackedDelimiter(SwarDelimiterPacker::IRI_CLOSE_DOT_NEWLINE, 4).toString(),
      "> .\n");
  EXPECT_EQ(
      PackedDelimiter(SwarDelimiterPacker::IRI_CLOSE_DOT_CRLF, 5).toString(),
      "> .\r\n");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::LITERAL_QUOTE, 1).toString(),
            "\"");
  EXPECT_EQ(
      PackedDelimiter(SwarDelimiterPacker::LITERAL_QUOTE_SPACE, 2).toString(),
      "\" ");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::LITERAL_QUOTE_SPACE_OPEN, 3)
                .toString(),
            "\" <");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::LITERAL_QUOTE_DOT_NEWLINE, 4)
                .toString(),
            "\" .\n");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::LITERAL_QUOTE_DOT_CRLF, 5)
                .toString(),
            "\" .\r\n");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::SEMICOLON_SPACE, 2).toString(),
            "; ");
  EXPECT_EQ(PackedDelimiter(SwarDelimiterPacker::COMMA_SPACE, 2).toString(),
            ", ");

  EXPECT_EQ(SwarDelimiterPacker::TRIPLE_S_TO_P_IRI.toString(), "> <");
  EXPECT_EQ(SwarDelimiterPacker::TRIPLE_P_TO_O_IRI.toString(), "> <");
  EXPECT_EQ(SwarDelimiterPacker::TRIPLE_P_TO_O_LIT.toString(), "> \"");
  EXPECT_EQ(SwarDelimiterPacker::TRIPLE_O_IRI_END.toString(), "> .\n");
  EXPECT_EQ(SwarDelimiterPacker::TRIPLE_O_LIT_END.toString(), "\" .\n");
  EXPECT_EQ(SwarDelimiterPacker::TRIPLE_O_RAW_END.toString(), " .\n");
}

// _____________________________________________________________________________
TEST(SwarDelimiterPackerTest, EndToEndRowSerializationComparison) {
  // Test N-Triples row serialization: <s> <p> <o> .\n
  std::string_view sub = "http://example.org/entity/Q42";
  std::string_view pred = "http://example.org/prop/P31";
  std::string_view obj = "http://example.org/entity/Q5";

  std::vector<char> scalarBuf(256, 0);
  std::vector<char> swarBuf(256, 0);

  // Scalar byte-by-byte writer
  char* sPtr = scalarBuf.data();
  *sPtr++ = '<';
  std::memcpy(sPtr, sub.data(), sub.size());
  sPtr += sub.size();
  *sPtr++ = '>';
  *sPtr++ = ' ';
  *sPtr++ = '<';
  std::memcpy(sPtr, pred.data(), pred.size());
  sPtr += pred.size();
  *sPtr++ = '>';
  *sPtr++ = ' ';
  *sPtr++ = '<';
  std::memcpy(sPtr, obj.data(), obj.size());
  sPtr += obj.size();
  *sPtr++ = '>';
  *sPtr++ = ' ';
  *sPtr++ = '.';
  *sPtr++ = '\n';

  // SWAR writer
  char* wPtr = swarBuf.data();
  *wPtr++ = '<';
  std::memcpy(wPtr, sub.data(), sub.size());
  wPtr += sub.size();
  wPtr = SwarDelimiterPacker::writeDelim64<3>(
      wPtr, SwarDelimiterPacker::TRIPLE_S_TO_P_IRI.pattern());
  std::memcpy(wPtr, pred.data(), pred.size());
  wPtr += pred.size();
  wPtr = SwarDelimiterPacker::writeDelim64<3>(
      wPtr, SwarDelimiterPacker::TRIPLE_P_TO_O_IRI.pattern());
  std::memcpy(wPtr, obj.data(), obj.size());
  wPtr += obj.size();
  wPtr = SwarDelimiterPacker::writeDelim64<4>(
      wPtr, SwarDelimiterPacker::TRIPLE_O_IRI_END.pattern());

  size_t scalarLen = sPtr - scalarBuf.data();
  size_t swarLen = wPtr - swarBuf.data();

  EXPECT_EQ(scalarLen, swarLen);
  EXPECT_EQ(std::string_view(scalarBuf.data(), scalarLen),
            std::string_view(swarBuf.data(), swarLen));
  EXPECT_EQ(std::string_view(swarBuf.data(), swarLen),
            "<http://example.org/entity/Q42> <http://example.org/prop/P31> "
            "<http://example.org/entity/Q5> .\n");

  // Test CSV quoted row: "col1","col2","col3"\n
  std::string_view c1 = "Alice";
  std::string_view c2 = "30";
  std::string_view c3 = "Freiburg";

  scalarBuf.assign(256, 0);
  swarBuf.assign(256, 0);

  // Scalar CSV
  sPtr = scalarBuf.data();
  *sPtr++ = '"';
  std::memcpy(sPtr, c1.data(), c1.size());
  sPtr += c1.size();
  *sPtr++ = '"';
  *sPtr++ = ',';
  *sPtr++ = '"';
  std::memcpy(sPtr, c2.data(), c2.size());
  sPtr += c2.size();
  *sPtr++ = '"';
  *sPtr++ = ',';
  *sPtr++ = '"';
  std::memcpy(sPtr, c3.data(), c3.size());
  sPtr += c3.size();
  *sPtr++ = '"';
  *sPtr++ = '\n';

  // SWAR CSV
  wPtr = swarBuf.data();
  *wPtr++ = '"';
  std::memcpy(wPtr, c1.data(), c1.size());
  wPtr += c1.size();
  wPtr = SwarDelimiterPacker::writeDelim64<3>(
      wPtr, SwarDelimiterPacker::CSV_QUOTE_COMMA_QUOTE);
  std::memcpy(wPtr, c2.data(), c2.size());
  wPtr += c2.size();
  wPtr = SwarDelimiterPacker::writeDelim64<3>(
      wPtr, SwarDelimiterPacker::CSV_QUOTE_COMMA_QUOTE);
  std::memcpy(wPtr, c3.data(), c3.size());
  wPtr += c3.size();
  wPtr = SwarDelimiterPacker::writeDelim64<2>(
      wPtr, SwarDelimiterPacker::CSV_QUOTE_NEWLINE);

  scalarLen = sPtr - scalarBuf.data();
  swarLen = wPtr - swarBuf.data();

  EXPECT_EQ(scalarLen, swarLen);
  EXPECT_EQ(std::string_view(scalarBuf.data(), scalarLen),
            std::string_view(swarBuf.data(), swarLen));
  EXPECT_EQ(std::string_view(swarBuf.data(), swarLen),
            "\"Alice\",\"30\",\"Freiburg\"\n");
}

}  // namespace
