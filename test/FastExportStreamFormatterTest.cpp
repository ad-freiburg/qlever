// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

#include "./util/GTestHelpers.h"
#include "backports/span.h"
#include "engine/ConstructTypes.h"
#include "engine/FastExportStreamFormatter.h"
#include "global/Constants.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/Exception.h"

namespace {

using namespace ql::export_formatting;
using namespace qlever::constructExport;

// Collect everything the formatter emits in streaming mode.
struct CollectingFormatter {
  std::string output_;
  FastExportStreamFormatter formatter_;
  CollectingFormatter()
      : formatter_([this](std::string_view chunk) {
          output_.append(chunk.data(), chunk.size());
        }) {}
};

// Turtle output for a plain literal must equal the input bytes: the
// no-special-characters fast path writes the literal through untouched.
TEST(FastExportStreamFormatterTest, TurtlePlainLiteralPassesThrough) {
  CollectingFormatter collector;
  EvaluatedTermData term{"\"Simple Label 5\"@en", nullptr};
  collector.formatter_.writeTerm(term, ExportFormat::Turtle);
  auto summary = std::move(collector.formatter_).finalize();
  EXPECT_EQ(collector.output_, "\"Simple Label 5\"@en");
  EXPECT_EQ(summary.totalTriples_, 0u);
}

// Embedded raw quotes in a normalized literal are escaped once, not twice.
TEST(FastExportStreamFormatterTest, TurtleEmbeddedQuotesEscapedOnce) {
  CollectingFormatter collector;
  EvaluatedTermData term{"\"Title with \"quotes\"\"", nullptr};
  collector.formatter_.writeTerm(term, ExportFormat::Turtle);
  static_cast<void>(std::move(collector.formatter_).finalize());
  EXPECT_EQ(collector.output_, "\"Title with \\\"quotes\\\"\"");
}

// A fully-qualified encoded literal in CSV output is escaped exactly like
// `RdfEscaping::escapeForCsv` applied to the whole term in `formatTriple`.
// A double-typed literal always takes the fully-qualified form (unlike ints,
// which use the short form in CSV, exactly as in `formatTerm`).
TEST(FastExportStreamFormatterTest, CsvFullyQualifiedLiteralMatchesBaseline) {
  CollectingFormatter collector;
  EvaluatedTermData term{"NaN", XSD_DOUBLE_TYPE};
  collector.formatter_.writeTerm(term, ExportFormat::Csv);
  static_cast<void>(std::move(collector.formatter_).finalize());
  const std::string expected = RdfEscaping::escapeForCsv(
      absl::StrCat("\"NaN\"^^<", XSD_DOUBLE_TYPE, ">"));
  EXPECT_EQ(collector.output_, expected);
  EXPECT_EQ(collector.output_,
            "\"\"\"NaN\"\"^^<http://www.w3.org/2001/"
            "XMLSchema#double>\"");
}

// `writeRow` only supports tabular formats: it throws
// `ad_utility::Exception` for Turtle and N-Triples, while CSV and TSV rows
// use the right delimiter and escaping.
TEST(FastExportStreamFormatterTest, WriteRowRejectsNonTabularFormat) {
  CollectingFormatter collector;
  const std::array<std::string_view, 2> cells{"a,b", "c"};
  AD_EXPECT_THROW_WITH_MESSAGE(
      collector.formatter_.writeRow(ExportFormat::Turtle, cells),
      ::testing::HasSubstr("format == ExportFormat::Csv"));
}

TEST(FastExportStreamFormatterTest, WriteRowCsvAndTsv) {
  CollectingFormatter csvCollector;
  const std::array<std::string_view, 2> cells{"a,b", "c"};
  csvCollector.formatter_.writeRow(ExportFormat::Csv, cells);
  static_cast<void>(std::move(csvCollector.formatter_).finalize());
  EXPECT_EQ(csvCollector.output_, "\"a,b\",c\n");

  CollectingFormatter tsvCollector;
  tsvCollector.formatter_.writeRow(ExportFormat::Tsv, cells);
  static_cast<void>(std::move(tsvCollector.formatter_).finalize());
  EXPECT_EQ(tsvCollector.output_, "a,b\tc\n");
}

// TSV escaping matches `RdfEscaping::escapeForTsv` byte for byte: tabs become
// spaces, newlines become `\n`, and a carriage return passes through.
TEST(FastExportStreamFormatterTest, TsvEscapingMatchesBaseline) {
  for (std::string_view field :
       {"plain", "a\tb", "line1\nline2", "cr\rinside", "mixed\t\r\n", ""}) {
    CollectingFormatter collector;
    collector.formatter_.writeEscapedTsv(field);
    static_cast<void>(std::move(collector.formatter_).finalize());
    EXPECT_EQ(collector.output_, RdfEscaping::escapeForTsv(std::string{field}))
        << "field: " << field;
  }
}

// Encoded integers use the short form in Turtle and the fully-qualified form
// in N-Triples, exactly like `ConstructTripleInstantiator::formatTerm`.
TEST(FastExportStreamFormatterTest, EncodedIntegerShortAndQualifiedForm) {
  EvaluatedTermData term{"42", XSD_INT_TYPE};
  CollectingFormatter turtle;
  turtle.formatter_.writeTerm(term, ExportFormat::Turtle);
  static_cast<void>(std::move(turtle.formatter_).finalize());
  EXPECT_EQ(turtle.output_, "42");

  CollectingFormatter ntriples;
  ntriples.formatter_.writeTerm(term, ExportFormat::NTriples);
  static_cast<void>(std::move(ntriples.formatter_).finalize());
  EXPECT_EQ(ntriples.output_, absl::StrCat("\"42\"^^<", XSD_INT_TYPE, ">"));
}

// IRIs are wrapped in angle brackets unless already enclosed or a blank node.
// Blank nodes and the extreme 64-bit integers are written verbatim.
TEST(FastExportStreamFormatterTest, IriBlankNodeAndIntegerExtremes) {
  CollectingFormatter collector;
  auto& f = collector.formatter_;
  f.writeIri("http://a");
  f.writeIri("<http://b>");
  f.writeIri("_:c");
  f.writeBlankNode("_:u", 7, "_x");
  f.writeInteger(std::numeric_limits<int64_t>::min());
  f.writeChar(' ');
  f.writeInteger(std::numeric_limits<uint64_t>::max());
  static_cast<void>(std::move(f).finalize());
  EXPECT_EQ(collector.output_,
            "<http://a><http://b>_:c_:u7_x-9223372036854775808 "
            "18446744073709551615");
}

// Streaming mode flushes full chunks to the sink; the concatenated output and
// the summary counters are independent of the chunk boundaries.
TEST(FastExportStreamFormatterTest, StreamingFlushAcrossChunks) {
  std::string output;
  size_t numChunks = 0;
  FastExportStreamFormatter formatter{
      [&](std::string_view chunk) {
        output.append(chunk);
        ++numChunks;
      },
      FastExportStreamFormatter::SAFETY_WATERMARK * 2};
  const std::string field(1000, 'x');
  std::string expected;
  for (size_t i = 0; i < 50; ++i) {
    formatter.writeRaw(field);
    expected += field;
  }
  const auto summary = std::move(formatter).finalize();
  EXPECT_EQ(output, expected);
  EXPECT_GT(numChunks, 1u);
  EXPECT_EQ(summary.chunksEmitted_, numChunks);
  EXPECT_EQ(summary.totalBytesWritten_, expected.size());
}

// `currentChunk` shows the buffered bytes and is empty after `finalize`.
TEST(FastExportStreamFormatterTest, CurrentChunkEmptyAfterFinalize) {
  std::array<char, 8> buffer{};
  FastExportStreamFormatter formatter{
      ql::span<char>{buffer.data(), buffer.size()}};
  formatter.writeRaw("ab");
  EXPECT_EQ(formatter.currentChunk(), "ab");
  auto& ref = formatter;
  static_cast<void>(std::move(formatter).finalize());
  EXPECT_TRUE(ref.currentChunk().empty());
}

// `ensureAvailable` throws, so the write functions must not be `noexcept`:
// an exception escaping a `noexcept` function calls `std::terminate`.
TEST(FastExportStreamFormatterTest, WriteFunctionsAreNotNoexcept) {
  static_assert(
      !noexcept(std::declval<FastExportStreamFormatter&>().writeChar('x')));
  static_assert(!noexcept(
      std::declval<FastExportStreamFormatter&>().writeRaw(std::string_view{})));
  static_assert(
      !noexcept(std::declval<FastExportStreamFormatter&>().writeInteger(42)));
}

// Fixed-span overflow throws instead of terminating or overwriting memory.
TEST(FastExportStreamFormatterTest, FixedSpanOverflowThrows) {
  std::array<char, 4> buffer{};
  FastExportStreamFormatter formatter{
      ql::span<char>{buffer.data(), buffer.size()}};
  formatter.writeRaw("ab");
  AD_EXPECT_THROW_WITH_MESSAGE(formatter.writeRaw("cdef"),
                               ::testing::HasSubstr("buffer overflow"));
}

}  // namespace
