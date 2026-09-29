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
#include <vector>

#include "./util/GTestHelpers.h"
#include "backports/span.h"
#include "engine/ConstructTripleInstantiator.h"
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

// Literals and TSV fields with special characters at the start, in the
// middle, at the end and back to back are escaped exactly like the legacy
// `RdfEscaping` functions, in streaming mode and in a fixed span of exactly
// the escaped size (the escaping reserves the exact size, not an upper bound).
TEST(FastExportStreamFormatterTest, EscapingMatchesLegacyAtAllPositions) {
  const std::string longRun(300, 'x');
  const std::array<std::string, 7> contents{
      "\\starts with a backslash",
      "ends with a quote\"",
      "a\nb\rc\"d\\e",
      "\"\"\\\\\n\n\r\r",
      "\"",
      absl::StrCat(longRun, "\"", longRun, "\n", longRun),
      "no special characters"};
  for (const auto& content : contents) {
    for (std::string_view suffix : {"", "@en", "^^<http://example.org/dt>"}) {
      const std::string literal = absl::StrCat("\"", content, "\"", suffix);
      const std::string expected =
          RdfEscaping::validRDFLiteralFromNormalized(literal);
      EvaluatedTermData term{literal, nullptr};
      CollectingFormatter collector;
      collector.formatter_.writeTerm(term, ExportFormat::Turtle);
      static_cast<void>(std::move(collector.formatter_).finalize());
      EXPECT_EQ(collector.output_, expected);
      std::string exact(expected.size(), '\0');
      FastExportStreamFormatter fixed{
          ql::span<char>(exact.data(), exact.size())};
      fixed.writeTerm(term, ExportFormat::Turtle);
      EXPECT_EQ(fixed.currentChunk(), expected);
    }
    const std::string tsvField = absl::StrCat("\t", content, "\t\n");
    const std::string expectedTsv = RdfEscaping::escapeForTsv(tsvField);
    CollectingFormatter collector;
    collector.formatter_.writeEscapedTsv(tsvField);
    static_cast<void>(std::move(collector.formatter_).finalize());
    EXPECT_EQ(collector.output_, expectedTsv);
    std::string exact(expectedTsv.size(), '\0');
    FastExportStreamFormatter fixed{ql::span<char>(exact.data(), exact.size())};
    fixed.writeEscapedTsv(tsvField);
    EXPECT_EQ(fixed.currentChunk(), expectedTsv);
  }
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

// _____________________________________________________________________________
// A wide mix of terms: IRIs, blank nodes, plain / language-tagged / typed
// vocabulary literals with every special character of every format at
// several positions (inside and beyond the first 8-byte block of the scan),
// empty literals, and encoded literals in short and fully-qualified form.
std::vector<EvaluatedTerm> termsForAllFormats() {
  std::vector<EvaluatedTerm> terms;
  auto add = [&terms](std::string s, const char* type = nullptr) {
    terms.push_back(std::make_shared<const EvaluatedTermData>(
        EvaluatedTermData{std::move(s), type}));
  };
  add("<http://example.org/s>");
  add("<http://example.org/with,comma\"quote>");
  add("_:b0");
  add("\"\"");
  add("\"plain\"");
  add("\"text\"@en");
  add("\"typed\"^^<http://www.w3.org/2001/XMLSchema#string>");
  for (std::string special : {"\\", "\"", "\n", "\r", "\t", ","}) {
    for (size_t pos : {0u, 3u, 7u, 8u, 12u, 15u, 16u, 21u}) {
      std::string content(24, 'a');
      content.insert(pos, special);
      add(absl::StrCat("\"", content, "\""));
      add(absl::StrCat("\"", content, "\"@de"));
    }
  }
  add("\"" + std::string(5000, '\n') + "\"");
  add("42", XSD_INT_TYPE);
  add("-3.25", XSD_DECIMAL_TYPE);
  add("true", XSD_BOOLEAN_TYPE);
  add("1", XSD_BOOLEAN_TYPE);
  add("NaN", XSD_DOUBLE_TYPE);
  add("2024-01-02", XSD_DATE_TYPE);
  return terms;
}

// `writeTriple` produces exactly the bytes of the legacy `formatTriple` for
// every term in every position and every format (Turtle, N-Triples, CSV,
// TSV), in streaming mode with a small chunk (forcing flushes and a chunk
// that grows for the largest term) and in a fixed span.
TEST(FastExportStreamFormatterTest, WriteTripleMatchesLegacyForAllFormats) {
  using ad_utility::MediaType;
  const auto terms = termsForAllFormats();
  const auto& iri = terms.front();
  for (MediaType mediaType : {MediaType::turtle, MediaType::ntriples,
                              MediaType::csv, MediaType::tsv}) {
    const ExportFormat format = toExportFormat(mediaType);
    std::vector<EvaluatedTriple> triples;
    // The legacy Turtle / N-Triples path escapes literals only in the object
    // position (the only position where a literal can occur); CSV and TSV
    // escape every position.
    const bool tabular =
        format == ExportFormat::Csv || format == ExportFormat::Tsv;
    for (const auto& term : terms) {
      if (tabular) {
        triples.push_back(EvaluatedTriple{term, iri, iri});
        triples.push_back(EvaluatedTriple{iri, term, iri});
      }
      triples.push_back(EvaluatedTriple{iri, iri, term});
    }
    std::string legacy;
    for (const auto& triple : triples) {
      absl::StrAppend(&legacy, formatTriple(triple, mediaType));
    }

    std::string streamed;
    FastExportStreamFormatter formatter{
        [&streamed](std::string_view chunk) { streamed.append(chunk); },
        FastExportStreamFormatter::SAFETY_WATERMARK * 2};
    for (const auto& triple : triples) {
      formatter.writeTriple(format, triple);
    }
    EXPECT_EQ(formatter.totalTriples(), triples.size());
    EXPECT_EQ(formatter.totalBytesWritten(), legacy.size());
    const auto summary = std::move(formatter).finalize();
    EXPECT_EQ(streamed, legacy) << ad_utility::toString(mediaType);
    EXPECT_EQ(summary.totalTriples_, triples.size());
    EXPECT_EQ(summary.totalBytesWritten_, legacy.size());

    std::string fixed(legacy.size(), '\0');
    FastExportStreamFormatter fixedFormatter{
        ql::span<char>{fixed.data(), fixed.size()}};
    for (const auto& triple : triples) {
      fixedFormatter.writeTriple(format, triple);
    }
    EXPECT_EQ(fixedFormatter.currentChunk(), legacy);
    EXPECT_EQ(fixedFormatter.bytesBuffered(), legacy.size());
    // A flush in fixed-span mode has no sink: the bytes only count as
    // written.
    fixedFormatter.flush();
    EXPECT_EQ(fixedFormatter.bytesBuffered(), 0u);
    EXPECT_EQ(fixedFormatter.totalBytesWritten(), legacy.size());
    const auto fixedSummary = std::move(fixedFormatter).finalize();
    EXPECT_EQ(fixedSummary.chunksEmitted_, 0u);
  }
}

// `writeLiteral` writes the content verbatim, then a language tag (with or
// without its `@`) or a datatype, the language tag taking precedence.
TEST(FastExportStreamFormatterTest, WriteLiteralSuffixes) {
  CollectingFormatter collector;
  auto& f = collector.formatter_;
  f.writeLiteral("a");
  f.writeLiteral("b", "http://dt");
  f.writeLiteral("c", "", "en");
  f.writeLiteral("d", "", "@de");
  f.writeLiteral("e", "http://dt", "fr");
  f.writeLiteral("");
  static_cast<void>(std::move(f).finalize());
  EXPECT_EQ(collector.output_,
            "\"a\"\"b\"^^<http://dt>\"c\"@en\"d\"@de\"e\"@fr\"\"");
}

// CSV fields: only fields with `,`, `"`, `\r` or `\n` are quoted, embedded
// quotes are doubled (also several and at both ends), like
// `RdfEscaping::escapeForCsv`.
TEST(FastExportStreamFormatterTest, CsvEscapingMatchesLegacy) {
  for (std::string_view field :
       {"", "plain", "a,b", "\"", "\"\"", "\"start", "end\"", "mid\"dle",
        "two\"quo\"tes", "cr\rhere", "nl\nhere", "long field without specials",
        "long field with a comma, beyond eight bytes"}) {
    CollectingFormatter collector;
    collector.formatter_.writeEscapedCsv(field);
    static_cast<void>(std::move(collector.formatter_).finalize());
    EXPECT_EQ(collector.output_, RdfEscaping::escapeForCsv(std::string{field}))
        << "field: " << field;
  }
}

// A single field larger than the whole streaming chunk grows the chunk, and
// the following writes continue in the grown chunk.
TEST(FastExportStreamFormatterTest, StreamingChunkGrowsForLargeField) {
  CollectingFormatter collector;
  auto& f = collector.formatter_;
  const std::string large(3 * FastExportStreamFormatter::DEFAULT_CHUNK_SIZE,
                          'y');
  f.writeRaw("head");
  f.writeRaw(large);
  f.writeRaw("");
  f.writeChar('!');
  EXPECT_EQ(f.totalBytesWritten(), 4 + large.size() + 1);
  const auto summary = std::move(f).finalize();
  EXPECT_EQ(collector.output_, absl::StrCat("head", large, "!"));
  EXPECT_EQ(summary.chunksEmitted_, 2u);
}

// A flush without buffered bytes emits no chunk.
TEST(FastExportStreamFormatterTest, FlushWithoutDataEmitsNothing) {
  size_t numChunks = 0;
  FastExportStreamFormatter f{[&numChunks](std::string_view) { ++numChunks; }};
  f.flush();
  f.writeRaw("x");
  f.flush();
  f.flush();
  EXPECT_EQ(numChunks, 1u);
  EXPECT_EQ(std::move(f).finalize().chunksEmitted_, 1u);
}

// Every supported media type maps to its format; others are rejected.
TEST(FastExportStreamFormatterTest, ToExportFormat) {
  using ad_utility::MediaType;
  EXPECT_EQ(toExportFormat(MediaType::turtle), ExportFormat::Turtle);
  EXPECT_EQ(toExportFormat(MediaType::ntriples), ExportFormat::NTriples);
  EXPECT_EQ(toExportFormat(MediaType::csv), ExportFormat::Csv);
  EXPECT_EQ(toExportFormat(MediaType::tsv), ExportFormat::Tsv);
  AD_EXPECT_THROW_WITH_MESSAGE(toExportFormat(MediaType::sparqlJson),
                               ::testing::HasSubstr("Unsupported media type"));
}

// The special-character tables, evaluated at runtime, mark exactly the
// characters that the respective escaping changes.
TEST(FastExportStreamFormatterTest, SpecialCharacterTables) {
  auto marked = [](const std::array<bool, 256>& table) {
    std::string result;
    for (size_t c = 0; c < table.size(); ++c) {
      if (table[c]) {
        result.push_back(static_cast<char>(c));
      }
    }
    return result;
  };
  EXPECT_EQ(marked(detail::makeCsvSpecialTable()), "\n\r\",");
  EXPECT_EQ(marked(detail::makeTsvSpecialTable()), "\t\n");
  EXPECT_EQ(marked(detail::makeTurtleSpecialTable()), "\n\r\"\\");
  for (size_t length = 0; length < 20; ++length) {
    for (size_t pos = 0; pos <= length; ++pos) {
      std::string s(length, 'a');
      EXPECT_FALSE(detail::hasSpecialCharacters<detail::csvSpecialTable>(s));
      if (pos < length) {
        s[pos] = ',';
        EXPECT_TRUE(detail::hasSpecialCharacters<detail::csvSpecialTable>(s))
            << length << " " << pos;
      }
    }
  }
}

// Contract checks: a streaming formatter needs a sink, an empty fixed span is
// allowed, a Turtle literal needs its quotes, a triple needs
// all three terms, and rows are only CSV or TSV.
TEST(FastExportStreamFormatterTest, ContractChecks) {
  EXPECT_ANY_THROW(
      FastExportStreamFormatter{FastExportStreamFormatter::ChunkSink{}});
  FastExportStreamFormatter empty{ql::span<char>{}};
  EXPECT_TRUE(empty.currentChunk().empty());

  CollectingFormatter collector;
  auto& f = collector.formatter_;
  EXPECT_ANY_THROW(f.writeEscapedTurtleLiteral("no quote"));
  EXPECT_ANY_THROW(f.writeEscapedTurtleLiteral("\"unterminated"));
  auto term = std::make_shared<const EvaluatedTermData>(
      EvaluatedTermData{"<http://s>", nullptr});
  EXPECT_ANY_THROW(f.writeTriple(ExportFormat::Turtle,
                                 EvaluatedTriple{nullptr, term, term}));
  EXPECT_ANY_THROW(f.writeTriple(ExportFormat::Turtle,
                                 EvaluatedTriple{term, nullptr, term}));
  EXPECT_ANY_THROW(f.writeTriple(ExportFormat::Turtle,
                                 EvaluatedTriple{term, term, nullptr}));
  std::array<std::string_view, 1> cells{"x"};
  EXPECT_ANY_THROW(f.writeRow(ExportFormat::NTriples, cells));
}

}  // namespace
