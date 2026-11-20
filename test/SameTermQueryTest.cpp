// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Jerven Bolleman <jerven.bolleman@sib.swiss>, SIB
//
// SIB = SIB Swiss Institute of Bioinformatics

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// End-to-end tests for the SPARQL function `sameTerm`, see
// https://www.w3.org/TR/sparql11-query/#func-sameTerm. In contrast to the unit
// tests in `SparqlExpressionTest.cpp`, these tests start from RDF terms in a
// Turtle input and in SPARQL queries, and hence also test how these terms are
// stored by QLever (for example, which literals are folded into the same `Id`).

#include <gmock/gmock.h>

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/QueryPlanner.h"
#include "util/GTestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/ParsedQueryTestHelpers.h"

namespace {
using ad_utility::testing::parseQuery;

// The Turtle input for the tests below. The object of each triple is an RDF
// term of a different kind, and the subject describes that term.
const std::string kg = R"ttl(
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix geo: <http://www.opengis.net/ont/geosparql#> .
@prefix : <http://example.org/> .
:integer :p 1 .
:integerWithLeadingZero :p "01"^^xsd:integer .
:int :p "1"^^xsd:int .
:decimal :p 1.0 .
:double :p 1.0E0 .
:doubleNaN :p "NaN"^^xsd:double .
:floatNaN :p "NaN"^^xsd:float .
:doubleZero :p "0.0E0"^^xsd:double .
:doubleNegativeZero :p "-0.0E0"^^xsd:double .
:booleanTrue :p true .
:booleanOne :p "1"^^xsd:boolean .
:string :p "1" .
:xsdString :p "1"^^xsd:string .
:langEn :p "1"@en .
:langEnUppercase :p "1"@EN .
:customDatatype :p "1"^^:datatype .
:iri :p :one .
:encodedIri :p <http://example.org/enc/1> .
:blankNode :p _:b .
:date :p "2025-01-01"^^xsd:date .
:dateTime :p "2025-01-01T00:00:00"^^xsd:dateTime .
:point :p "POINT(1 2)"^^geo:wktLiteral .
:pointWithDecimals :p "POINT(1.0 2.0)"^^geo:wktLiteral .
)ttl";

// Run the `query` on the index for `kg` (with `http://example.org/enc/` as a
// prefix for encoded IRIs) and return the result as TSV.
std::string runQuery(const std::string& query) {
  ad_utility::testing::TestIndexConfig config{kg};
  config.encodedPrefixesWithoutAngleBrackets = {"http://example.org/enc/"};
  auto qec = ad_utility::testing::getQec(std::move(config));
  qec->clearCacheUnpinnedOnly();
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  QueryPlanner qp{qec, cancellationHandle};
  auto pq = parseQuery(query);
  auto qet = qp.createExecutionTree(pq);
  ad_utility::Timer timer(ad_utility::Timer::Started);
  std::string result;
  for (const auto& block : ExportQueryExecutionTrees::computeResult(
           pq, *qet, ad_utility::MediaType::tsv, timer,
           std::move(cancellationHandle))) {
    result += block;
  }
  return result;
}

constexpr std::string_view prefixes =
    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> "
    "PREFIX : <http://example.org/> ";
}  // namespace

// _____________________________________________________________________________
TEST(SameTerm, pairsOfDifferentSubjectsWithTheSameTerm) {
  // All pairs of different subjects whose objects are the same RDF term.
  auto result = runQuery(absl::StrCat(
      prefixes,
      "SELECT ?a ?b WHERE { ?a :p ?x . ?b :p ?y "
      "FILTER (sameTerm(?x, ?y) && STR(?a) < STR(?b)) } ORDER BY ?a ?b"));
  EXPECT_EQ(result,
            "?a\t?b\n"
            // NOTE: The following pairs are NOT the same RDF term according to
            // the specification (their lexical forms or datatypes differ), but
            // QLever folds them into the same `Id` and does not store their
            // lexical form or exact datatype, so they are indistinguishable.
            "<http://example.org/decimal>\t<http://example.org/double>\n"
            "<http://example.org/doubleNaN>\t<http://example.org/floatNaN>\n"
            "<http://example.org/int>\t<http://example.org/integer>\n"
            "<http://example.org/int>\t"
            "<http://example.org/integerWithLeadingZero>\n"
            "<http://example.org/integer>\t"
            "<http://example.org/integerWithLeadingZero>\n"
            "<http://example.org/point>\t"
            "<http://example.org/pointWithDecimals>\n"
            // `"1"` and `"1"^^xsd:string` are the same term in RDF 1.1.
            "<http://example.org/string>\t<http://example.org/xsdString>\n");
}

// _____________________________________________________________________________
TEST(SameTerm, everyTermIsTheSameTermAsItself) {
  // In contrast to `=`, which is false for NaN and an error for terms whose
  // values cannot be compared, `sameTerm(?x, ?x)` is true for every term.
  auto all = runQuery(
      absl::StrCat(prefixes, "SELECT (COUNT(*) AS ?count) WHERE { ?a :p ?x }"));
  auto sameTerm = runQuery(absl::StrCat(
      prefixes,
      "SELECT (COUNT(*) AS ?count) WHERE { ?a :p ?x FILTER sameTerm(?x, ?x) "
      "}"));
  EXPECT_EQ(all, "?count\n23\n");
  EXPECT_EQ(sameTerm, all);

  // The NaNs are not equal to themselves.
  EXPECT_EQ(runQuery(absl::StrCat(prefixes,
                                  "SELECT ?a WHERE { ?a :p ?x "
                                  "FILTER (sameTerm(?x, ?x) && !(?x = ?x)) } "
                                  "ORDER BY ?a")),
            "?a\n<http://example.org/doubleNaN>\n"
            "<http://example.org/floatNaN>\n");
}

// _____________________________________________________________________________
TEST(SameTerm, termsThatAreEqualButNotTheSameTerm) {
  // Pairs of terms that are equal (`=`) but not the same term.
  auto result = runQuery(absl::StrCat(
      prefixes,
      "SELECT ?a ?b WHERE { ?a :p ?x . ?b :p ?y "
      "FILTER (?x = ?y && !sameTerm(?x, ?y) && STR(?a) < STR(?b)) } "
      "ORDER BY ?a ?b"));
  // The integers are equal to the decimal and the double, and the two zeros
  // are equal. Note that `"1"^^xsd:boolean` and `true` are equal, but not the
  // same term (in QLever and according to the specification).
  EXPECT_EQ(
      result,
      "?a\t?b\n"
      "<http://example.org/booleanOne>\t<http://example.org/booleanTrue>\n"
      "<http://example.org/decimal>\t<http://example.org/int>\n"
      "<http://example.org/decimal>\t<http://example.org/integer>\n"
      "<http://example.org/decimal>\t"
      "<http://example.org/integerWithLeadingZero>\n"
      "<http://example.org/double>\t<http://example.org/int>\n"
      "<http://example.org/double>\t<http://example.org/integer>\n"
      "<http://example.org/double>\t"
      "<http://example.org/integerWithLeadingZero>\n"
      "<http://example.org/doubleNegativeZero>\t"
      "<http://example.org/doubleZero>\n");
}

// _____________________________________________________________________________
TEST(SameTerm, constantsAndComputedTerms) {
  // `sameTerm` with constants from the query, with terms that are computed by
  // other expressions, and with UNDEF. Each `?rN` is the result of one call.
  auto result = runQuery(absl::StrCat(
      prefixes,
      "SELECT ?r1 ?r2 ?r3 ?r4 ?r5 ?r6 ?r7 ?r8 ?r9 ?r10 ?r11 ?r12 WHERE { "
      // Numeric constants.
      "BIND (sameTerm(1, 1) AS ?r1) "
      "BIND (sameTerm(1, 1.0) AS ?r2) "
      "BIND (sameTerm(\"NaN\"^^xsd:double, \"NaN\"^^xsd:double) AS ?r3) "
      // A computed literal that is folded into an `Id` like in the index.
      "BIND (sameTerm(STRDT(\"1\", xsd:integer), 1) AS ?r4) "
      // String constants, and strings computed by `STR`.
      "BIND (sameTerm(\"abc\", \"abc\"^^xsd:string) AS ?r5) "
      "BIND (sameTerm(\"abc\", \"abc\"@en) AS ?r6) "
      "BIND (sameTerm(STR(:abc), \"http://example.org/abc\") AS ?r7) "
      // An IRI and a string with the same content.
      "BIND (sameTerm(:abc, \"http://example.org/abc\") AS ?r8) "
      // An IRI that is computed by `IRI` (and hence stored in the local
      // vocab), and the same IRI as a constant (which is an encoded IRI).
      "BIND (sameTerm(IRI(\"http://example.org/enc/1\"), "
      "<http://example.org/enc/1>) AS ?r9) "
      "BIND (sameTerm(IRI(\"http://example.org/enc/1\"), "
      "<http://example.org/enc/2>) AS ?r10) "
      // UNDEF is an error.
      "BIND (sameTerm(?unbound, 1) AS ?r11) "
      "BIND (sameTerm(?unbound, ?unbound) AS ?r12) "
      "}"));
  EXPECT_EQ(result,
            "?r1\t?r2\t?r3\t?r4\t?r5\t?r6\t?r7\t?r8\t?r9\t?r10\t?r11\t?r12\n"
            "true\tfalse\ttrue\ttrue\ttrue\tfalse\ttrue\tfalse\ttrue\tfalse\t"
            "\t\n");
}

// _____________________________________________________________________________
TEST(SameTerm, termsFromTheIndexAndFromTheQuery) {
  // `sameTerm` of the objects of the index and constants from the query, which
  // are folded into `Id`s exactly like the terms of the index.
  auto subjectsWithObject = [](std::string_view object) {
    return runQuery(absl::StrCat(prefixes, "SELECT ?a WHERE { ?a :p ?x ",
                                 "FILTER sameTerm(?x, ", object,
                                 ") } ORDER BY ?a"));
  };
  EXPECT_EQ(subjectsWithObject("\"01\"^^xsd:integer"),
            "?a\n<http://example.org/int>\n<http://example.org/integer>\n"
            "<http://example.org/integerWithLeadingZero>\n");
  EXPECT_EQ(subjectsWithObject("true"),
            "?a\n<http://example.org/booleanTrue>\n");
  EXPECT_EQ(subjectsWithObject("\"1\"^^xsd:boolean"),
            "?a\n<http://example.org/booleanOne>\n");
  EXPECT_EQ(
      subjectsWithObject("\"1\""),
      "?a\n<http://example.org/string>\n<http://example.org/xsdString>\n");
  // NOTE: QLever stores language tags as they are given, and `sameTerm`
  // compares them character by character, so `"1"@EN` is not the same term as
  // `"1"@en` (although language tags are case-insensitive in BCP 47).
  EXPECT_EQ(subjectsWithObject("\"1\"@en"),
            "?a\n<http://example.org/langEn>\n");
  EXPECT_EQ(subjectsWithObject("\"1\"^^:datatype"),
            "?a\n<http://example.org/customDatatype>\n");
  EXPECT_EQ(subjectsWithObject(":one"), "?a\n<http://example.org/iri>\n");
  EXPECT_EQ(subjectsWithObject("<http://example.org/enc/1>"),
            "?a\n<http://example.org/encodedIri>\n");
  EXPECT_EQ(subjectsWithObject("\"-0.0E0\"^^xsd:double"),
            "?a\n<http://example.org/doubleNegativeZero>\n");
  EXPECT_EQ(subjectsWithObject("\"2025-01-01\"^^xsd:date"),
            "?a\n<http://example.org/date>\n");
  // A term that is not contained in the index.
  EXPECT_EQ(subjectsWithObject("\"not contained\""), "?a\n");
  // UNDEF is an error, so the `FILTER` removes all rows.
  EXPECT_EQ(subjectsWithObject("?unbound"), "?a\n");
}
