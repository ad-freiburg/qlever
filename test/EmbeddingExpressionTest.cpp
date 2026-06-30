// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Sebastian Walter <swalter@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <gmock/gmock.h>

#include <cmath>
#include <string>
#include <vector>

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/QueryPlanner.h"
#include "index/EncodedIriManager.h"
#include "index/vocabulary/VocabularyType.h"
#include "parser/SparqlParser.h"
#include "util/CancellationHandle.h"
#include "util/GTestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/Timer.h"
#include "util/http/MediaTypes.h"

using namespace ad_utility::testing;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

namespace {
// The `fp32Vector` datatype IRI (without brackets).
constexpr std::string_view kVecType =
    "http://qlever.cs.uni-freiburg.de/embeddings/fp32Vector";

// SPARQL prefix declarations used by all test queries.
constexpr std::string_view kPrefixes =
    "PREFIX emb: <http://qlever.cs.uni-freiburg.de/embeddings/>\n"
    "PREFIX embf: <http://qlever.cs.uni-freiburg.de/embeddings/functions/>\n"
    "PREFIX ex: <http://example.org/>\n";

// Serialize an inline `fp32Vector` literal for an array body like `[1, 0]`.
std::string vec(std::string_view body) {
  return absl::StrCat("\"", body, "\"^^<", kVecType, ">");
}

// A one-type knowledge graph following the new data model: the embedding type
// `ex:T` uses the given `metric`, and three entities `ex:a`/`ex:b`/`ex:c` carry
// the 2-D vectors a=[1,0], b=[0,1], c=[1,1] via an embedding node
// (`emb:hasEmbedding` -> node -> `emb:asFp32Vector` + `emb:type`).
std::string makeKg(std::string_view metricLocalName) {
  auto entity = [](std::string_view name, std::string_view node,
                   std::string_view body) {
    return absl::StrCat("ex:", name, " emb:hasEmbedding ex:", node, " .\n",
                        "ex:", node, " emb:type ex:T ; emb:asFp32Vector ",
                        vec(body), " .\n");
  };
  return absl::StrCat(
      "@prefix emb: <http://qlever.cs.uni-freiburg.de/embeddings/> .\n"
      "@prefix ex: <http://example.org/> .\n"
      "ex:T emb:hasMetric emb:",
      metricLocalName, " ; emb:hasDimension 2 ; emb:hasPrecision emb:fp32 .\n",
      entity("a", "na", "[1, 0]"), entity("b", "nb", "[0, 1]"),
      entity("c", "nc", "[1, 1]"));
}

// The property path from an entity to its stored vector.
constexpr std::string_view kPath = "emb:hasEmbedding/emb:asFp32Vector";

// Run a SELECT `query` against the Turtle `kg` and return the result rows (CSV,
// header dropped). Throws (propagating the query error) if execution fails. If
// `vocabularyType` is given, the index is built with that exact vocabulary type
// (e.g. to force the `EmbeddingVocabulary` sidecar path); otherwise a random
// type is used.
std::vector<std::string> runSelect(
    const std::string& kg, const std::string& query,
    std::optional<ad_utility::VocabularyType> vocabularyType = std::nullopt) {
  TestIndexConfig config{kg};
  config.vocabularyType = vocabularyType;
  auto qec = getQec(std::move(config));
  qec->clearCacheUnpinnedOnly();
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  QueryPlanner qp{qec, handle};
  static EncodedIriManager evM;
  auto pq = SparqlParser::parseQuery(&evM, query, {});
  auto qet = qp.createExecutionTree(pq);
  ad_utility::Timer timer{ad_utility::Timer::Started};
  std::string out;
  for (const auto& block : ExportQueryExecutionTrees::computeResult(
           pq, qet, ad_utility::MediaType::csv, timer, std::move(handle))) {
    out += block;
  }
  std::vector<std::string> rows = absl::StrSplit(out, '\n', absl::SkipEmpty());
  if (!rows.empty()) {
    rows.erase(rows.begin());  // drop the CSV header row
  }
  return rows;
}

// Convenience: the single scalar result of a one-row, one-column SELECT.
double singleDouble(const std::string& kg, const std::string& query) {
  auto rows = runSelect(kg, query);
  EXPECT_EQ(rows.size(), 1u);
  return std::stod(rows.at(0));
}
}  // namespace

// _____________________________________________________________________________
TEST(EmbeddingExpression, KnnOrdering) {
  // Nearest to [1, 0] under cosine: a (same direction) < c (45°) < b (90°).
  auto rows =
      runSelect(makeKg("cosine"),
                absl::StrCat(kPrefixes, "SELECT ?x { ?x ", kPath,
                             " ?e . BIND(embf:distance(?e, ", vec("[1, 0]"),
                             ", ex:T) AS ?d) } ORDER BY ASC(?d)"));
  EXPECT_THAT(rows,
              ElementsAre(HasSubstr("/a"), HasSubstr("/c"), HasSubstr("/b")));
}

// _____________________________________________________________________________
TEST(EmbeddingExpression, KnnOrderingWithEmbeddingSplitSidecar) {
  // Same kNN query as `KnnOrdering`, but force the combined geo+embedding split
  // so the stored vectors come from the `EmbeddingVocabulary` sidecar (the
  // zero-copy `getEmbedding` path) rather than being parsed on demand.
  for (auto vocabType :
       {ad_utility::VocabularyType::Enum::OnDiskCompressedEmbSplit,
        ad_utility::VocabularyType::Enum::OnDiskCompressedGeoEmbSplit}) {
    auto rows =
        runSelect(makeKg("cosine"),
                  absl::StrCat(kPrefixes, "SELECT ?x { ?x ", kPath,
                               " ?e . BIND(embf:distance(?e, ", vec("[1, 0]"),
                               ", ex:T) AS ?d) } ORDER BY ASC(?d)"),
                  ad_utility::VocabularyType{vocabType});
    EXPECT_THAT(rows,
                ElementsAre(HasSubstr("/a"), HasSubstr("/c"), HasSubstr("/b")));
  }
}

// _____________________________________________________________________________
TEST(EmbeddingExpression, ManyVsOneTwoVariables) {
  // The dominant kNN shape: the query vector is bound by a single-subject path
  // (`ex:a … ?q`), so BOTH operands of `embf:distance` are `Variable` columns
  // and `?q` is constant-valued at runtime. This exercises the many-vs-one fast
  // path (decode the shared query once). Query = a = [1,0], so nearest is a (0)
  // < c (45°) < b (90°).
  std::string kg = makeKg("cosine");
  // `?q` constant: distance(?e, ?q) with ?e varying.
  EXPECT_THAT(
      runSelect(kg, absl::StrCat(kPrefixes, "SELECT ?x { ?x ", kPath,
                                 " ?e . ex:a ", kPath, " ?q . ",
                                 "BIND(embf:distance(?e, ?q, ex:T) AS ?d) } "
                                 "ORDER BY ASC(?d)")),
      ElementsAre(HasSubstr("/a"), HasSubstr("/c"), HasSubstr("/b")));
  // Arguments swapped; cosine is symmetric.
  EXPECT_THAT(
      runSelect(kg, absl::StrCat(kPrefixes, "SELECT ?x { ?x ", kPath,
                                 " ?e . ex:a ", kPath, " ?q . ",
                                 "BIND(embf:distance(?q, ?e, ex:T) AS ?d) } "
                                 "ORDER BY ASC(?d)")),
      ElementsAre(HasSubstr("/a"), HasSubstr("/c"), HasSubstr("/b")));
}

// _____________________________________________________________________________
TEST(EmbeddingExpression, ManyVsOneUnsortedCandidates) {
  // Scramble the candidate order with `VALUES` so the candidate column is not
  // in storage order. The result must still be correct: `ORDER BY ASC(?d)`
  // ranks by distance, so a mis-associated `?d` would corrupt the `?x` order.
  // Query = a = [1,0], so the ranking is a (0) < c (45°) < b (90°), independent
  // of the input row order.
  std::string kg = makeKg("cosine");
  EXPECT_THAT(
      runSelect(kg, absl::StrCat(kPrefixes,
                                 "SELECT ?x { VALUES ?x { ex:c ex:b ex:a } ?x ",
                                 kPath, " ?e . ex:a ", kPath, " ?q . ",
                                 "BIND(embf:distance(?e, ?q, ex:T) AS ?d) } "
                                 "ORDER BY ASC(?d)")),
      ElementsAre(HasSubstr("/a"), HasSubstr("/c"), HasSubstr("/b")));
}

// _____________________________________________________________________________
TEST(EmbeddingExpression, ManyVsFewQueries) {
  // VALUES binds three query subjects, crossed with all candidates. Both
  // operands are `Variable` columns with three distinct values, so neither side
  // is constant-valued and this exercises the generic per-row path. The cosine
  // ranking per query must still be correct:
  //   from a=[1,0]: a<c<b ;  from b=[0,1]: b<c<a ;  from c=[1,1]: c<{a,b}
  // (the a/b tie at distance 1-1/√2 is broken by ?other, so a before b).
  std::string kg = makeKg("cosine");
  auto rows =
      runSelect(kg, absl::StrCat(kPrefixes,
                                 "SELECT ?other { VALUES ?qs { ex:a ex:b ex:c }"
                                 " ?qs ",
                                 kPath, " ?q . ?other ", kPath, " ?e . ",
                                 "BIND(embf:distance(?q, ?e, ex:T) AS ?d) } "
                                 "ORDER BY ?qs ASC(?d) ?other"));
  EXPECT_THAT(rows,
              ElementsAre(HasSubstr("/a"), HasSubstr("/c"), HasSubstr("/b"),
                          HasSubstr("/b"), HasSubstr("/c"), HasSubstr("/a"),
                          HasSubstr("/c"), HasSubstr("/a"), HasSubstr("/b")));
}

// _____________________________________________________________________________
TEST(EmbeddingExpression, ManyVsOneBothColumnsConstant) {
  // Two single-subject paths → both operand columns are constant-valued, so a
  // single distance is computed and replicated. cosine([1,0],[0,1]) = 1.
  std::string kg = makeKg("cosine");
  EXPECT_NEAR(
      singleDouble(kg,
                   absl::StrCat(kPrefixes, "SELECT ?d { ex:a ", kPath,
                                " ?qa . ex:b ", kPath, " ?qb . ",
                                "BIND(embf:distance(?qa, ?qb, ex:T) AS ?d) }")),
      1.0, 1e-5);
}

// _____________________________________________________________________________
TEST(EmbeddingExpression, ManyVsOneStrictErrorsTwoVariables) {
  // The fast path must keep the strict-error semantics: if the (constant) query
  // operand is not an embedding vector, it is still a hard query error. Here
  // the query Variable `?q` is bound to a plain string literal.
  std::string kg =
      absl::StrCat(makeKg("cosine"), "ex:q ex:label \"hello\" .\n");
  AD_EXPECT_THROW_WITH_MESSAGE(
      runSelect(kg, absl::StrCat(kPrefixes, "SELECT ?x { ?x ", kPath,
                                 " ?e . ex:q ex:label ?q . ",
                                 "BIND(embf:distance(?e, ?q, ex:T) AS ?d) }")),
      HasSubstr("not an embedding vector"));
}

// _____________________________________________________________________________
TEST(EmbeddingExpression, MetricValuesCosine) {
  // cosine([1,0],[0,1]) = 1 - 0 = 1; cosine([1,0],[1,1]) = 1 - 1/√2 ≈ 0.2929.
  std::string kg = makeKg("cosine");
  EXPECT_NEAR(singleDouble(
                  kg, absl::StrCat(kPrefixes, "SELECT ?d { BIND(embf:distance(",
                                   vec("[1, 0]"), ", ", vec("[0, 1]"),
                                   ", ex:T) AS ?d) }")),
              1.0, 1e-5);
  EXPECT_NEAR(singleDouble(
                  kg, absl::StrCat(kPrefixes, "SELECT ?d { BIND(embf:distance(",
                                   vec("[1, 0]"), ", ", vec("[1, 1]"),
                                   ", ex:T) AS ?d) }")),
              1.0 - 1.0 / std::sqrt(2.0), 1e-5);
}

// _____________________________________________________________________________
TEST(EmbeddingExpression, MetricValuesL2AndSquaredL2) {
  // ‖[1,0]-[0,1]‖² = 2, so l2 = √2, squared-l2 = 2.
  EXPECT_NEAR(singleDouble(makeKg("l2"),
                           absl::StrCat(kPrefixes, "SELECT ?d { BIND(",
                                        "embf:distance(", vec("[1, 0]"), ", ",
                                        vec("[0, 1]"), ", ex:T) AS ?d) }")),
              std::sqrt(2.0), 1e-5);
  EXPECT_NEAR(singleDouble(makeKg("squaredL2"),
                           absl::StrCat(kPrefixes, "SELECT ?d { BIND(",
                                        "embf:distance(", vec("[1, 0]"), ", ",
                                        vec("[0, 1]"), ", ex:T) AS ?d) }")),
              2.0, 1e-5);
}

// _____________________________________________________________________________
TEST(EmbeddingExpression, MetricValuesDotProduct) {
  // dot([1,0],[1,1]) = 1, returned negated: -1.
  EXPECT_NEAR(singleDouble(makeKg("dotProduct"),
                           absl::StrCat(kPrefixes, "SELECT ?d { BIND(",
                                        "embf:distance(", vec("[1, 0]"), ", ",
                                        vec("[1, 1]"), ", ex:T) AS ?d) }")),
              -1.0, 1e-5);
}

// _____________________________________________________________________________
TEST(EmbeddingExpression, StrictErrors) {
  std::string kg = makeKg("cosine");
  // Unknown type.
  AD_EXPECT_THROW_WITH_MESSAGE(
      runSelect(kg, absl::StrCat(kPrefixes, "SELECT ?d { BIND(embf:distance(",
                                 vec("[1, 0]"), ", ", vec("[0, 1]"),
                                 ", ex:nope) AS ?d) }")),
      HasSubstr("not a declared embedding type"));
  // Dimension mismatch (3-D vector against a 2-D type).
  AD_EXPECT_THROW_WITH_MESSAGE(
      runSelect(kg, absl::StrCat(kPrefixes, "SELECT ?d { BIND(embf:distance(",
                                 vec("[1, 0, 0]"), ", ", vec("[0, 1]"),
                                 ", ex:T) AS ?d) }")),
      HasSubstr("dimension"));
  // An argument that is not an embedding vector literal.
  AD_EXPECT_THROW_WITH_MESSAGE(
      runSelect(kg, absl::StrCat(kPrefixes,
                                 "SELECT ?d { BIND(embf:distance(\"hello\", ",
                                 vec("[0, 1]"), ", ex:T) AS ?d) }")),
      HasSubstr("not an embedding vector"));
  // Wrong arity (binary) is a parse-time error: the type must be named.
  AD_EXPECT_THROW_WITH_MESSAGE(
      runSelect(kg,
                absl::StrCat(kPrefixes, "SELECT ?d { BIND(embf:distance(",
                             vec("[1, 0]"), ", ", vec("[0, 1]"), ") AS ?d) }")),
      HasSubstr("embf:distance takes three arguments"));
}
