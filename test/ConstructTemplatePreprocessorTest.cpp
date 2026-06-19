// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "./util/AllocatorTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "engine/ConstructTemplatePreprocessor.h"
#include "index/Index.h"
#include "parser/data/Types.h"

namespace qlever::constructExport {

// `PrintTo` overloads so gmock shows human-readable output instead of raw
// bytes.
void PrintTo(const PrecomputedConstant& c, std::ostream* os) {
  *os << "PrecomputedConstant{\"" << c.evaluatedTerm_ << "\"}";
}
void PrintTo(const PrecomputedVariable& v, std::ostream* os) {
  *os << "PrecomputedVariable{" << v.columnIndex_ << "}";
}
void PrintTo(const PrecomputedBlankNode& b, std::ostream* os) {
  *os << "PrecomputedBlankNode{\"" << b.prefix_ << "\", \"" << b.suffix_
      << "\"}";
}
}  // namespace qlever::constructExport

namespace {

using namespace std::string_literals;
using ::testing::Optional;
using enum PositionInTriple;
using namespace qlever::constructExport;
using Triples = ad_utility::sparql_types::Triples;
using ::testing::ElementsAre;
using ::testing::UnorderedElementsAre;

// Composable matchers for `PreprocessedTerm` variants.
// see https://github.com/google/googletest/blob/main/docs/reference/matchers.md
static constexpr auto matchesPrecomputedConstant = [](const auto& value) {
  // only match the string, not the type field.
  return ::testing::VariantWith<PrecomputedConstant>(
      AD_FIELD(PrecomputedConstant, evaluatedTerm_,
               ::testing::Pointee(AD_FIELD(EvaluatedTermData, rdfTermString_,
                                           std::string(value)))));
};

static constexpr auto matchesPrecomputedVariable = [](const auto& columnIdx) {
  return ::testing::VariantWith<PrecomputedVariable>(
      AD_FIELD(PrecomputedVariable, columnIndex_, columnIdx));
};

static constexpr auto matchesPrecomputedBlankNode = [](const auto& prefix,
                                                       const auto& suffix) {
  return ::testing::VariantWith<PrecomputedBlankNode>(::testing::AllOf(
      AD_FIELD(PrecomputedBlankNode, prefix_, std::string(prefix)),
      AD_FIELD(PrecomputedBlankNode, suffix_, std::string(suffix))));
};

static constexpr auto matchSingleTriple = [](const auto& s, const auto& p,
                                             const auto& o) {
  return ElementsAre(ElementsAre(s, p, o));
};

// Matches a ground `EvaluatedTriple` by the raw `rdfTermString_` of each of its
// three (constant) terms. Ground triples are pre-instantiated at preprocessing
// time, so each term is a resolved `EvaluatedTerm` rather than a
// `PreprocessedTerm` variant.
static constexpr auto matchGroundTriple = [](const auto& s, const auto& p,
                                             const auto& o) {
  return ::testing::AllOf(
      AD_FIELD(EvaluatedTriple, subject_,
               ::testing::Pointee(AD_FIELD(EvaluatedTermData, rdfTermString_,
                                           std::string(s)))),
      AD_FIELD(EvaluatedTriple, predicate_,
               ::testing::Pointee(AD_FIELD(EvaluatedTermData, rdfTermString_,
                                           std::string(p)))),
      AD_FIELD(EvaluatedTriple, object_,
               ::testing::Pointee(AD_FIELD(EvaluatedTermData, rdfTermString_,
                                           std::string(o)))));
};

auto Const = matchesPrecomputedConstant;
auto Var = matchesPrecomputedVariable;
auto Bnode = matchesPrecomputedBlankNode;

// A shared test index, used to resolve constant template terms to their
// deduplication `ValueId`. The concrete vocabulary contents are irrelevant to
// these tests (they assert on the string/variant structure, not on the
// `dedupId_`); a real index is needed only because `preprocess` now requires
// one. Constructed once and reused across all tests.
const Index& testIndex() {
  static const Index& index = ad_utility::testing::getQec()->getIndex();
  return index;
}

// A shared mutable `LocalVocab` for the `preprocessTerm` call sites (which take
// it by reference to hold the entries backing constant literal `dedupId_`s).
// These tests assert only on the string/variant structure, so a single reused
// instance is sufficient.
LocalVocab& testLocalVocab() {
  static LocalVocab localVocab;
  return localVocab;
}

TEST(ConstructTemplatePreprocessorTest, preprocessIri) {
  Triples triples;
  triples.push_back({GraphTerm{Iri{"<http://s>"}}, GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Iri{"<http://o>"}}});

  VariableToColumnMap varMap;
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  // A fully-constant triple is classified as ground: it is pre-instantiated
  // into `groundTriples_` and excluded from `preprocessedTriples_`.
  EXPECT_TRUE(result.preprocessedTriples_.empty());
  EXPECT_THAT(
      result.groundTriples_,
      ElementsAre(matchGroundTriple("<http://s>", "<http://p>", "<http://o>")));

  EXPECT_TRUE(result.uniqueVariableColumns_.empty());
}

TEST(ConstructTemplatePreprocessorTest, preprocessLiteralInObjectPosition) {
  Triples triples;
  triples.push_back({GraphTerm{Iri{"<http://s>"}}, GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Literal{"\"hello\""}}});

  VariableToColumnMap varMap;
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  // Constant subject/predicate and a constant literal object -> ground triple.
  EXPECT_TRUE(result.preprocessedTriples_.empty());
  EXPECT_THAT(
      result.groundTriples_,
      ElementsAre(matchGroundTriple("<http://s>", "<http://p>", "\"hello\"")));

  EXPECT_TRUE(result.uniqueVariableColumns_.empty());
}

TEST(ConstructTemplatePreprocessorTest, preprocessLiteralInSubjectPosition) {
  // Literals in subject position are invalid; evaluate returns nullopt,
  // so the preprocessor throws away that triple entirely, that is, the
  // preprocessedTriples do not contain that template triple.
  Triples triples;
  triples.push_back({GraphTerm{Literal{"invalid"}},
                     GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Iri{"<http://o>"}}});

  VariableToColumnMap varMap;
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  EXPECT_TRUE(result.preprocessedTriples_.empty());
  EXPECT_TRUE(result.uniqueVariableColumns_.empty());
}

TEST(ConstructTemplatePreprocessorTest, preprocessVariableBound) {
  Triples triples;
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Iri{"<http://o>"}}});

  VariableToColumnMap varMap;
  varMap[Variable{"?x"}] = makeAlwaysDefinedColumn(3);
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  // After preprocessing, columnIndex_ holds the original IdTable column (3).
  EXPECT_THAT(
      result.preprocessedTriples_,
      matchSingleTriple(Var(3), Const("<http://p>"), Const("<http://o>")));

  // The unique variable columns should contain column 3.
  ASSERT_EQ(result.uniqueVariableColumns_.size(), 1);
  EXPECT_EQ(result.uniqueVariableColumns_[0], 3);
}

TEST(ConstructTemplatePreprocessorTest, preprocessVariableUnbound) {
  // A triple with an unbound variable is dropped entirely.
  Triples triples;
  triples.push_back({GraphTerm{Variable{"?unbound"}},
                     GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Iri{"<http://o>"}}});

  VariableToColumnMap varMap;  // ?unbound is not in the map
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  EXPECT_TRUE(result.preprocessedTriples_.empty());
  EXPECT_TRUE(result.uniqueVariableColumns_.empty());
}

TEST(ConstructTemplatePreprocessorTest, preprocessBlankNodeUserDefined) {
  Triples triples;
  triples.push_back({GraphTerm{BlankNode{false, "myNode"}},
                     GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Iri{"<http://o>"}}});

  VariableToColumnMap varMap;
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  EXPECT_THAT(result.preprocessedTriples_,
              matchSingleTriple(Bnode("_:u", "_myNode"), Const("<http://p>"),
                                Const("<http://o>")));

  ASSERT_TRUE(result.uniqueVariableColumns_.empty());
}

TEST(ConstructTemplatePreprocessorTest, preprocessBlankNodeGenerated) {
  Triples triples;
  triples.push_back({GraphTerm{BlankNode{true, "gen"}},
                     GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Iri{"<http://o>"}}});

  VariableToColumnMap varMap;
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  EXPECT_THAT(result.preprocessedTriples_,
              matchSingleTriple(Bnode("_:g", "_gen"), Const("<http://p>"),
                                Const("<http://o>")));

  ASSERT_TRUE(result.uniqueVariableColumns_.empty());
}

TEST(ConstructTemplatePreprocessorTest, emptyTriples) {
  Triples triples;
  VariableToColumnMap varMap;
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  EXPECT_TRUE(result.preprocessedTriples_.empty());
  EXPECT_TRUE(result.uniqueVariableColumns_.empty());
}

TEST(ConstructTemplatePreprocessorTest,
     sameVariableWithinSingleTripleDeduplicates) {
  // ?x appears in subject and object of the same triple: one unique column.
  Triples triples;
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Variable{"?x"}}});

  VariableToColumnMap varMap;
  varMap[Variable{"?x"}] = makeAlwaysDefinedColumn(5);
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  EXPECT_THAT(result.preprocessedTriples_,
              matchSingleTriple(Var(5), Const("<http://p>"), Var(5)));

  ASSERT_EQ(result.uniqueVariableColumns_.size(), 1);
  EXPECT_EQ(result.uniqueVariableColumns_[0], 5);
}

TEST(ConstructTemplatePreprocessorTest,
     sameVariableAcrossMultipleTriplesDeduplicates) {
  // ?x appears in two different triples: still one unique column.
  Triples triples;
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{Iri{"<http://p1>"}},
                     GraphTerm{Iri{"<http://o1>"}}});
  triples.push_back({GraphTerm{Iri{"<http://s2>"}},
                     GraphTerm{Iri{"<http://p2>"}}, GraphTerm{Variable{"?x"}}});

  VariableToColumnMap varMap;
  varMap[Variable{"?x"}] = makeAlwaysDefinedColumn(2);
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  EXPECT_THAT(
      result.preprocessedTriples_,
      ElementsAre(
          ElementsAre(Var(2), Const("<http://p1>"), Const("<http://o1>")),
          ElementsAre(Const("<http://s2>"), Const("<http://p2>"), Var(2))));

  ASSERT_EQ(result.uniqueVariableColumns_.size(), 1);
  EXPECT_EQ(result.uniqueVariableColumns_[0], 2);
}

TEST(ConstructTemplatePreprocessorTest,
     differentVariablesCollectMultipleColumns) {
  // ?x and ?y are different variables with different columns.
  Triples triples;
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Variable{"?y"}}});

  VariableToColumnMap varMap;
  varMap[Variable{"?x"}] = makeAlwaysDefinedColumn(0);
  varMap[Variable{"?y"}] = makeAlwaysDefinedColumn(1);
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  EXPECT_THAT(result.preprocessedTriples_,
              matchSingleTriple(Var(0), Const("<http://p>"), Var(1)));

  EXPECT_THAT(result.uniqueVariableColumns_, UnorderedElementsAre(0, 1));
}

TEST(ConstructTemplatePreprocessorTest,
     multipleVariablesAcrossTriplesDeduplicates) {
  // Three triples with ?x, ?y, ?z. ?x appears in two triples.
  // Expected: 3 unique columns (for ?x, ?y, ?z).
  Triples triples;
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{Iri{"<http://p1>"}},
                     GraphTerm{Variable{"?y"}}});
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{Iri{"<http://p2>"}},
                     GraphTerm{Variable{"?z"}}});
  triples.push_back({GraphTerm{Variable{"?y"}}, GraphTerm{Iri{"<http://p3>"}},
                     GraphTerm{Variable{"?z"}}});

  VariableToColumnMap varMap;
  varMap[Variable{"?x"}] = makeAlwaysDefinedColumn(0);
  varMap[Variable{"?y"}] = makeAlwaysDefinedColumn(1);
  varMap[Variable{"?z"}] = makeAlwaysDefinedColumn(2);
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  EXPECT_THAT(result.preprocessedTriples_,
              ElementsAre(ElementsAre(Var(0), Const("<http://p1>"), Var(1)),
                          ElementsAre(Var(0), Const("<http://p2>"), Var(2)),
                          ElementsAre(Var(1), Const("<http://p3>"), Var(2))));

  ASSERT_EQ(result.uniqueVariableColumns_.size(), 3);
  EXPECT_THAT(result.uniqueVariableColumns_, UnorderedElementsAre(0, 1, 2));
}

TEST(ConstructTemplatePreprocessorTest, unboundVariableDropsTriple) {
  // ?x is bound (column 0), ?unbound is not in the map.
  // The entire triple is dropped because ?unbound is undefined.
  Triples triples;
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Variable{"?unbound"}}});

  VariableToColumnMap varMap;
  varMap[Variable{"?x"}] = makeAlwaysDefinedColumn(0);
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  EXPECT_TRUE(result.preprocessedTriples_.empty());
  EXPECT_TRUE(result.uniqueVariableColumns_.empty());
}

TEST(ConstructTemplatePreprocessorTest,
     unboundVariableDropsOnlyAffectedTriple) {
  // Triple 1 has ?unbound (not in varMap) -> dropped.
  // Triple 2 has ?x (bound, column 0) -> kept.
  // ?x should still appear in uniqueVariableColumns_.
  Triples triples;
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Variable{"?unbound"}}});
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Iri{"<http://o>"}}});

  VariableToColumnMap varMap;
  varMap[Variable{"?x"}] = makeAlwaysDefinedColumn(0);
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  EXPECT_THAT(
      result.preprocessedTriples_,
      matchSingleTriple(Var(0), Const("<http://p>"), Const("<http://o>")));

  ASSERT_EQ(result.uniqueVariableColumns_.size(), 1);
  EXPECT_EQ(result.uniqueVariableColumns_[0], 0);
}

TEST(ConstructTemplatePreprocessorTest, multipleTriplesConstantsOnly) {
  Triples triples;
  triples.push_back({GraphTerm{Iri{"<http://s1>"}},
                     GraphTerm{Iri{"<http://p1>"}},
                     GraphTerm{Iri{"<http://o1>"}}});
  triples.push_back({GraphTerm{Iri{"<http://s2>"}},
                     GraphTerm{Iri{"<http://p2>"}},
                     GraphTerm{Iri{"<http://o2>"}}});

  VariableToColumnMap varMap;
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  // Both triples are fully constant, so both become ground triples (in template
  // order) and `preprocessedTriples_` stays empty.
  EXPECT_TRUE(result.preprocessedTriples_.empty());
  EXPECT_THAT(
      result.groundTriples_,
      ElementsAre(
          matchGroundTriple("<http://s1>", "<http://p1>", "<http://o1>"),
          matchGroundTriple("<http://s2>", "<http://p2>", "<http://o2>")));

  EXPECT_TRUE(result.uniqueVariableColumns_.empty());
}

// A ground triple and a non-ground triple are partitioned: the ground triple
// goes to `groundTriples_`, the non-ground triple to `preprocessedTriples_`,
// and the parallel per-triple arrays cover only the non-ground triple (and stay
// aligned with it).
TEST(ConstructTemplatePreprocessorTest, groundAndNonGroundTriplesPartitioned) {
  Triples triples;
  triples.push_back({GraphTerm{Iri{"<http://gs>"}},
                     GraphTerm{Iri{"<http://gp>"}},
                     GraphTerm{Iri{"<http://go>"}}});
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Iri{"<http://o>"}}});

  VariableToColumnMap varMap;
  varMap[Variable{"?x"}] = makeAlwaysDefinedColumn(0);
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  EXPECT_THAT(result.groundTriples_,
              ElementsAre(matchGroundTriple("<http://gs>", "<http://gp>",
                                            "<http://go>")));
  EXPECT_THAT(
      result.preprocessedTriples_,
      matchSingleTriple(Var(0), Const("<http://p>"), Const("<http://o>")));

  // Parallel arrays describe only the single non-ground triple.
  ASSERT_EQ(result.variableColumnsPerTriple_.size(), 1);
  EXPECT_THAT(result.variableColumnsPerTriple_[0], ElementsAre(0));
  ASSERT_EQ(result.tripleContainsBlankNode_.size(), 1);
  EXPECT_FALSE(result.tripleContainsBlankNode_[0]);
  EXPECT_THAT(result.uniqueVariableColumns_, ElementsAre(0));
}

// A triple whose only non-constant term is a blank node is NOT ground: blank
// node IDs are minted per row, so the triple must be instantiated per row and
// therefore stays in `preprocessedTriples_`.
TEST(ConstructTemplatePreprocessorTest, blankNodeTripleIsNotGround) {
  Triples triples;
  triples.push_back({GraphTerm{BlankNode{false, "b"}},
                     GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Iri{"<http://o>"}}});

  VariableToColumnMap varMap;
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  EXPECT_TRUE(result.groundTriples_.empty());
  EXPECT_THAT(result.preprocessedTriples_,
              matchSingleTriple(Bnode("_:u", "_b"), Const("<http://p>"),
                                Const("<http://o>")));
  ASSERT_EQ(result.tripleContainsBlankNode_.size(), 1);
  EXPECT_TRUE(result.tripleContainsBlankNode_[0]);
}

TEST(ConstructTemplatePreprocessorTest, mixedTermTypesAcrossTriples) {
  // Triple 1: IRI, IRI, Variable
  // Triple 2: BlankNode, IRI, Literal
  Triples triples;
  triples.push_back({GraphTerm{Iri{"<http://s>"}}, GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Variable{"?val"}}});
  triples.push_back({GraphTerm{BlankNode{false, "b1"}},
                     GraphTerm{Iri{"<http://q>"}},
                     GraphTerm{Literal{"\"text\""}}});

  VariableToColumnMap varMap;
  varMap[Variable{"?val"}] = makeAlwaysDefinedColumn(4);
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  EXPECT_THAT(
      result.preprocessedTriples_,
      ElementsAre(ElementsAre(Const("<http://s>"), Const("<http://p>"), Var(4)),
                  ElementsAre(Bnode("_:u", "_b1"), Const("<http://q>"),
                              Const("\"text\""))));

  ASSERT_EQ(result.uniqueVariableColumns_.size(), 1);
  EXPECT_EQ(result.uniqueVariableColumns_[0], 4);
}

// =============================================================================
// Tests for ConstructTemplatePreprocessor::preprocessTerm()
// =============================================================================

TEST(ConstructTemplatePreprocessorTest, preprocessTermIri) {
  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocessTerm(
      GraphTerm{Iri{"<http://s>"}}, SUBJECT, varMap, testIndex(),
      testLocalVocab());
  ASSERT_TRUE(result.has_value());

  EXPECT_THAT(result.value(), Const("<http://s>"));
}

TEST(ConstructTemplatePreprocessorTest, preprocessTermLiteralObject) {
  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocessTerm(
      GraphTerm{Literal{"\"hello\""}}, OBJECT, varMap, testIndex(),
      testLocalVocab());
  ASSERT_TRUE(result.has_value());

  EXPECT_THAT(result.value(), Const("\"hello\""));
}

TEST(ConstructTemplatePreprocessorTest, preprocessTermLiteralSubject) {
  // Literals in subject position are invalid; evaluate returns empty string,
  // so preprocessTerm returns nullopt.
  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocessTerm(
      GraphTerm{Literal{"invalid"}}, SUBJECT, varMap, testIndex(),
      testLocalVocab());
  EXPECT_EQ(result, std::nullopt);
}

TEST(ConstructTemplatePreprocessorTest, preprocessTermVariableBound) {
  VariableToColumnMap varMap;
  varMap[Variable{"?x"}] = makeAlwaysDefinedColumn(3);
  auto result = ConstructTemplatePreprocessor::preprocessTerm(
      GraphTerm{Variable{"?x"}}, SUBJECT, varMap, testIndex(),
      testLocalVocab());
  ASSERT_TRUE(result.has_value());

  EXPECT_THAT(result.value(), Var(3));
}

TEST(ConstructTemplatePreprocessorTest, preprocessTermVariableUnbound) {
  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocessTerm(
      GraphTerm{Variable{"?unbound"}}, SUBJECT, varMap, testIndex(),
      testLocalVocab());
  EXPECT_EQ(result, std::nullopt);
}

TEST(ConstructTemplatePreprocessorTest, preprocessTermBlankNodeUser) {
  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocessTerm(
      GraphTerm{BlankNode{false, "myNode"}}, SUBJECT, varMap, testIndex(),
      testLocalVocab());
  ASSERT_TRUE(result.has_value());

  EXPECT_THAT(result.value(), Bnode("_:u", "_myNode"));
}

TEST(ConstructTemplatePreprocessorTest, preprocessTermBlankNodeGenerated) {
  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocessTerm(
      GraphTerm{BlankNode{true, "gen"}}, SUBJECT, varMap, testIndex(),
      testLocalVocab());
  ASSERT_TRUE(result.has_value());

  EXPECT_THAT(result.value(), Bnode("_:g", "_gen"));
}
}  // namespace
