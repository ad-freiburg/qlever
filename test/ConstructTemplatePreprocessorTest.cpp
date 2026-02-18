// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "./util/AllocatorTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "engine/ConstructQueryEvaluator.h"
#include "engine/ConstructTemplatePreprocessor.h"
#include "index/Index.h"
#include "parser/data/ConstructQueryExportContext.h"
#include "parser/data/Types.h"

namespace qlever::constructExport {

// `PrintTo` overloads so gmock shows human-readable output instead of raw
// bytes.
void PrintTo(const PrecomputedConstant& c, std::ostream* os) {
  *os << "PrecomputedConstant{\"" << c.value_ << "\"}";
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

// Minimal context wrapper for tests
struct ContextWrapper {
  Index index_{ad_utility::makeUnlimitedAllocator<Id>()};
  Result resultTable_{
      IdTable{ad_utility::testing::makeAllocator()}, {}, LocalVocab{}};
  VariableToColumnMap variableColumns_{};

  ConstructQueryExportContext createContextForRow(size_t row,
                                                  size_t rowOffset = 0) const {
    return {row,
            resultTable_.idTable(),
            resultTable_.localVocab(),
            variableColumns_,
            index_,
            rowOffset};
  }
};

// Composable matchers for `PreprocessedTerm` variants.
// see https://github.com/google/googletest/blob/main/docs/reference/matchers.md
static constexpr auto matchesPrecomputedConstant(const auto& value) {
  return ::testing::VariantWith<PrecomputedConstant>(
      AD_FIELD(PrecomputedConstant, value_, std::string(value)));
}

static constexpr auto matchesPrecomputedVariable(const auto& columnIdx) {
  return ::testing::VariantWith<PrecomputedVariable>(
      AD_FIELD(PrecomputedVariable, columnIndex_, columnIdx));
}

static constexpr auto matchPrecomputedBlankNode(const auto& prefix,
                                                const auto& suffix) {
  return ::testing::VariantWith<PrecomputedBlankNode>(::testing::AllOf(
      AD_FIELD(PrecomputedBlankNode, prefix_, std::string(prefix)),
      AD_FIELD(PrecomputedBlankNode, suffix_, std::string(suffix))));
}

auto matchConst = [](const auto& value) {
  return matchesPrecomputedConstant(value);
};

auto matchVar = [](const auto& value) {
  return matchesPrecomputedVariable(value);
};

auto matchBnode = [](const auto& prefix, const auto& suffix) {
  return matchPrecomputedBlankNode(prefix, suffix);
};

TEST(ConstructTemplatePreprocessorTest, preprocessIri) {
  Triples triples;
  triples.push_back({GraphTerm{Iri{"<http://s>"}}, GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Iri{"<http://o>"}}});

  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

  ASSERT_EQ(result.preprocessedTriples_.size(), 1);

  EXPECT_THAT(result.preprocessedTriples_,
              ::testing::ElementsAre(::testing::ElementsAre(
                  matchConst("<http://s>"), matchConst("<http://p>"),
                  matchConst("<http://o>"))));

  EXPECT_TRUE(result.uniqueVariableColumns_.empty());
}

TEST(ConstructTemplatePreprocessorTest, preprocessLiteralInObjectPosition) {
  Triples triples;
  triples.push_back({GraphTerm{Iri{"<http://s>"}}, GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Literal{"hello"}}});

  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

  ASSERT_EQ(result.preprocessedTriples_.size(), 1);

  EXPECT_THAT(result.preprocessedTriples_,
              ::testing::ElementsAre(::testing::ElementsAre(
                  matchConst("<http://s>"), matchConst("<http://p>"),
                  matchConst("hello"))));
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
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

  EXPECT_TRUE(result.preprocessedTriples_.empty());
  EXPECT_TRUE(result.uniqueVariableColumns_.empty());
}

TEST(ConstructTemplatePreprocessorTest, preprocessVariableBound) {
  Triples triples;
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Iri{"<http://o>"}}});

  VariableToColumnMap varMap;
  varMap[Variable{"?x"}] = makeAlwaysDefinedColumn(3);
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

  ASSERT_EQ(result.preprocessedTriples_.size(), 1);

  EXPECT_THAT(
      result.preprocessedTriples_,
      ::testing::ElementsAre(::testing::ElementsAre(
          matchVar(3), matchConst("<http://p>"), matchConst("<http://o>"))));

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
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

  EXPECT_TRUE(result.preprocessedTriples_.empty());
  EXPECT_TRUE(result.uniqueVariableColumns_.empty());
}

TEST(ConstructTemplatePreprocessorTest, preprocessBlankNodeUserDefined) {
  Triples triples;
  triples.push_back({GraphTerm{BlankNode{false, "myNode"}},
                     GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Iri{"<http://o>"}}});

  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);
  ASSERT_TRUE(result.uniqueVariableColumns_.empty());

  ASSERT_EQ(result.preprocessedTriples_.size(), 1);

  EXPECT_THAT(result.preprocessedTriples_,
              ::testing::ElementsAre(::testing::ElementsAre(
                  matchBnode("_:u", "_myNode"), matchConst("<http://p>"),
                  matchConst("<http://o>"))));
}

TEST(ConstructTemplatePreprocessorTest, preprocessBlankNodeGenerated) {
  Triples triples;
  triples.push_back({GraphTerm{BlankNode{true, "gen"}},
                     GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Iri{"<http://o>"}}});

  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

  ASSERT_EQ(result.preprocessedTriples_.size(), 1);
  ASSERT_TRUE(result.uniqueVariableColumns_.empty());

  EXPECT_THAT(result.preprocessedTriples_,
              ::testing::ElementsAre(::testing::ElementsAre(
                  matchBnode("_:g", "_gen"), matchConst("<http://p>"),
                  matchConst("<http://o>"))));
}

TEST(ConstructTemplatePreprocessorTest, emptyTriples) {
  Triples triples;
  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

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
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

  ASSERT_EQ(result.uniqueVariableColumns_.size(), 1);
  EXPECT_EQ(result.uniqueVariableColumns_[0], 5);

  ASSERT_EQ(result.preprocessedTriples_.size(), 1);

  EXPECT_THAT(result.preprocessedTriples_,
              ::testing::ElementsAre(::testing::ElementsAre(
                  matchVar(5), matchConst("<http://p>"), matchVar(5))));
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
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

  ASSERT_EQ(result.uniqueVariableColumns_.size(), 1);
  EXPECT_EQ(result.uniqueVariableColumns_[0], 2);

  ASSERT_EQ(result.preprocessedTriples_.size(), 2);

  const auto& triple1 = result.preprocessedTriples_[0];
  EXPECT_THAT(triple1,
              ::testing::ElementsAre(matchVar(2), matchConst("<http://p1>"),
                                     matchConst("<http://o1>")));

  const auto& triple2 = result.preprocessedTriples_[1];
  EXPECT_THAT(triple2,
              ::testing::ElementsAre(matchConst("<http://s2>"),
                                     matchConst("<http://p2>"), matchVar(2)));
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
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

  ASSERT_EQ(result.uniqueVariableColumns_.size(), 2);
  // Order from HashSet is unspecified, so use UnorderedElementsAre.
  EXPECT_THAT(result.uniqueVariableColumns_,
              ::testing::UnorderedElementsAre(0, 1));

  ASSERT_EQ(result.preprocessedTriples_.size(), 1);

  EXPECT_THAT(result.preprocessedTriples_,
              ::testing::ElementsAre(::testing::ElementsAre(
                  matchVar(0), matchConst("<http://p>"), matchVar(1))));
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
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

  ASSERT_EQ(result.preprocessedTriples_.size(), 3);
  ASSERT_EQ(result.uniqueVariableColumns_.size(), 3);
  EXPECT_THAT(result.uniqueVariableColumns_,
              ::testing::UnorderedElementsAre(0, 1, 2));

  ASSERT_EQ(result.preprocessedTriples_.size(), 3);

  auto& triple1 = result.preprocessedTriples_[0];
  EXPECT_THAT(triple1,
              ::testing::ElementsAre(matchVar(0), matchConst("<http://p1>"),
                                     matchVar(1)));

  auto& triple2 = result.preprocessedTriples_[1];
  EXPECT_THAT(triple2,
              ::testing::ElementsAre(matchVar(0), matchConst("<http://p2>"),
                                     matchVar(2)));

  auto& triple3 = result.preprocessedTriples_[2];
  EXPECT_THAT(triple3,
              ::testing::ElementsAre(matchVar(1), matchConst("<http://p3>"),
                                     matchVar(2)));
}

TEST(ConstructTemplatePreprocessorTest, unboundVariableDropsTriple) {
  // ?x is bound (column 0), ?unbound is not in the map.
  // The entire triple is dropped because ?unbound is undefined.
  Triples triples;
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Variable{"?unbound"}}});

  VariableToColumnMap varMap;
  varMap[Variable{"?x"}] = makeAlwaysDefinedColumn(0);
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

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
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

  ASSERT_EQ(result.uniqueVariableColumns_.size(), 1);
  EXPECT_EQ(result.uniqueVariableColumns_[0], 0);

  ASSERT_EQ(result.preprocessedTriples_.size(), 1);

  EXPECT_THAT(
      result.preprocessedTriples_,
      ::testing::ElementsAre(::testing::ElementsAre(
          matchVar(0), matchConst("<http://p>"), matchConst("<http://o>"))));
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
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

  ASSERT_EQ(result.preprocessedTriples_.size(), 2);
  EXPECT_TRUE(result.uniqueVariableColumns_.empty());

  ASSERT_EQ(result.preprocessedTriples_.size(), 2);

  auto& triple1 = result.preprocessedTriples_[0];
  EXPECT_THAT(triple1, ::testing::ElementsAre(matchConst("<http://s1>"),
                                              matchConst("<http://p1>"),
                                              matchConst("<http://o1>")));

  auto& triple2 = result.preprocessedTriples_[1];
  EXPECT_THAT(triple2, ::testing::ElementsAre(matchConst("<http://s2>"),
                                              matchConst("<http://p2>"),
                                              matchConst("<http://o2>")));
}

TEST(ConstructTemplatePreprocessorTest, mixedTermTypesAcrossTriples) {
  // Triple 1: IRI, IRI, Variable
  // Triple 2: BlankNode, IRI, Literal
  Triples triples;
  triples.push_back({GraphTerm{Iri{"<http://s>"}}, GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Variable{"?val"}}});
  triples.push_back({GraphTerm{BlankNode{false, "b1"}},
                     GraphTerm{Iri{"<http://q>"}}, GraphTerm{Literal{"text"}}});

  VariableToColumnMap varMap;
  varMap[Variable{"?val"}] = makeAlwaysDefinedColumn(4);
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

  ASSERT_EQ(result.preprocessedTriples_.size(), 2);

  auto& triple1 = result.preprocessedTriples_[0];
  EXPECT_THAT(triple1,
              ::testing::ElementsAre(matchConst("<http://s>"),
                                     matchConst("<http://p>"), matchVar(4)));

  auto& triple2 = result.preprocessedTriples_[1];
  EXPECT_THAT(triple2, ::testing::ElementsAre(matchBnode("_:u", "_b1"),
                                              matchConst("<http://q>"),
                                              matchConst("text")));

  ASSERT_EQ(result.uniqueVariableColumns_.size(), 1);
  EXPECT_EQ(result.uniqueVariableColumns_[0], 4);
}

// =============================================================================
// Tests for ConstructTemplatePreprocessor::preprocessTerm()
// =============================================================================

TEST(ConstructTemplatePreprocessorTest, preprocessTermIri) {
  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocessTerm(
      GraphTerm{Iri{"<http://s>"}}, SUBJECT, varMap);
  ASSERT_TRUE(result.has_value());

  EXPECT_THAT(result.value(), matchConst("<http://s>"));
}

TEST(ConstructTemplatePreprocessorTest, preprocessTermLiteralObject) {
  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocessTerm(
      GraphTerm{Literal{"hello"}}, OBJECT, varMap);
  ASSERT_TRUE(result.has_value());

  EXPECT_THAT(result.value(), matchConst("hello"));
}

TEST(ConstructTemplatePreprocessorTest, preprocessTermLiteralSubject) {
  // Literals in subject position are invalid; evaluate returns empty string,
  // so preprocessTerm returns nullopt.
  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocessTerm(
      GraphTerm{Literal{"invalid"}}, SUBJECT, varMap);
  EXPECT_EQ(result, std::nullopt);
}

TEST(ConstructTemplatePreprocessorTest, preprocessTermVariableBound) {
  VariableToColumnMap varMap;
  varMap[Variable{"?x"}] = makeAlwaysDefinedColumn(3);
  auto result = ConstructTemplatePreprocessor::preprocessTerm(
      GraphTerm{Variable{"?x"}}, SUBJECT, varMap);
  ASSERT_TRUE(result.has_value());

  EXPECT_THAT(result.value(), matchVar(3));
}

TEST(ConstructTemplatePreprocessorTest, preprocessTermVariableUnbound) {
  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocessTerm(
      GraphTerm{Variable{"?unbound"}}, SUBJECT, varMap);
  EXPECT_EQ(result, std::nullopt);
}

TEST(ConstructTemplatePreprocessorTest, preprocessTermBlankNodeUser) {
  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocessTerm(
      GraphTerm{BlankNode{false, "myNode"}}, SUBJECT, varMap);
  ASSERT_TRUE(result.has_value());

  EXPECT_THAT(result.value(), matchBnode("_:u", "_myNode"));
}

TEST(ConstructTemplatePreprocessorTest, preprocessTermBlankNodeGenerated) {
  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocessTerm(
      GraphTerm{BlankNode{true, "gen"}}, SUBJECT, varMap);
  ASSERT_TRUE(result.has_value());

  EXPECT_THAT(result.value(), matchBnode("_:g", "_gen"));
}
}  // namespace
