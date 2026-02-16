// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>

#include "./util/AllocatorTestHelpers.h"
#include "engine/ConstructQueryEvaluator.h"
#include "engine/ConstructTemplatePreprocessor.h"
#include "index/Index.h"
#include "parser/data/ConstructQueryExportContext.h"
#include "parser/data/Types.h"

using namespace std::string_literals;
using ::testing::Optional;
using enum PositionInTriple;

using namespace qlever::constructExport;
using Triples = ad_utility::sparql_types::Triples;

namespace {
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
}  // namespace

// =============================================================================
// Tests for ConstructTemplatePreprocessor::preprocess()
// =============================================================================

TEST(ConstructTemplatePreprocessorTest, preprocessIri) {
  Triples triples;
  triples.push_back({GraphTerm{Iri{"<http://s>"}}, GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Iri{"<http://o>"}}});

  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

  ASSERT_EQ(result.preprocessedTriples_.size(), 1);
  const auto& triple = result.preprocessedTriples_[0];

  // All three positions should be PrecomputedConstant with the IRI string.
  ASSERT_TRUE(std::holds_alternative<PrecomputedConstant>(triple[0]));
  ASSERT_TRUE(std::holds_alternative<PrecomputedConstant>(triple[1]));
  ASSERT_TRUE(std::holds_alternative<PrecomputedConstant>(triple[2]));

  EXPECT_EQ(std::get<PrecomputedConstant>(triple[0]).value_, "<http://s>");
  EXPECT_EQ(std::get<PrecomputedConstant>(triple[1]).value_, "<http://p>");
  EXPECT_EQ(std::get<PrecomputedConstant>(triple[2]).value_, "<http://o>");

  EXPECT_TRUE(result.uniqueVariableColumns_.empty());
}

TEST(ConstructTemplatePreprocessorTest, preprocessLiteralInObjectPosition) {
  Triples triples;
  triples.push_back({GraphTerm{Iri{"<http://s>"}}, GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Literal{"hello"}}});

  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

  const auto& objectTerm = result.preprocessedTriples_[0][2];
  ASSERT_TRUE(std::holds_alternative<PrecomputedConstant>(objectTerm));
  EXPECT_EQ(std::get<PrecomputedConstant>(objectTerm).value_, "hello");
}

TEST(ConstructTemplatePreprocessorTest, preprocessLiteralInSubjectPosition) {
  // Literals in subject position are invalid; evaluate returns nullopt,
  // so the preprocessor stores empty string.
  Triples triples;
  triples.push_back({GraphTerm{Literal{"invalid"}},
                     GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Iri{"<http://o>"}}});

  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

  const auto& subjectTerm = result.preprocessedTriples_[0][0];
  ASSERT_TRUE(std::holds_alternative<PrecomputedConstant>(subjectTerm));
  EXPECT_EQ(std::get<PrecomputedConstant>(subjectTerm).value_, "");
}

TEST(ConstructTemplatePreprocessorTest, preprocessVariableBound) {
  Triples triples;
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Iri{"<http://o>"}}});

  VariableToColumnMap varMap;
  varMap[Variable{"?x"}] = makeAlwaysDefinedColumn(3);
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

  const auto& subjectTerm = result.preprocessedTriples_[0][0];
  ASSERT_TRUE(std::holds_alternative<PrecomputedVariable>(subjectTerm));
  EXPECT_EQ(std::get<PrecomputedVariable>(subjectTerm).columnIndex_, 3);

  // The unique variable columns should contain column 3.
  ASSERT_EQ(result.uniqueVariableColumns_.size(), 1);
  EXPECT_EQ(result.uniqueVariableColumns_[0], 3);
}

TEST(ConstructTemplatePreprocessorTest, preprocessVariableUnbound) {
  Triples triples;
  triples.push_back({GraphTerm{Variable{"?unbound"}},
                     GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Iri{"<http://o>"}}});

  VariableToColumnMap varMap;  // ?unbound is not in the map
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

  const auto& subjectTerm = result.preprocessedTriples_[0][0];
  ASSERT_TRUE(std::holds_alternative<PrecomputedVariable>(subjectTerm));
  EXPECT_EQ(std::get<PrecomputedVariable>(subjectTerm).columnIndex_,
            std::nullopt);

  EXPECT_TRUE(result.uniqueVariableColumns_.empty());
}

TEST(ConstructTemplatePreprocessorTest, preprocessBlankNodeUserDefined) {
  Triples triples;
  triples.push_back({GraphTerm{BlankNode{false, "myNode"}},
                     GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Iri{"<http://o>"}}});

  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

  const auto& subjectTerm = result.preprocessedTriples_[0][0];
  ASSERT_TRUE(std::holds_alternative<PrecomputedBlankNode>(subjectTerm));
  const auto& bn = std::get<PrecomputedBlankNode>(subjectTerm);
  EXPECT_EQ(bn.prefix_, "_:u");
  EXPECT_EQ(bn.suffix_, "_myNode");
}

TEST(ConstructTemplatePreprocessorTest, preprocessBlankNodeGenerated) {
  Triples triples;
  triples.push_back({GraphTerm{BlankNode{true, "gen"}},
                     GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Iri{"<http://o>"}}});

  VariableToColumnMap varMap;
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

  const auto& subjectTerm = result.preprocessedTriples_[0][0];
  ASSERT_TRUE(std::holds_alternative<PrecomputedBlankNode>(subjectTerm));
  const auto& bn = std::get<PrecomputedBlankNode>(subjectTerm);
  EXPECT_EQ(bn.prefix_, "_:g");
  EXPECT_EQ(bn.suffix_, "_gen");
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
}

TEST(ConstructTemplatePreprocessorTest, unboundVariablesNotInUniqueColumns) {
  // ?x is bound (column 0), ?unbound is not in the map.
  // Only column 0 should appear in uniqueVariableColumns_.
  Triples triples;
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{Iri{"<http://p>"}},
                     GraphTerm{Variable{"?unbound"}}});

  VariableToColumnMap varMap;
  varMap[Variable{"?x"}] = makeAlwaysDefinedColumn(0);
  auto result = ConstructTemplatePreprocessor::preprocess(triples, varMap);

  ASSERT_EQ(result.uniqueVariableColumns_.size(), 1);
  EXPECT_EQ(result.uniqueVariableColumns_[0], 0);

  // Verify the unbound variable has nullopt column index.
  const auto& objectTerm = result.preprocessedTriples_[0][2];
  ASSERT_TRUE(std::holds_alternative<PrecomputedVariable>(objectTerm));
  EXPECT_EQ(std::get<PrecomputedVariable>(objectTerm).columnIndex_,
            std::nullopt);
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

  // Verify values of second triple.
  EXPECT_EQ(
      std::get<PrecomputedConstant>(result.preprocessedTriples_[1][0]).value_,
      "<http://s2>");
  EXPECT_EQ(
      std::get<PrecomputedConstant>(result.preprocessedTriples_[1][1]).value_,
      "<http://p2>");
  EXPECT_EQ(
      std::get<PrecomputedConstant>(result.preprocessedTriples_[1][2]).value_,
      "<http://o2>");
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

  // Triple 1: subject=constant, predicate=constant, object=variable(col 4)
  EXPECT_TRUE(std::holds_alternative<PrecomputedConstant>(
      result.preprocessedTriples_[0][0]));
  EXPECT_TRUE(std::holds_alternative<PrecomputedVariable>(
      result.preprocessedTriples_[0][2]));
  EXPECT_EQ(std::get<PrecomputedVariable>(result.preprocessedTriples_[0][2])
                .columnIndex_,
            4);

  // Triple 2: subject=blanknode, predicate=constant, object=constant("text")
  EXPECT_TRUE(std::holds_alternative<PrecomputedBlankNode>(
      result.preprocessedTriples_[1][0]));
  EXPECT_TRUE(std::holds_alternative<PrecomputedConstant>(
      result.preprocessedTriples_[1][2]));
  EXPECT_EQ(
      std::get<PrecomputedConstant>(result.preprocessedTriples_[1][2]).value_,
      "text");

  ASSERT_EQ(result.uniqueVariableColumns_.size(), 1);
  EXPECT_EQ(result.uniqueVariableColumns_[0], 4);
}

// =============================================================================
// Tests for ConstructQueryEvaluator::evaluatePreprocessed()
// =============================================================================

TEST(ConstructQueryEvaluatorTest, evaluatePreprocessedConstant) {
  PrecomputedConstant constant{"<http://example.org>"};
  EXPECT_EQ(ConstructQueryEvaluator::evaluatePreprocessed(constant),
            "<http://example.org>");
}

TEST(ConstructQueryEvaluatorTest, evaluatePreprocessedBlankNode) {
  ContextWrapper wrapper;
  PrecomputedBlankNode bn{"_:u", "_label"};

  auto ctx0 = wrapper.createContextForRow(0);
  EXPECT_EQ(ConstructQueryEvaluator::evaluatePreprocessed(bn, ctx0),
            "_:u0_label");

  auto ctx10 = wrapper.createContextForRow(10);
  EXPECT_EQ(ConstructQueryEvaluator::evaluatePreprocessed(bn, ctx10),
            "_:u10_label");

  // With row offset: rowOffset=5 + rowIndex=7 = 12
  auto ctx12 = wrapper.createContextForRow(7, 5);
  EXPECT_EQ(ConstructQueryEvaluator::evaluatePreprocessed(bn, ctx12),
            "_:u12_label");
}

TEST(ConstructQueryEvaluatorTest, evaluatePreprocessedBlankNodeGenerated) {
  ContextWrapper wrapper;
  PrecomputedBlankNode bn{"_:g", "_gen"};

  auto ctx = wrapper.createContextForRow(3, 10);
  EXPECT_EQ(ConstructQueryEvaluator::evaluatePreprocessed(bn, ctx),
            "_:g13_gen");
}

// =============================================================================
// Tests for evaluatePreprocessedTerm (variant dispatch)
// =============================================================================

TEST(ConstructQueryEvaluatorTest, evaluatePreprocessedTermConstant) {
  ContextWrapper wrapper;
  PreprocessedTerm term = PrecomputedConstant{"<http://test>"};

  auto ctx = wrapper.createContextForRow(0);
  EXPECT_THAT(ConstructQueryEvaluator::evaluatePreprocessedTerm(term, ctx),
              Optional("<http://test>"s));
}

TEST(ConstructQueryEvaluatorTest, evaluatePreprocessedTermBlankNode) {
  ContextWrapper wrapper;
  PreprocessedTerm term = PrecomputedBlankNode{"_:u", "_x"};

  auto ctx = wrapper.createContextForRow(5, 10);
  EXPECT_THAT(ConstructQueryEvaluator::evaluatePreprocessedTerm(term, ctx),
              Optional("_:u15_x"s));
}

// =============================================================================
// Tests for evaluatePreprocessedTriple
// =============================================================================

TEST(ConstructQueryEvaluatorTest, evaluatePreprocessedTripleAllConstants) {
  ContextWrapper wrapper;
  PreprocessedTriple triple = {PrecomputedConstant{"<http://s>"},
                               PrecomputedConstant{"<http://p>"},
                               PrecomputedConstant{"<http://o>"}};

  auto ctx = wrapper.createContextForRow(0);
  auto result =
      ConstructQueryEvaluator::evaluatePreprocessedTriple(triple, ctx);

  EXPECT_EQ(result.subject_, "<http://s>");
  EXPECT_EQ(result.predicate_, "<http://p>");
  EXPECT_EQ(result.object_, "<http://o>");
  EXPECT_FALSE(result.isEmpty());
}

TEST(ConstructQueryEvaluatorTest, evaluatePreprocessedTripleWithBlankNode) {
  ContextWrapper wrapper;
  PreprocessedTriple triple = {PrecomputedBlankNode{"_:u", "_node"},
                               PrecomputedConstant{"<http://p>"},
                               PrecomputedConstant{"literal"}};

  auto ctx = wrapper.createContextForRow(3, 7);
  auto result =
      ConstructQueryEvaluator::evaluatePreprocessedTriple(triple, ctx);

  EXPECT_EQ(result.subject_, "_:u10_node");
  EXPECT_EQ(result.predicate_, "<http://p>");
  EXPECT_EQ(result.object_, "literal");
}
