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
#include "./util/TripleComponentTestHelpers.h"
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
auto iriV = ad_utility::testing::iriV;

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

auto Const = matchesPrecomputedConstant;
auto Var = matchesPrecomputedVariable;
auto Bnode = matchesPrecomputedBlankNode;

// A shared test index, used to resolve constant template terms to their
// deduplication `ValueId`. The concrete vocabulary contents are irrelevant to
// the structural tests below (they assert on the string/variant structure); a
// real index is needed only because `preprocess` now requires one. Constructed
// once and reused across all tests.
const Index& testIndex() {
  static const Index& index = ad_utility::testing::getQec()->getIndex();
  return index;
}

// A shared mutable `LocalVocab` for the `preprocessTerm` call sites (which take
// it by reference to hold the entries backing constant literal `dedupId_`s).
LocalVocab& testLocalVocab() {
  static LocalVocab localVocab;
  return localVocab;
}

TEST(ConstructTemplatePreprocessorTest, preprocessIri) {
  Triples triples;
  triples.push_back({GraphTerm{iriV("<http://s>")},
                     GraphTerm{iriV("<http://p>")},
                     GraphTerm{iriV("<http://o>")}});

  VariableToColumnMap varMap;
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  EXPECT_THAT(result.preprocessedTriples_,
              matchSingleTriple(Const("<http://s>"), Const("<http://p>"),
                                Const("<http://o>")));

  EXPECT_TRUE(result.uniqueVariableColumns_.empty());
}

TEST(ConstructTemplatePreprocessorTest, preprocessLiteralInObjectPosition) {
  Triples triples;
  triples.push_back({GraphTerm{iriV("<http://s>")},
                     GraphTerm{iriV("<http://p>")},
                     GraphTerm{Literal{"\"hello\""}}});

  VariableToColumnMap varMap;
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  EXPECT_THAT(result.preprocessedTriples_,
              matchSingleTriple(Const("<http://s>"), Const("<http://p>"),
                                Const("\"hello\"")));

  EXPECT_TRUE(result.uniqueVariableColumns_.empty());
}

TEST(ConstructTemplatePreprocessorTest, preprocessLiteralInSubjectPosition) {
  // Literals in subject position are invalid; evaluate returns nullopt,
  // so the preprocessor throws away that triple entirely, that is, the
  // preprocessedTriples do not contain that template triple.
  Triples triples;
  triples.push_back({GraphTerm{Literal{"invalid"}},
                     GraphTerm{iriV("<http://p>")},
                     GraphTerm{iriV("<http://o>")}});

  VariableToColumnMap varMap;
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  EXPECT_TRUE(result.preprocessedTriples_.empty());
  EXPECT_TRUE(result.uniqueVariableColumns_.empty());
}

// A template constant must resolve to the *same* `ValueId` that the identical
// RDF term receives when it is read from the input knowledge graph during index
// building. This is the property the shared full-triple deduplication filter
// relies on: only if equal terms have equal ids can a constant-bearing template
// triple be recognized as equal to a data-bearing one. We veriy this for all
// three id-assignment paths:
//   (a) a vocabulary IRI,
//   (b) a vocabulary (language-tagged) literal,
//   (c) an inline-encoded literal.
TEST(ConstructTemplatePreprocessorTest, constantResolvesToSameValueIdAsData) {
  // Build an index whose data contains exactly the terms used as constants
  // below, so they are present in the vocabulary.
  const Index& index = ad_utility::testing::getQec(
                           "<http://s> <http://p> <http://o> . "
                           "<http://s> <http://p> \"foo\"@en . "
                           "<http://s> <http://p> 1 .")
                           ->getIndex();
  auto getId = ad_utility::testing::makeGetId(index);

  // Preprocess the single template triple `<http://s> <http://p> OBJ` and
  // return the dedup `ValueId` computed for the object constant.
  auto objectDedupId = [&index](GraphTerm object) {
    Triples triples;
    triples.push_back({GraphTerm{iriV("<http://s>")},
                       GraphTerm{iriV("<http://p>")}, std::move(object)});
    VariableToColumnMap varMap;
    auto result =
        ConstructTemplatePreprocessor::preprocess(triples, varMap, index);
    return std::get<PrecomputedConstant>(
               result.preprocessedTriples_.at(0).at(2))
        .dedupId_;
  };

  // (a) IRI constant vs. the same IRI from the data (vocabulary lookup).
  EXPECT_EQ(objectDedupId(GraphTerm{iriV("<http://o>")}), getId("<http://o>"));

  // (b) Non-encoded (language-tagged) literal vs. the same literal from data.
  EXPECT_EQ(objectDedupId(GraphTerm{Literal{"\"foo\"@en"}}),
            getId("\"foo\"@en"));

  // (c) Inline-encoded literal: its `ValueId` is a pure function of the value,
  // independent of the vocabulary.
  EXPECT_EQ(objectDedupId(GraphTerm{
                Literal{"\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>"}}),
            Id::makeFromInt(1));
}

TEST(ConstructTemplatePreprocessorTest, preprocessVariableBound) {
  Triples triples;
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{iriV("<http://p>")},
                     GraphTerm{iriV("<http://o>")}});

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
                     GraphTerm{iriV("<http://p>")},
                     GraphTerm{iriV("<http://o>")}});

  VariableToColumnMap varMap;  // ?unbound is not in the map
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  EXPECT_TRUE(result.preprocessedTriples_.empty());
  EXPECT_TRUE(result.uniqueVariableColumns_.empty());
}

TEST(ConstructTemplatePreprocessorTest, preprocessBlankNodeUserDefined) {
  Triples triples;
  triples.push_back({GraphTerm{BlankNode{false, "myNode"}},
                     GraphTerm{iriV("<http://p>")},
                     GraphTerm{iriV("<http://o>")}});

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
                     GraphTerm{iriV("<http://p>")},
                     GraphTerm{iriV("<http://o>")}});

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
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{iriV("<http://p>")},
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
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{iriV("<http://p1>")},
                     GraphTerm{iriV("<http://o1>")}});
  triples.push_back({GraphTerm{iriV("<http://s2>")},
                     GraphTerm{iriV("<http://p2>")},
                     GraphTerm{Variable{"?x"}}});

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
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{iriV("<http://p>")},
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
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{iriV("<http://p1>")},
                     GraphTerm{Variable{"?y"}}});
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{iriV("<http://p2>")},
                     GraphTerm{Variable{"?z"}}});
  triples.push_back({GraphTerm{Variable{"?y"}}, GraphTerm{iriV("<http://p3>")},
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
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{iriV("<http://p>")},
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
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{iriV("<http://p>")},
                     GraphTerm{Variable{"?unbound"}}});
  triples.push_back({GraphTerm{Variable{"?x"}}, GraphTerm{iriV("<http://p>")},
                     GraphTerm{iriV("<http://o>")}});

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
  triples.push_back({GraphTerm{iriV("<http://s1>")},
                     GraphTerm{iriV("<http://p1>")},
                     GraphTerm{iriV("<http://o1>")}});
  triples.push_back({GraphTerm{iriV("<http://s2>")},
                     GraphTerm{iriV("<http://p2>")},
                     GraphTerm{iriV("<http://o2>")}});

  VariableToColumnMap varMap;
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  EXPECT_THAT(
      result.preprocessedTriples_,
      ElementsAre(ElementsAre(Const("<http://s1>"), Const("<http://p1>"),
                              Const("<http://o1>")),
                  ElementsAre(Const("<http://s2>"), Const("<http://p2>"),
                              Const("<http://o2>"))));

  EXPECT_TRUE(result.uniqueVariableColumns_.empty());
}

TEST(ConstructTemplatePreprocessorTest, mixedTermTypesAcrossTriples) {
  // Triple 1: IRI, IRI, Variable
  // Triple 2: BlankNode, IRI, Literal
  Triples triples;
  triples.push_back({GraphTerm{iriV("<http://s>")},
                     GraphTerm{iriV("<http://p>")},
                     GraphTerm{Variable{"?val"}}});
  triples.push_back({GraphTerm{BlankNode{false, "b1"}},
                     GraphTerm{iriV("<http://q>")},
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
      GraphTerm{iriV("<http://s>")}, SUBJECT, varMap, testIndex(),
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

// Return the resolved deduplication `ValueId` of the constant at position
// `pos` of the preprocessed triple at index `tripleIdx`.
ValueId dedupIdAt(const PreprocessedConstructTemplate& result, size_t tripleIdx,
                  size_t pos) {
  return std::get<PrecomputedConstant>(
             result.preprocessedTriples_.at(tripleIdx).at(pos))
      .dedupId_;
}

// Every constant term (IRI or object literal) is resolved to a defined
// deduplication `ValueId` during preprocessing.
TEST(ConstructTemplatePreprocessorTest, dedupIdIsResolvedForConstants) {
  Triples triples;
  triples.push_back({GraphTerm{iriV("<http://s>")},
                     GraphTerm{iriV("<http://p>")},
                     GraphTerm{Literal{"\"hello\""}}});

  VariableToColumnMap varMap;
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  ASSERT_EQ(result.preprocessedTriples_.size(), 1);
  for (size_t pos = 0; pos < NUM_TRIPLE_POSITIONS; ++pos) {
    EXPECT_FALSE(dedupIdAt(result, 0, pos).isUndefined());
  }
}

// Equal constants resolve to equal ids and distinct constants to distinct ids,
// so the resolved `dedupId_` is a faithful key for full-triple deduplication.
TEST(ConstructTemplatePreprocessorTest, dedupIdIsStableAndDistinct) {
  Triples triples;
  triples.push_back({GraphTerm{iriV("<http://a>")},
                     GraphTerm{iriV("<http://p>")},
                     GraphTerm{iriV("<http://a>")}});
  triples.push_back({GraphTerm{iriV("<http://b>")},
                     GraphTerm{iriV("<http://p>")},
                     GraphTerm{iriV("<http://b>")}});

  VariableToColumnMap varMap;
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  ASSERT_EQ(result.preprocessedTriples_.size(), 2);
  // The same IRI `<http://a>` in subject and object position of triple 0.
  EXPECT_EQ(dedupIdAt(result, 0, 0), dedupIdAt(result, 0, 2));
  // The shared predicate `<http://p>` across both triples.
  EXPECT_EQ(dedupIdAt(result, 0, 1), dedupIdAt(result, 1, 1));
  // Distinct IRIs `<http://a>` vs `<http://b>` must differ.
  EXPECT_NE(dedupIdAt(result, 0, 0), dedupIdAt(result, 1, 0));
}

// `tripleContainsBlankNode_` is parallel to `preprocessedTriples_` and flags
// exactly the triples that contain a blank node in any position.
TEST(ConstructTemplatePreprocessorTest, tripleContainsBlankNodeFlag) {
  Triples triples;
  // Triple 0: all constants -> no blank node.
  triples.push_back({GraphTerm{iriV("<http://s>")},
                     GraphTerm{iriV("<http://p>")},
                     GraphTerm{iriV("<http://o>")}});
  // Triple 1: subject is a blank node -> flagged.
  triples.push_back({GraphTerm{BlankNode{false, "b"}},
                     GraphTerm{iriV("<http://p>")},
                     GraphTerm{iriV("<http://o>")}});

  VariableToColumnMap varMap;
  auto result =
      ConstructTemplatePreprocessor::preprocess(triples, varMap, testIndex());

  EXPECT_THAT(result.tripleContainsBlankNode_, ElementsAre(false, true));
}
}  // namespace
