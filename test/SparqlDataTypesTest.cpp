// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gmock/gmock.h>

#include "./util/AllocatorTestHelpers.h"
#include "engine/ConstructQueryEvaluator.h"
#include "index/Index.h"
#include "parser/data/ConstructQueryExportContext.h"
#include "parser/data/Types.h"

using namespace std::string_literals;
using ::testing::Optional;
using enum PositionInTriple;

namespace {
struct ContextWrapper {
  Index _index{ad_utility::makeUnlimitedAllocator<Id>()};
  Result _resultTable{
      IdTable{ad_utility::testing::makeAllocator()}, {}, LocalVocab{}};
  // TODO<joka921> `VariableToColumnMap`
  VariableToColumnMap _hashMap{};

  ConstructQueryExportContext createContextForRow(size_t row,
                                                  size_t rowOffset = 0) const {
    return {row,
            _resultTable.idTable(),
            _resultTable.localVocab(),
            _hashMap,
            _index,
            rowOffset};
  }

  void setIdTable(IdTable&& table) {
    _resultTable =
        Result{std::move(table), {}, _resultTable.getSharedLocalVocab()};
  }
};

ContextWrapper prepareContext() { return {}; }
}  // namespace

TEST(SparqlDataTypesTest, BlankNodeInvalidLabelsThrowException) {
  EXPECT_THROW(BlankNode(false, ""), ad_utility::Exception);
  EXPECT_THROW(BlankNode(false, "label with spaces"), ad_utility::Exception);
  EXPECT_THROW(BlankNode(false, "trailing-dash-"), ad_utility::Exception);
  EXPECT_THROW(BlankNode(false, "-leading-dash"), ad_utility::Exception);
  EXPECT_THROW(BlankNode(false, "trailing.dots."), ad_utility::Exception);
  EXPECT_THROW(BlankNode(false, ".leading.dots"), ad_utility::Exception);
}

TEST(SparqlDataTypesTest, BlankNodeEvaluatesCorrectlyBasedOnContext) {
  auto wrapper = prepareContext();

  BlankNode blankNodeA{false, "a"};
  BlankNode blankNodeB{true, "b"};
  ConstructQueryExportContext context0 = wrapper.createContextForRow(0);

  // BlankNode evaluation doesn't depend on position
  EXPECT_THAT(ConstructQueryEvaluator::evaluateBlankNode(blankNodeA, context0),
              Optional("_:u0_a"s));
  EXPECT_THAT(ConstructQueryEvaluator::evaluateBlankNode(blankNodeB, context0),
              Optional("_:g0_b"s));

  ConstructQueryExportContext context10 = wrapper.createContextForRow(10);

  EXPECT_THAT(ConstructQueryEvaluator::evaluateBlankNode(blankNodeA, context10),
              Optional("_:u10_a"s));
  EXPECT_THAT(ConstructQueryEvaluator::evaluateBlankNode(blankNodeB, context10),
              Optional("_:g10_b"s));

  ConstructQueryExportContext context12 = wrapper.createContextForRow(7, 5);

  EXPECT_THAT(ConstructQueryEvaluator::evaluateBlankNode(blankNodeA, context12),
              Optional("_:u12_a"s));
  EXPECT_THAT(ConstructQueryEvaluator::evaluateBlankNode(blankNodeB, context12),
              Optional("_:g12_b"s));
}

TEST(SparqlDataTypesTest, BlankNodeEvaluateIsPropagatedCorrectly) {
  auto wrapper = prepareContext();

  BlankNode blankNode{false, "label"};
  ConstructQueryExportContext context = wrapper.createContextForRow(42);

  auto expectedLabel = Optional("_:u42_label"s);

  EXPECT_THAT(ConstructQueryEvaluator::evaluateBlankNode(blankNode, context),
              expectedLabel);
  EXPECT_THAT(
      ConstructQueryEvaluator::evaluate(GraphTerm{blankNode}, context, SUBJECT),
      expectedLabel);
}

// Note: The unified rdfTypes/Iri class no longer validates in the constructor
// because it is used in many internal contexts. IRI validation happens at
// parse time via fromIriref and related factory methods.

TEST(SparqlDataTypesTest, IriValidIriIsPreserved) {
  ASSERT_EQ(Iri{"<http://valid-iri>"}.toStringRepresentation(),
            "<http://valid-iri>");
}

TEST(SparqlDataTypesTest, IriEvaluatesCorrectlyBasedOnContext) {
  std::string iriString{"<http://some-iri>"};
  Iri iri{iriString};

  // Iri evaluation doesn't depend on context or position
  EXPECT_THAT(ConstructQueryEvaluator::evaluateIri(iri), Optional(iriString));
}

TEST(SparqlDataTypesTest, IriEvaluateIsPropagatedCorrectly) {
  auto wrapper = prepareContext();

  Iri iri{"<http://some-iri>"};
  ConstructQueryExportContext context = wrapper.createContextForRow(42);

  auto expectedString = Optional("<http://some-iri>"s);

  EXPECT_THAT(ConstructQueryEvaluator::evaluateIri(iri), expectedString);
  EXPECT_THAT(
      ConstructQueryEvaluator::evaluate(GraphTerm{iri}, context, SUBJECT),
      expectedString);
}

TEST(SparqlDataTypesTest, LiteralBooleanIsCorrectlyFormatted) {
  EXPECT_EQ(Literal::literalWithoutQuotes("true").toStringRepresentation(),
            "\"true\"");
  EXPECT_EQ(Literal::literalWithoutQuotes("false").toStringRepresentation(),
            "\"false\"");
}

TEST(SparqlDataTypesTest, LiteralStringIsCorrectlyFormatted) {
  EXPECT_EQ(Literal::literalWithoutQuotes("abcdef").toStringRepresentation(),
            "\"abcdef\"");
  EXPECT_EQ(
      Literal::literalWithoutQuotes("\U0001f937\U0001f3fc\u200d\u2642\ufe0f")
          .toStringRepresentation(),
      "\"🤷🏼‍♂️\"");
}

TEST(SparqlDataTypesTest, LiteralEvaluatesCorrectlyBasedOnContext) {
  std::string literalString{"\"true\""};
  Literal literal = Literal::literalWithoutQuotes("true");

  EXPECT_EQ(ConstructQueryEvaluator::evaluateLiteral(literal, SUBJECT),
            std::nullopt);
  EXPECT_EQ(ConstructQueryEvaluator::evaluateLiteral(literal, PREDICATE),
            std::nullopt);
  EXPECT_THAT(ConstructQueryEvaluator::evaluateLiteral(literal, OBJECT),
              Optional(literalString));
}

TEST(SparqlDataTypesTest, LiteralEvaluateIsPropagatedCorrectly) {
  auto wrapper = prepareContext();

  Literal literal = Literal::literalWithoutQuotes("some literal");
  ConstructQueryExportContext context = wrapper.createContextForRow(42);

  EXPECT_EQ(ConstructQueryEvaluator::evaluateLiteral(literal, SUBJECT),
            std::nullopt);
  EXPECT_EQ(
      ConstructQueryEvaluator::evaluate(GraphTerm{literal}, context, SUBJECT),
      std::nullopt);
  EXPECT_EQ(
      ConstructQueryEvaluator::evaluate(GraphTerm{literal}, context, SUBJECT),
      std::nullopt);

  auto expectedString = Optional("\"some literal\""s);

  EXPECT_THAT(ConstructQueryEvaluator::evaluateLiteral(literal, OBJECT),
              expectedString);
  EXPECT_THAT(
      ConstructQueryEvaluator::evaluate(GraphTerm{literal}, context, OBJECT),
      expectedString);
  EXPECT_THAT(
      ConstructQueryEvaluator::evaluate(GraphTerm{literal}, context, OBJECT),
      expectedString);
}

TEST(SparqlDataTypesTest, VariableNormalizesDollarSign) {
  Variable varWithQuestionMark{"?abc"};
  EXPECT_EQ(varWithQuestionMark.name(), "?abc");

  Variable varWithDollarSign{"$abc"};
  EXPECT_EQ(varWithQuestionMark.name(), "?abc");
}

TEST(SparqlDataTypesTest, VariableInvalidNamesThrowException) {
  if constexpr (!ad_utility::areExpensiveChecksEnabled) {
    GTEST_SKIP()
        << "validity of variable names is only checked with expensive checks";
  }
  EXPECT_THROW(Variable("no_leading_var_or_dollar", true),
               ad_utility::Exception);
  EXPECT_THROW(Variable("", true), ad_utility::Exception);
  EXPECT_THROW(Variable("? var with space", true), ad_utility::Exception);
  EXPECT_THROW(Variable("?", true), ad_utility::Exception);
  EXPECT_THROW(Variable("$", true), ad_utility::Exception);
}

TEST(SparqlDataTypesTest, VariableEvaluatesCorrectlyBasedOnContext) {
  auto wrapper = prepareContext();

  wrapper._hashMap[Variable{"?var"}] = makeAlwaysDefinedColumn(0);
  IdTable table{ad_utility::testing::makeAllocator()};
  table.setNumColumns(1);
  Id value1 = Id::makeFromInt(69);
  Id value2 = Id::makeFromInt(420);
  table.push_back({value1});
  table.push_back({value2});

  wrapper.setIdTable(std::move(table));

  Variable variable{"?var"};
  ConstructQueryExportContext context0 = wrapper.createContextForRow(0);

  EXPECT_THAT(ConstructQueryEvaluator::evaluateVar(variable, context0),
              Optional("69"s));

  // Row offset should be ignored.
  ConstructQueryExportContext context0b = wrapper.createContextForRow(0, 42);

  EXPECT_THAT(ConstructQueryEvaluator::evaluateVar(variable, context0b),
              Optional("69"s));

  ConstructQueryExportContext context1 = wrapper.createContextForRow(1);

  EXPECT_THAT(ConstructQueryEvaluator::evaluateVar(variable, context1),
              Optional("420"s));
}

TEST(SparqlDataTypesTest, VariableEvaluatesNothingForUnusedName) {
  auto wrapper = prepareContext();

  Variable variable{"?var"};
  ConstructQueryExportContext context0 = wrapper.createContextForRow(0);

  EXPECT_EQ(ConstructQueryEvaluator::evaluateVar(variable, context0),
            std::nullopt);

  ConstructQueryExportContext context1337 = wrapper.createContextForRow(1337);

  EXPECT_EQ(ConstructQueryEvaluator::evaluateVar(variable, context1337),
            std::nullopt);
}

TEST(SparqlDataTypesTest, VariableEvaluateIsPropagatedCorrectly) {
  auto wrapper = prepareContext();

  wrapper._hashMap[Variable{"?var"}] = makeAlwaysDefinedColumn(0);
  IdTable table{ad_utility::testing::makeAllocator()};
  table.setNumColumns(1);
  Id value = Id::makeFromInt(69);
  table.push_back({value});
  wrapper.setIdTable(std::move(table));

  Variable variableKnown{"?var"};
  ConstructQueryExportContext context = wrapper.createContextForRow(0);

  EXPECT_THAT(ConstructQueryEvaluator::evaluateVar(variableKnown, context),
              Optional("69"s));
  EXPECT_THAT(ConstructQueryEvaluator::evaluate(GraphTerm{variableKnown},
                                                context, SUBJECT),
              Optional("69"s));

  Variable variableUnknown{"?unknownVar"};

  EXPECT_EQ(ConstructQueryEvaluator::evaluateVar(variableUnknown, context),
            std::nullopt);
  EXPECT_EQ(ConstructQueryEvaluator::evaluate(GraphTerm{variableUnknown},
                                              context, SUBJECT),
            std::nullopt);
}
