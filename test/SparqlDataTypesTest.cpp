// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gmock/gmock.h>

#include "./util/AllocatorTestHelpers.h"
#include "parser/data/VarOrTerm.h"

using namespace std::string_literals;
using ::testing::Optional;
using enum PositionInTriple;

namespace {
struct ContextWrapper {
  Index _index{};
  ResultTable _resultTable{ad_utility::testing::makeAllocator()};
  // TODO<joka921> `VariableToColumnMap`
  ad_utility::HashMap<Variable, size_t> _hashMap{};

  ConstructQueryExportContext createContextForRow(size_t row) const {
    return {row, _resultTable, _hashMap, _index};
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
  using enum PositionInTriple;

  EXPECT_THAT(blankNodeA.evaluate(context0, SUBJECT), Optional("_:u0_a"s));
  EXPECT_THAT(blankNodeA.evaluate(context0, PREDICATE), Optional("_:u0_a"s));
  EXPECT_THAT(blankNodeA.evaluate(context0, OBJECT), Optional("_:u0_a"s));
  EXPECT_THAT(blankNodeB.evaluate(context0, SUBJECT), Optional("_:g0_b"s));
  EXPECT_THAT(blankNodeB.evaluate(context0, PREDICATE), Optional("_:g0_b"s));
  EXPECT_THAT(blankNodeB.evaluate(context0, SUBJECT), Optional("_:g0_b"s));

  ConstructQueryExportContext context10 = wrapper.createContextForRow(10);

  EXPECT_THAT(blankNodeA.evaluate(context10, SUBJECT), Optional("_:u10_a"s));
  EXPECT_THAT(blankNodeA.evaluate(context10, PREDICATE), Optional("_:u10_a"s));
  EXPECT_THAT(blankNodeA.evaluate(context10, OBJECT), Optional("_:u10_a"s));
  EXPECT_THAT(blankNodeB.evaluate(context10, SUBJECT), Optional("_:g10_b"s));
  EXPECT_THAT(blankNodeB.evaluate(context10, PREDICATE), Optional("_:g10_b"s));
  EXPECT_THAT(blankNodeB.evaluate(context10, SUBJECT), Optional("_:g10_b"s));
}

TEST(SparqlDataTypesTest, BlankNodeEvaluateIsPropagatedCorrectly) {
  auto wrapper = prepareContext();

  BlankNode blankNode{false, "label"};
  ConstructQueryExportContext context = wrapper.createContextForRow(42);

  auto expectedLabel = Optional("_:u42_label"s);

  EXPECT_THAT(blankNode.evaluate(context, SUBJECT), expectedLabel);
  EXPECT_THAT(GraphTerm{blankNode}.evaluate(context, SUBJECT), expectedLabel);
  EXPECT_THAT(VarOrTerm{GraphTerm{blankNode}}.evaluate(context, SUBJECT),
              expectedLabel);
}

TEST(SparqlDataTypesTest, IriInvalidSyntaxThrowsException) {
  EXPECT_THROW(Iri{"http://linkwithoutangularbrackets"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<<nestedangularbrackets>>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<duplicatedangularbracker>>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<<duplicatedangularbracker>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<noend"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"nostart>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<\"withdoublequote>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<{withcurlybrace>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<}withcurlybrace>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<|withpipesymbol>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<^withcaret>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<\\withbackslash>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<`withbacktick>"}, ad_utility::Exception);
  // U+0000 (NULL) to U+0020 (Space) are all forbidden characters
  // but the following two are probably the most common cases
  EXPECT_THROW(Iri{"<with whitespace>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<with\r\nnewline>"}, ad_utility::Exception);
}

TEST(SparqlDataTypesTest, IriValidIriIsPreserved) {
  ASSERT_EQ(Iri{"<http://valid-iri>"}.iri(), "<http://valid-iri>");
}

TEST(SparqlDataTypesTest, IriEvaluatesCorrectlyBasedOnContext) {
  auto wrapper = prepareContext();

  std::string iriString{"<http://some-iri>"};
  Iri iri{iriString};
  ConstructQueryExportContext context0 = wrapper.createContextForRow(0);

  EXPECT_THAT(iri.evaluate(context0, SUBJECT), Optional(iriString));
  EXPECT_THAT(iri.evaluate(context0, PREDICATE), Optional(iriString));
  EXPECT_THAT(iri.evaluate(context0, OBJECT), Optional(iriString));

  ConstructQueryExportContext context1337 = wrapper.createContextForRow(1337);

  EXPECT_THAT(iri.evaluate(context1337, SUBJECT), Optional(iriString));
  EXPECT_THAT(iri.evaluate(context1337, PREDICATE), Optional(iriString));
  EXPECT_THAT(iri.evaluate(context1337, OBJECT), Optional(iriString));
}

TEST(SparqlDataTypesTest, IriEvaluateIsPropagatedCorrectly) {
  auto wrapper = prepareContext();

  Iri iri{"<http://some-iri>"};
  ConstructQueryExportContext context = wrapper.createContextForRow(42);

  auto expectedString = Optional("<http://some-iri>"s);

  EXPECT_THAT(iri.evaluate(context, SUBJECT), expectedString);
  EXPECT_THAT(GraphTerm{iri}.evaluate(context, SUBJECT), expectedString);
  EXPECT_THAT(VarOrTerm{GraphTerm{iri}}.evaluate(context, SUBJECT),
              expectedString);
}

TEST(SparqlDataTypesTest, LiteralBooleanIsCorrectlyFormatted) {
  EXPECT_EQ(Literal{true}.literal(), "true");
  EXPECT_EQ(Literal{false}.literal(), "false");
}

TEST(SparqlDataTypesTest, LiteralStringIsCorrectlyFormatted) {
  EXPECT_EQ(Literal{"abcdef"}.literal(), "abcdef");
  EXPECT_EQ(Literal{"\U0001f937\U0001f3fc\u200d\u2642\ufe0f"}.literal(),
            "🤷🏼‍♂️");
}

TEST(SparqlDataTypesTest, LiteralNumberIsCorrectlyFormatted) {
  EXPECT_EQ(Literal{1234567890}.literal(), "1234567890");
  EXPECT_EQ(Literal{-1337}.literal(), "-1337");
  EXPECT_EQ(Literal{1.3}.literal(), "1.3");
}

TEST(SparqlDataTypesTest, LiteralEvaluatesCorrectlyBasedOnContext) {
  auto wrapper = prepareContext();

  std::string literalString{"true"};
  Literal literal{literalString};
  ConstructQueryExportContext context0 = wrapper.createContextForRow(0);

  EXPECT_EQ(literal.evaluate(context0, SUBJECT), std::nullopt);
  EXPECT_EQ(literal.evaluate(context0, PREDICATE), std::nullopt);
  EXPECT_THAT(literal.evaluate(context0, OBJECT), Optional(literalString));

  ConstructQueryExportContext context1337 = wrapper.createContextForRow(1337);

  EXPECT_EQ(literal.evaluate(context1337, SUBJECT), std::nullopt);
  EXPECT_EQ(literal.evaluate(context1337, PREDICATE), std::nullopt);
  EXPECT_THAT(literal.evaluate(context1337, OBJECT), Optional(literalString));
}

TEST(SparqlDataTypesTest, LiteralEvaluateIsPropagatedCorrectly) {
  auto wrapper = prepareContext();

  Literal literal{"some literal"};
  ConstructQueryExportContext context = wrapper.createContextForRow(42);

  EXPECT_EQ(literal.evaluate(context, SUBJECT), std::nullopt);
  EXPECT_EQ(GraphTerm{literal}.evaluate(context, SUBJECT), std::nullopt);
  EXPECT_EQ(VarOrTerm{GraphTerm{literal}}.evaluate(context, SUBJECT),
            std::nullopt);

  auto expectedString = Optional("some literal"s);

  EXPECT_THAT(literal.evaluate(context, OBJECT), expectedString);
  EXPECT_THAT(GraphTerm{literal}.evaluate(context, OBJECT), expectedString);
  EXPECT_THAT(VarOrTerm{GraphTerm{literal}}.evaluate(context, OBJECT),
              expectedString);
}

TEST(SparqlDataTypesTest, VariableNormalizesDollarSign) {
  Variable varWithQuestionMark{"?abc"};
  EXPECT_EQ(varWithQuestionMark.name(), "?abc");

  Variable varWithDollarSign{"$abc"};
  EXPECT_EQ(varWithQuestionMark.name(), "?abc");
}

TEST(SparqlDataTypesTest, VariableInvalidNamesThrowException) {
  EXPECT_THROW(Variable{"no_leading_var_or_dollar"}, ad_utility::Exception);
  EXPECT_THROW(Variable{""}, ad_utility::Exception);
  EXPECT_THROW(Variable{"? var with space"}, ad_utility::Exception);
  EXPECT_THROW(Variable{"?"}, ad_utility::Exception);
  EXPECT_THROW(Variable{"$"}, ad_utility::Exception);
}

TEST(SparqlDataTypesTest, VariableEvaluatesCorrectlyBasedOnContext) {
  auto wrapper = prepareContext();

  wrapper._hashMap[Variable{"?var"}] = 0;
  wrapper._resultTable._resultTypes.push_back(qlever::ResultType::VERBATIM);
  wrapper._resultTable._idTable.setNumColumns(1);
  Id value1 = Id::makeFromInt(69);
  Id value2 = Id::makeFromInt(420);
  wrapper._resultTable._idTable.push_back({value1});
  wrapper._resultTable._idTable.push_back({value2});

  Variable variable{"?var"};
  ConstructQueryExportContext context0 = wrapper.createContextForRow(0);

  EXPECT_THAT(variable.evaluate(context0, SUBJECT), Optional("69"s));
  EXPECT_THAT(variable.evaluate(context0, PREDICATE), Optional("69"s));
  EXPECT_THAT(variable.evaluate(context0, OBJECT), Optional("69"s));

  ConstructQueryExportContext context1 = wrapper.createContextForRow(1);

  EXPECT_THAT(variable.evaluate(context1, SUBJECT), Optional("420"s));
  EXPECT_THAT(variable.evaluate(context1, PREDICATE), Optional("420"s));
  EXPECT_THAT(variable.evaluate(context1, OBJECT), Optional("420"s));
}

TEST(SparqlDataTypesTest, VariableEvaluatesNothingForUnusedName) {
  auto wrapper = prepareContext();

  Variable variable{"?var"};
  ConstructQueryExportContext context0 = wrapper.createContextForRow(0);

  EXPECT_EQ(variable.evaluate(context0, SUBJECT), std::nullopt);
  EXPECT_EQ(variable.evaluate(context0, PREDICATE), std::nullopt);
  EXPECT_EQ(variable.evaluate(context0, OBJECT), std::nullopt);

  ConstructQueryExportContext context1337 = wrapper.createContextForRow(1337);

  EXPECT_EQ(variable.evaluate(context1337, SUBJECT), std::nullopt);
  EXPECT_EQ(variable.evaluate(context1337, PREDICATE), std::nullopt);
  EXPECT_EQ(variable.evaluate(context1337, OBJECT), std::nullopt);
}

TEST(SparqlDataTypesTest, VariableEvaluateIsPropagatedCorrectly) {
  auto wrapper = prepareContext();

  wrapper._hashMap[Variable{"?var"}] = 0;
  wrapper._resultTable._resultTypes.push_back(qlever::ResultType::VERBATIM);
  wrapper._resultTable._idTable.setNumColumns(1);
  Id value = Id::makeFromInt(69);
  wrapper._resultTable._idTable.push_back({value});

  Variable variableKnown{"?var"};
  ConstructQueryExportContext context = wrapper.createContextForRow(0);

  EXPECT_THAT(variableKnown.evaluate(context, SUBJECT), Optional("69"s));
  EXPECT_THAT(VarOrTerm{variableKnown}.evaluate(context, SUBJECT),
              Optional("69"s));

  Variable variableUnknown{"?unknownVar"};

  EXPECT_EQ(variableUnknown.evaluate(context, SUBJECT), std::nullopt);
  EXPECT_EQ(VarOrTerm{variableUnknown}.evaluate(context, SUBJECT),
            std::nullopt);
}
