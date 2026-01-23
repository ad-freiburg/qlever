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

namespace {
constexpr auto evaluate = [](auto&&... args) {
  return ConstructQueryEvaluator::evaluateTerm(AD_FWD(args)...);
};
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

  EXPECT_THAT(evaluate(blankNodeA, context0, SUBJECT), Optional("_:u0_a"s));
  EXPECT_THAT(evaluate(blankNodeA, context0, PREDICATE), Optional("_:u0_a"s));
  EXPECT_THAT(evaluate(blankNodeA, context0, OBJECT), Optional("_:u0_a"s));
  EXPECT_THAT(evaluate(blankNodeB, context0, SUBJECT), Optional("_:g0_b"s));
  EXPECT_THAT(evaluate(blankNodeB, context0, PREDICATE), Optional("_:g0_b"s));
  EXPECT_THAT(evaluate(blankNodeB, context0, OBJECT), Optional("_:g0_b"s));

  ConstructQueryExportContext context10 = wrapper.createContextForRow(10);

  EXPECT_THAT(evaluate(blankNodeA, context10, SUBJECT), Optional("_:u10_a"s));
  EXPECT_THAT(evaluate(blankNodeA, context10, PREDICATE), Optional("_:u10_a"s));
  EXPECT_THAT(evaluate(blankNodeA, context10, OBJECT), Optional("_:u10_a"s));
  EXPECT_THAT(evaluate(blankNodeB, context10, SUBJECT), Optional("_:g10_b"s));
  EXPECT_THAT(evaluate(blankNodeB, context10, PREDICATE), Optional("_:g10_b"s));
  EXPECT_THAT(evaluate(blankNodeB, context10, SUBJECT), Optional("_:g10_b"s));

  ConstructQueryExportContext context12 = wrapper.createContextForRow(7, 5);

  EXPECT_THAT(evaluate(blankNodeA, context12, SUBJECT), Optional("_:u12_a"s));
  EXPECT_THAT(evaluate(blankNodeA, context12, PREDICATE), Optional("_:u12_a"s));
  EXPECT_THAT(evaluate(blankNodeA, context12, OBJECT), Optional("_:u12_a"s));
  EXPECT_THAT(evaluate(blankNodeB, context12, SUBJECT), Optional("_:g12_b"s));
  EXPECT_THAT(evaluate(blankNodeB, context12, PREDICATE), Optional("_:g12_b"s));
  EXPECT_THAT(evaluate(blankNodeB, context12, SUBJECT), Optional("_:g12_b"s));
}

TEST(SparqlDataTypesTest, BlankNodeEvaluateIsPropagatedCorrectly) {
  auto wrapper = prepareContext();

  BlankNode blankNode{false, "label"};
  ConstructQueryExportContext context = wrapper.createContextForRow(42);

  auto expectedLabel = Optional("_:u42_label"s);

  EXPECT_THAT(evaluate(blankNode, context, SUBJECT), expectedLabel);
  EXPECT_THAT(evaluate(GraphTerm{blankNode}, context, SUBJECT), expectedLabel);
  EXPECT_THAT(evaluate(GraphTerm{blankNode}, context, SUBJECT), expectedLabel);
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

  EXPECT_THAT(evaluate(iri, context0, SUBJECT), Optional(iriString));
  EXPECT_THAT(evaluate(iri, context0, PREDICATE), Optional(iriString));
  EXPECT_THAT(evaluate(iri, context0, OBJECT), Optional(iriString));

  ConstructQueryExportContext context1337 = wrapper.createContextForRow(1337);

  EXPECT_THAT(evaluate(iri, context1337, SUBJECT), Optional(iriString));
  EXPECT_THAT(evaluate(iri, context1337, PREDICATE), Optional(iriString));
  EXPECT_THAT(evaluate(iri, context1337, OBJECT), Optional(iriString));
}

TEST(SparqlDataTypesTest, IriEvaluateIsPropagatedCorrectly) {
  auto wrapper = prepareContext();

  Iri iri{"<http://some-iri>"};
  ConstructQueryExportContext context = wrapper.createContextForRow(42);

  auto expectedString = Optional("<http://some-iri>"s);

  EXPECT_THAT(evaluate(iri, context, SUBJECT), expectedString);
  EXPECT_THAT(evaluate(GraphTerm{iri}, context, SUBJECT), expectedString);
  EXPECT_THAT(evaluate(GraphTerm{iri}, context, SUBJECT), expectedString);
}

TEST(SparqlDataTypesTest, LiteralBooleanIsCorrectlyFormatted) {
  EXPECT_EQ(Literal{"true"}.literal(), "true");
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

  EXPECT_EQ(evaluate(literal, context0, SUBJECT), std::nullopt);
  EXPECT_EQ(evaluate(literal, context0, PREDICATE), std::nullopt);
  EXPECT_THAT(evaluate(literal, context0, OBJECT), Optional(literalString));

  ConstructQueryExportContext context1337 = wrapper.createContextForRow(1337);

  EXPECT_EQ(evaluate(literal, context1337, SUBJECT), std::nullopt);
  EXPECT_EQ(evaluate(literal, context1337, PREDICATE), std::nullopt);
  EXPECT_THAT(evaluate(literal, context1337, OBJECT), Optional(literalString));
}

TEST(SparqlDataTypesTest, LiteralEvaluateIsPropagatedCorrectly) {
  auto wrapper = prepareContext();

  Literal literal{"some literal"};
  ConstructQueryExportContext context = wrapper.createContextForRow(42);

  EXPECT_EQ(evaluate(literal, context, SUBJECT), std::nullopt);
  EXPECT_EQ(evaluate(GraphTerm{literal}, context, SUBJECT), std::nullopt);
  EXPECT_EQ(evaluate(GraphTerm{literal}, context, SUBJECT), std::nullopt);

  auto expectedString = Optional("some literal"s);

  EXPECT_THAT(evaluate(literal, context, OBJECT), expectedString);
  EXPECT_THAT(evaluate(GraphTerm{literal}, context, OBJECT), expectedString);
  EXPECT_THAT(evaluate(GraphTerm{literal}, context, OBJECT), expectedString);
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

  EXPECT_THAT(evaluate(variable, context0, SUBJECT), Optional("69"s));
  EXPECT_THAT(evaluate(variable, context0, PREDICATE), Optional("69"s));
  EXPECT_THAT(evaluate(variable, context0, OBJECT), Optional("69"s));

  // Row offset should be ignored.
  ConstructQueryExportContext context0b = wrapper.createContextForRow(0, 42);

  EXPECT_THAT(evaluate(variable, context0b, SUBJECT), Optional("69"s));
  EXPECT_THAT(evaluate(variable, context0b, PREDICATE), Optional("69"s));
  EXPECT_THAT(evaluate(variable, context0b, OBJECT), Optional("69"s));

  ConstructQueryExportContext context1 = wrapper.createContextForRow(1);

  EXPECT_THAT(evaluate(variable, context1, SUBJECT), Optional("420"s));
  EXPECT_THAT(evaluate(variable, context1, PREDICATE), Optional("420"s));
  EXPECT_THAT(evaluate(variable, context1, OBJECT), Optional("420"s));
}

TEST(SparqlDataTypesTest, VariableEvaluatesNothingForUnusedName) {
  auto wrapper = prepareContext();

  Variable variable{"?var"};
  ConstructQueryExportContext context0 = wrapper.createContextForRow(0);

  EXPECT_EQ(evaluate(variable, context0, SUBJECT), std::nullopt);
  EXPECT_EQ(evaluate(variable, context0, PREDICATE), std::nullopt);
  EXPECT_EQ(evaluate(variable, context0, OBJECT), std::nullopt);

  ConstructQueryExportContext context1337 = wrapper.createContextForRow(1337);

  EXPECT_EQ(evaluate(variable, context1337, SUBJECT), std::nullopt);
  EXPECT_EQ(evaluate(variable, context1337, PREDICATE), std::nullopt);
  EXPECT_EQ(evaluate(variable, context1337, OBJECT), std::nullopt);
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

  EXPECT_THAT(evaluate(variableKnown, context, SUBJECT), Optional("69"s));
  EXPECT_THAT(evaluate(GraphTerm{variableKnown}, context, SUBJECT),
              Optional("69"s));

  Variable variableUnknown{"?unknownVar"};

  EXPECT_EQ(evaluate(variableUnknown, context, SUBJECT), std::nullopt);
  EXPECT_EQ(evaluate(GraphTerm{variableUnknown}, context, SUBJECT),
            std::nullopt);
}
