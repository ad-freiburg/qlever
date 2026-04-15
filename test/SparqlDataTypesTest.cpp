// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gmock/gmock.h>

#include "./util/AllocatorTestHelpers.h"
#include "engine/ConstructBatchEvaluator.h"
#include "engine/ConstructTemplatePreprocessor.h"
#include "engine/ConstructTripleInstantiator.h"
#include "engine/ConstructTypes.h"
#include "engine/Result.h"
#include "index/Index.h"
#include "parser/data/Types.h"

using namespace std::string_literals;
using ::testing::Optional;
using enum PositionInTriple;

namespace {
struct ContextWrapper {
  Index _index{ad_utility::makeUnlimitedAllocator<Id>()};
  Result _resultTable{
      IdTable{ad_utility::testing::makeAllocator()}, {}, LocalVocab{}};
  VariableToColumnMap _hashMap{};

  void setIdTable(IdTable&& table) {
    _resultTable =
        Result{std::move(table), {}, _resultTable.getSharedLocalVocab()};
  }
};

ContextWrapper prepareContext() { return {}; }

// Evaluate a single CONSTRUCT template term.
// (preprocessTerm -> evaluateBatch -> instantiateTerm -> formatTerm).
// `row` is the IdTable row index; `rowOffset` shifts blank-node IDs.
std::optional<std::string> evaluate(const GraphTerm& term,
                                    const ContextWrapper& wrapper, size_t row,
                                    PositionInTriple position,
                                    size_t rowOffset = 0) {
  using namespace qlever::constructExport;
  auto preprocessed = ConstructTemplatePreprocessor::preprocessTerm(
      term, position, wrapper._hashMap);
  if (!preprocessed) return std::nullopt;

  BatchEvaluationResult batchResult;
  batchResult.numRows_ = 1;

  if (const auto* var = std::get_if<PrecomputedVariable>(&*preprocessed)) {
    IdCache cache{100};
    std::vector<size_t> cols{var->columnIndex_};
    BatchEvaluationContext ctx{wrapper._resultTable.idTable(), row, row + 1};
    batchResult = ConstructBatchEvaluator::evaluateBatch(
        cols, ctx, wrapper._resultTable.localVocab(), wrapper._index, cache);
  }

  auto result = instantiateTerm(*preprocessed, batchResult, 0, rowOffset + row);
  if (!result) return std::nullopt;
  return formatTerm(**result, false);
}
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
  using enum PositionInTriple;

  EXPECT_THAT(evaluate(blankNodeA, wrapper, 0, SUBJECT), Optional("_:u0_a"s));
  EXPECT_THAT(evaluate(blankNodeA, wrapper, 0, PREDICATE), Optional("_:u0_a"s));
  EXPECT_THAT(evaluate(blankNodeA, wrapper, 0, OBJECT), Optional("_:u0_a"s));
  EXPECT_THAT(evaluate(blankNodeB, wrapper, 0, SUBJECT), Optional("_:g0_b"s));
  EXPECT_THAT(evaluate(blankNodeB, wrapper, 0, PREDICATE), Optional("_:g0_b"s));
  EXPECT_THAT(evaluate(blankNodeB, wrapper, 0, OBJECT), Optional("_:g0_b"s));

  EXPECT_THAT(evaluate(blankNodeA, wrapper, 10, SUBJECT), Optional("_:u10_a"s));
  EXPECT_THAT(evaluate(blankNodeA, wrapper, 10, PREDICATE),
              Optional("_:u10_a"s));
  EXPECT_THAT(evaluate(blankNodeA, wrapper, 10, OBJECT), Optional("_:u10_a"s));
  EXPECT_THAT(evaluate(blankNodeB, wrapper, 10, SUBJECT), Optional("_:g10_b"s));
  EXPECT_THAT(evaluate(blankNodeB, wrapper, 10, PREDICATE),
              Optional("_:g10_b"s));
  EXPECT_THAT(evaluate(blankNodeB, wrapper, 10, SUBJECT), Optional("_:g10_b"s));

  // rowOffset=5, row=7 → blankNodeRowId=12
  EXPECT_THAT(evaluate(blankNodeA, wrapper, 7, SUBJECT, 5),
              Optional("_:u12_a"s));
  EXPECT_THAT(evaluate(blankNodeA, wrapper, 7, PREDICATE, 5),
              Optional("_:u12_a"s));
  EXPECT_THAT(evaluate(blankNodeA, wrapper, 7, OBJECT, 5),
              Optional("_:u12_a"s));
  EXPECT_THAT(evaluate(blankNodeB, wrapper, 7, SUBJECT, 5),
              Optional("_:g12_b"s));
  EXPECT_THAT(evaluate(blankNodeB, wrapper, 7, PREDICATE, 5),
              Optional("_:g12_b"s));
  EXPECT_THAT(evaluate(blankNodeB, wrapper, 7, SUBJECT, 5),
              Optional("_:g12_b"s));
}

TEST(SparqlDataTypesTest, BlankNodeEvaluateIsPropagatedCorrectly) {
  auto wrapper = prepareContext();

  BlankNode blankNode{false, "label"};

  auto expectedLabel = Optional("_:u42_label"s);

  EXPECT_THAT(evaluate(blankNode, wrapper, 42, SUBJECT), expectedLabel);
  EXPECT_THAT(evaluate(GraphTerm{blankNode}, wrapper, 42, SUBJECT),
              expectedLabel);
  EXPECT_THAT(evaluate(GraphTerm{blankNode}, wrapper, 42, SUBJECT),
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

  EXPECT_THAT(evaluate(iri, wrapper, 0, SUBJECT), Optional(iriString));
  EXPECT_THAT(evaluate(iri, wrapper, 0, PREDICATE), Optional(iriString));
  EXPECT_THAT(evaluate(iri, wrapper, 0, OBJECT), Optional(iriString));

  EXPECT_THAT(evaluate(iri, wrapper, 1337, SUBJECT), Optional(iriString));
  EXPECT_THAT(evaluate(iri, wrapper, 1337, PREDICATE), Optional(iriString));
  EXPECT_THAT(evaluate(iri, wrapper, 1337, OBJECT), Optional(iriString));
}

TEST(SparqlDataTypesTest, IriEvaluateIsPropagatedCorrectly) {
  auto wrapper = prepareContext();

  Iri iri{"<http://some-iri>"};

  auto expectedString = Optional("<http://some-iri>"s);

  EXPECT_THAT(evaluate(iri, wrapper, 42, SUBJECT), expectedString);
  EXPECT_THAT(evaluate(GraphTerm{iri}, wrapper, 42, SUBJECT), expectedString);
  EXPECT_THAT(evaluate(GraphTerm{iri}, wrapper, 42, SUBJECT), expectedString);
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

  EXPECT_EQ(evaluate(literal, wrapper, 0, SUBJECT), std::nullopt);
  EXPECT_EQ(evaluate(literal, wrapper, 0, PREDICATE), std::nullopt);
  EXPECT_THAT(evaluate(literal, wrapper, 0, OBJECT), Optional(literalString));

  EXPECT_EQ(evaluate(literal, wrapper, 1337, SUBJECT), std::nullopt);
  EXPECT_EQ(evaluate(literal, wrapper, 1337, PREDICATE), std::nullopt);
  EXPECT_THAT(evaluate(literal, wrapper, 1337, OBJECT),
              Optional(literalString));
}

TEST(SparqlDataTypesTest, LiteralEvaluateIsPropagatedCorrectly) {
  auto wrapper = prepareContext();

  Literal literal{"some literal"};

  EXPECT_EQ(evaluate(literal, wrapper, 42, SUBJECT), std::nullopt);
  EXPECT_EQ(evaluate(GraphTerm{literal}, wrapper, 42, SUBJECT), std::nullopt);
  EXPECT_EQ(evaluate(GraphTerm{literal}, wrapper, 42, SUBJECT), std::nullopt);

  auto expectedString = Optional("some literal"s);

  EXPECT_THAT(evaluate(literal, wrapper, 42, OBJECT), expectedString);
  EXPECT_THAT(evaluate(GraphTerm{literal}, wrapper, 42, OBJECT),
              expectedString);
  EXPECT_THAT(evaluate(GraphTerm{literal}, wrapper, 42, OBJECT),
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

  EXPECT_THAT(evaluate(variable, wrapper, 0, SUBJECT), Optional("69"s));
  EXPECT_THAT(evaluate(variable, wrapper, 0, PREDICATE), Optional("69"s));
  EXPECT_THAT(evaluate(variable, wrapper, 0, OBJECT), Optional("69"s));

  // Row offset should be ignored for variables.
  EXPECT_THAT(evaluate(variable, wrapper, 0, SUBJECT, 42), Optional("69"s));
  EXPECT_THAT(evaluate(variable, wrapper, 0, PREDICATE, 42), Optional("69"s));
  EXPECT_THAT(evaluate(variable, wrapper, 0, OBJECT, 42), Optional("69"s));

  EXPECT_THAT(evaluate(variable, wrapper, 1, SUBJECT), Optional("420"s));
  EXPECT_THAT(evaluate(variable, wrapper, 1, PREDICATE), Optional("420"s));
  EXPECT_THAT(evaluate(variable, wrapper, 1, OBJECT), Optional("420"s));
}

TEST(SparqlDataTypesTest, VariableEvaluatesNothingForUnusedName) {
  auto wrapper = prepareContext();

  Variable variable{"?var"};

  EXPECT_EQ(evaluate(variable, wrapper, 0, SUBJECT), std::nullopt);
  EXPECT_EQ(evaluate(variable, wrapper, 0, PREDICATE), std::nullopt);
  EXPECT_EQ(evaluate(variable, wrapper, 0, OBJECT), std::nullopt);

  EXPECT_EQ(evaluate(variable, wrapper, 1337, SUBJECT), std::nullopt);
  EXPECT_EQ(evaluate(variable, wrapper, 1337, PREDICATE), std::nullopt);
  EXPECT_EQ(evaluate(variable, wrapper, 1337, OBJECT), std::nullopt);
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

  EXPECT_THAT(evaluate(variableKnown, wrapper, 0, SUBJECT), Optional("69"s));
  EXPECT_THAT(evaluate(GraphTerm{variableKnown}, wrapper, 0, SUBJECT),
              Optional("69"s));

  Variable variableUnknown{"?unknownVar"};

  EXPECT_EQ(evaluate(variableUnknown, wrapper, 0, SUBJECT), std::nullopt);
  EXPECT_EQ(evaluate(GraphTerm{variableUnknown}, wrapper, 0, SUBJECT),
            std::nullopt);
}
