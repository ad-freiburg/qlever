//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "engine/sparqlExpressions/GroupConcatExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"

namespace {
using namespace sparqlExpression;
namespace tc = ad_utility::triple_component;

// _____________________________________________________________________________
void expectIdsAreConcatenatedTo(bool distinct, const IdTable& idTable,
                                const ExpressionResult& expected,
                                ad_utility::source_location location =
                                    ad_utility::source_location::current()) {
  AD_CONTRACT_CHECK(idTable.numColumns() == 1);
  auto* qec = ad_utility::testing::getQec();
  auto g = generateLocationTrace(location);

  Variable var{"?x"};
  LocalVocab localVocab;
  ad_utility::SharedCancellationHandle cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  VariableToColumnMap map{
      {var, {0, ColumnIndexAndTypeInfo::PossiblyUndefined}}};

  EvaluationContext context{
      *qec,
      map,
      idTable,
      idTable.getAllocator(),
      localVocab,
      std::move(cancellationHandle),
      std::chrono::time_point<std::chrono::steady_clock>::max()};

  GroupConcatExpression expression{
      distinct, std::make_unique<VariableExpression>(var), ";"};

  auto result = expression.evaluate(&context);
  EXPECT_EQ(result, expected);
}

// _____________________________________________________________________________
void expectLiteralsAreConcatenatedTo(
    bool distinct, const std::vector<tc::Literal>& literals,
    const ad_utility::triple_component::Literal& literal,
    ad_utility::source_location location =
        ad_utility::source_location::current()) {
  LocalVocab localVocab;
  IdTable input{1, ad_utility::makeUnlimitedAllocator<Id>()};

  for (const auto& inputLiteral : literals) {
    auto idx =
        localVocab.getIndexAndAddIfNotContained(LocalVocabEntry{inputLiteral});
    input.push_back({Id::makeFromLocalVocabIndex(idx)});
  }
  expectIdsAreConcatenatedTo(
      distinct, input, IdOrLiteralOrIri{LocalVocabEntry{literal}}, location);
}

auto lit = [](std::string s) {
  return tc::Literal::fromStringRepresentation(std::move(s));
};
}  // namespace

// _____________________________________________________________________________
TEST(GroupConcatExpression, basicConcatenation) {
  expectLiteralsAreConcatenatedTo(false, {}, lit("\"\""));
  expectLiteralsAreConcatenatedTo(true, {}, lit("\"\""));
  expectLiteralsAreConcatenatedTo(false, {lit("\"\"")}, lit("\"\""));
  expectLiteralsAreConcatenatedTo(false, {lit("\"a\"")}, lit("\"a\""));
  expectLiteralsAreConcatenatedTo(true, {lit("\"a\"")}, lit("\"a\""));
  expectLiteralsAreConcatenatedTo(true, {lit("\"a\""), lit("\"a\"")},
                                  lit("\"a\""));
  expectLiteralsAreConcatenatedTo(false, {lit("\"a\""), lit("\"b\"")},
                                  lit("\"a;b\""));
  expectLiteralsAreConcatenatedTo(
      true, {lit("\"a\""), lit("\"a\""), lit("\"b\"")}, lit("\"a;b\""));

  expectLiteralsAreConcatenatedTo(
      false, {lit("\"a\""), lit("\"b\""), lit("\"\"")}, lit("\"a;b;\""));
  expectLiteralsAreConcatenatedTo(
      false, {lit("\"a\""), lit("\"b\""), lit("\"\""), lit("\"\"")},
      lit("\"a;b;;\""));
  expectLiteralsAreConcatenatedTo(
      false,
      {lit("\"a\""), lit("\"b\""), lit("\"\""), lit("\"\""), lit("\"c\"")},
      lit("\"a;b;;;c\""));
}

// _____________________________________________________________________________
TEST(GroupConcatExpression, concatenationWithUndefined) {
  LocalVocab localVocab;
  expectIdsAreConcatenatedTo(false,
                             makeIdTableFromVector({{Id::makeUndefined()}}),
                             ExpressionResult{Id::makeUndefined()});

  auto idx = localVocab.getIndexAndAddIfNotContained(
      LocalVocabEntry::fromStringRepresentation("\"a\""));
  auto a = Id::makeFromLocalVocabIndex(idx);
  expectIdsAreConcatenatedTo(
      false, makeIdTableFromVector({{Id::makeUndefined()}, {a}}),
      ExpressionResult{Id::makeUndefined()});
  expectIdsAreConcatenatedTo(
      false, makeIdTableFromVector({{a}, {Id::makeUndefined()}}),
      ExpressionResult{Id::makeUndefined()});
}

// _____________________________________________________________________________
TEST(GroupConcatExpression, concatenationWithLanguageTags) {
  expectLiteralsAreConcatenatedTo(false, {lit("\"a\"@en")}, lit("\"a\"@en"));
  expectLiteralsAreConcatenatedTo(true, {lit("\"a\"@en")}, lit("\"a\"@en"));
  expectLiteralsAreConcatenatedTo(true, {lit("\"a\"@en"), lit("\"a\"@en")},
                                  lit("\"a\"@en"));
  expectLiteralsAreConcatenatedTo(false, {lit("\"a\"@en"), lit("\"b\"@en")},
                                  lit("\"a;b\"@en"));
  expectLiteralsAreConcatenatedTo(false, {lit("\"a\""), lit("\"b\"@en")},
                                  lit("\"a;b\""));
  expectLiteralsAreConcatenatedTo(true, {lit("\"a\""), lit("\"a\"@en")},
                                  lit("\"a;a\""));
  expectLiteralsAreConcatenatedTo(false, {lit("\"a\"@en"), lit("\"b\"")},
                                  lit("\"a;b\""));
  expectLiteralsAreConcatenatedTo(false, {lit("\"a\"@en"), lit("\"b\"@de")},
                                  lit("\"a;b\""));
  expectLiteralsAreConcatenatedTo(true, {lit("\"a\"@en"), lit("\"a\"@de")},
                                  lit("\"a;a\""));
}

// _____________________________________________________________________________
TEST(GroupConcatExpression, getCacheKey) {
  Variable var{"?x"};
  VariableToColumnMap map{
      {var, {0, ColumnIndexAndTypeInfo::PossiblyUndefined}}};

  std::string sep = "👻";

  GroupConcatExpression expressionNonDistinct{
      false, std::make_unique<VariableExpression>(var), sep};
  EXPECT_THAT(expressionNonDistinct.getCacheKey(map),
              ::testing::HasSubstr(sep));

  GroupConcatExpression expressionDistinct{
      true, std::make_unique<VariableExpression>(var), sep};
  EXPECT_THAT(expressionDistinct.getCacheKey(map), ::testing::HasSubstr(sep));

  EXPECT_NE(expressionNonDistinct.getCacheKey(map),
            expressionDistinct.getCacheKey(map));
}
