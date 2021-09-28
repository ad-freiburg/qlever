//
// Created by johannes on 12.05.21.
//

#include <gtest/gtest.h>

#include "../src/parser/SparqlExpression.h"

using namespace sparqlExpression;

struct DummyExpression : public SparqlExpression {
  DummyExpression(ExpressionResult result) : _result{std::move(result)} {}
  mutable ExpressionResult _result;
  ExpressionResult evaluate(EvaluationContext*) const override {
    return std::move(_result);
  }
  vector<std::string> getUnaggregatedVariables() override { return {}; }
  string getCacheKey(
      [[maybe_unused]] const VariableToColumnMap& varColMap) const override {
    return "DummyDummyDummDumm"s;
  }

  std::span<SparqlExpression::Ptr> children() override { return {}; }
};

template <typename BinaryExpression = ConditionalOrExpression>
auto testBinary = [](const ExpressionResult& expected, auto&&... operands) {
  QueryExecutionContext* ctxt = nullptr;
  ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(1000)};
  sparqlExpression::VariableToColumnAndResultTypeMap map;
  ResultTable::LocalVocab localVocab;
  IdTable table{alloc};
  sparqlExpression::EvaluationContext context{*ctxt, map, table, alloc,
                                              localVocab};
  std::vector<SparqlExpression::Ptr> children;
  (..., children.push_back(std::make_unique<DummyExpression>(
            ExpressionResult{std::move(operands)})));

  auto c = BinaryExpression::create(std::move(children));

  ExpressionResult res = c->evaluate(&context);
  ASSERT_EQ(res, expected);
};

auto testOr = testBinary<ConditionalOrExpression>;
auto testAnd = testBinary<ConditionalAndExpression>;

template <typename T>
using LV = VectorWithMemoryLimit<T>;
TEST(SparqlExpression, OrAnd) {
  ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(1000)};
  LV<double> d{{1.0, 2.0, 0.0, 0.0}, alloc};
  LV<bool> b{{false, true, true, false}, alloc};
  LV<bool> bOrD{{true, true, true, false}, alloc};
  LV<bool> bAndD{{false, true, false, false}, alloc};

  LV<bool> allTrue{{true, true, true, true}, alloc};
  LV<bool> allFalse{{false, false, false, false}, alloc};

  testOr(b.clone(), b.clone());
  testOr(d.clone(), d.clone());
  testAnd(b.clone(), b.clone());
  testAnd(d.clone(), d.clone());

  testOr(bOrD.clone(), d.clone(), b.clone());
  testOr(bOrD.clone(), d.clone(), b.clone());
  testOr(bOrD.clone(), allFalse.clone(), allFalse.clone(), b.clone(),
         allFalse.clone(), d.clone());
  testOr(allTrue.clone(), allFalse.clone(), allTrue.clone(), b.clone(),
         allFalse.clone(), d.clone());

  testAnd(bAndD.clone(), d.clone(), b.clone());
  testAnd(bAndD.clone(), d.clone(), b.clone());
  testAnd(bAndD.clone(), allTrue.clone(), allTrue.clone(), b.clone(),
          allTrue.clone(), d.clone());
  testAnd(allFalse.clone(), allTrue.clone(), allTrue.clone(), b.clone(),
          allFalse.clone(), d.clone());
  testAnd(allTrue.clone(), allTrue.clone());
}