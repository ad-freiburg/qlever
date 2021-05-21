//
// Created by johannes on 12.05.21.
//

#include <gtest/gtest.h>

#include "../src/parser/SparqlExpression.h"

using namespace sparqlExpression;

struct DummyExpression : public SparqlExpression {
  DummyExpression(EvaluateResult result) : _result{std::move(result)} {}
  EvaluateResult _result;
  EvaluateResult evaluate(EvaluationInput*) const override { return _result; }
  vector<std::string*> strings() override { return {}; }
};

TEST(SparqlExpression, Or) {
  ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(1000)};
  sparqlExpression::LimitedVector<double> d{{1.0, 2.0, 0.0, 0.0}, alloc};
  sparqlExpression::LimitedVector<bool> b{{false, false, true, false}, alloc};
  sparqlExpression::LimitedVector<bool> expected{{true, true, true, false}, alloc};

  QueryExecutionContext* ctxt = nullptr;
  sparqlExpression::SparqlExpression::VariableColumnMap map;
  IdTable table{alloc};
  sparqlExpression::SparqlExpression::EvaluationInput input{*ctxt, map, table, alloc};
  std::vector<SparqlExpression::Ptr> children;
  children.push_back(
      std::make_unique<DummyExpression>(SparqlExpression::EvaluateResult{d}));
  children.push_back(
      std::make_unique<DummyExpression>(SparqlExpression::EvaluateResult{b}));

  ConditionalOrExpression c{std::move(children)};

  SparqlExpression::EvaluateResult res = c.evaluate(&input);
  auto ptr = std::get_if<sparqlExpression::LimitedVector<bool>>(&res);
  ASSERT_TRUE(ptr);
  ASSERT_EQ(*ptr, expected);
}