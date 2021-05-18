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
  std::vector<double> d{1.0, 2.0, 0.0, 0.0};
  std::vector<bool> b{false, false, true, false};
  std::vector<bool> expected{true, true, true, false};

  QueryExecutionContext* ctxt = nullptr ;
  sparqlExpression::SparqlExpression::VariableColumnMap map;
  ad_utility::AllocatorWithLimit<Id> alloc {ad_utility::makeAllocationMemoryLeftThreadsafeObject(1000)};
  IdTable table{alloc};
  sparqlExpression::SparqlExpression::EvaluationInput input{*ctxt, map, table};
  std::vector<SparqlExpression::Ptr> children;
  children.push_back(
      std::make_unique<DummyExpression>(SparqlExpression::EvaluateResult{d}));
  children.push_back(
      std::make_unique<DummyExpression>(SparqlExpression::EvaluateResult{b}));

  ConditionalOrExpression c{std::move(children)};

  SparqlExpression::EvaluateResult res = c.evaluate(&input);
  auto ptr = std::get_if<std::vector<bool>>(&res);
  ASSERT_TRUE(ptr);
  ASSERT_EQ(*ptr, expected);
}