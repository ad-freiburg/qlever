//
// Created by johannes on 12.05.21.
//

#include <gtest/gtest.h>

#include "../src/parser/SparqlExpression.h"

struct DummyExpression : public SparqlExpression {
  DummyExpression(EvaluateResult result) : _result{std::move(result)} {}
  EvaluateResult _result;
  EvaluateResult evaluate(evaluationInput *) const override {
    return _result;
  }
};

TEST(SparqlExpression, Or) {
  std::vector<double> d {1.0, 2.0, 0.0, 0.0};
  std::vector<bool> b {false, false, true, false};
  std::vector<bool> expected {true, true, true, false};

  evaluationInput i;
  i._inputSize = d.size();;
  std::vector<SparqlExpression::Ptr> children;
  children.push_back(std::make_unique<DummyExpression>(SparqlExpression::EvaluateResult{d}));
  children.push_back(std::make_unique<DummyExpression>(SparqlExpression::EvaluateResult{b}));

  ConditionalOrExpression c{std::move(children)};

  SparqlExpression::EvaluateResult res = c.evaluate(&i);
  auto ptr = std::get_if<std::vector<bool>>(&res);
  ASSERT_TRUE(ptr);
  ASSERT_EQ(*ptr, expected);


}