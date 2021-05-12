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
  DummyExpression dummy1 {SparqlExpression::EvaluateResult{d}};
  DummyExpression dummy2 {SparqlExpression::EvaluateResult{b}};


}