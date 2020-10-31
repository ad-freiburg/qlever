//
// Created by johannes on 29.10.20.
//

#ifndef QLEVER_EXPRESSION_H
#define QLEVER_EXPRESSION_H

#include "../../../global/Id.h"
#include <memory>
#include <vector>

#include "../../../engine/ResultTable.h"
#include "../../../util/HashMap.h"
#include "../../../engine/QueryExecutionContext.h"

struct Input {
  ad_utility::HashMap<std::string, size_t>*  variableColumnMap_;
  ResultTable* input_;
  QueryExecutionContext* qec_;
  bool requireNumericResult;
};

class Expression {
 public:
  virtual std::vector<FancyId> evaluate(Input) const = 0;


};

class AddExpression : public Expression {
 public:
  std::vector<FancyId> evaluate(Input inp) const override {
    auto  a = a_->evaluate(inp);
    auto  b = b_->evaluate(inp);
    AD_CHECK(a.size() == b.size());
    for (size_t i = 0; i < a.size(); ++i) {
      a[i] = a[i] * b[i];
    }

    return a;
  }
 private:
  std::unique_ptr<Expression> a_;
  std::unique_ptr<Expression> b_;
};

class VariableExpression : public Expression {
 public:
  std::vector<FancyId> evaluate(Input inp) const override {
    if (!inp.variableColumnMap_->contains(variable_)) {
      throw std::runtime_error("Variable " + variable_ + " could not be mapped to column. Please report this");
    }

    auto col = (*inp.variableColumnMap_)[variable_];
    std::vector<FancyId> res;
    res.reserve(inp.input_->size());
    const auto& d = inp.input_->_data;
    AD_CHECK(col < d.cols());

    switch (inp.input_->_resultTypes[col]) {
      case ResultTable::ResultType::KB:
        if (inp.requireNumericResult) {
          for (size_t i = 0; i < d.size(); ++i) {
            res.push_back(inp.qec_->getIndex()
                              .idToNumericValue(d(i, col).getUnsigned())
                              .value_or(ID_NO_VALUE));
          }
        } else {
          AD_CHECK(false);
        }
      default:
       throw std::runtime_error("Expression evaluation is currently only supported for KnowledgeBase columns") ;
    }

    return res;
  }

 private:
  std::string variable_; // the variable's name
};

#endif  // QLEVER_EXPRESSION_H
