//
// Created by johannes on 29.10.20.
//

#ifndef QLEVER_EXPRESSIONTREE_H
#define QLEVER_EXPRESSIONTREE_H

#include <memory>
#include <vector>

#include "../../global/Id.h"
#include "../../util/HashMap.h"
#include "../QueryExecutionContext.h"
#include "../ResultTable.h"


class ExpressionTree {
 public:

  using Ptr = std::unique_ptr<ExpressionTree>;

  struct Input {
    const ad_utility::HashMap<std::string, size_t>*  variableColumnMap_;
    ResultTable* input_;
    QueryExecutionContext* qec_;
    bool requireNumericResult;
  };
  virtual std::vector<FancyId> evaluate(Input) const = 0;


};

class AddExpression : public ExpressionTree {
 public:

  AddExpression(Ptr a, Ptr b) : a_(std::move(a)), b_(std::move(b)) {}

  std::vector<FancyId> evaluate(ExpressionTree::Input inp) const override {
    auto  a = a_->evaluate(inp);
    auto  b = b_->evaluate(inp);
    AD_CHECK(a.size() == b.size());
    for (size_t i = 0; i < a.size(); ++i) {
      a[i] = a[i] + b[i];
    }

    return a;
  }
 private:
  std::unique_ptr<ExpressionTree> a_;
  std::unique_ptr<ExpressionTree> b_;
};

class MultiplyExpression : public ExpressionTree {
 public:
  MultiplyExpression(Ptr a, Ptr b) : a_(std::move(a)), b_(std::move(b)) {}
  std::vector<FancyId> evaluate(ExpressionTree::Input inp) const override {
    auto  a = a_->evaluate(inp);
    auto  b = b_->evaluate(inp);
    AD_CHECK(a.size() == b.size());
    for (size_t i = 0; i < a.size(); ++i) {
      a[i] = a[i] * b[i];
    }

    return a;
  }
  const ExpressionTree * a() const {return a_.get();}
  const ExpressionTree * b() const {return b_.get();}
 private:
  std::unique_ptr<ExpressionTree> a_;
  std::unique_ptr<ExpressionTree> b_;
};
class DivideExpression : public ExpressionTree {
 public:
  DivideExpression(Ptr a, Ptr b) : a_(std::move(a)), b_(std::move(b)) {}
  std::vector<FancyId> evaluate(ExpressionTree::Input inp) const override {
    auto  a = a_->evaluate(inp);
    auto  b = b_->evaluate(inp);
    AD_CHECK(a.size() == b.size());
    for (size_t i = 0; i < a.size(); ++i) {
      a[i] = a[i] / b[i];
    }

    return a;
  }
 private:
  std::unique_ptr<ExpressionTree> a_;
  std::unique_ptr<ExpressionTree> b_;
};

class VariableExpression : public ExpressionTree {
 public:
  VariableExpression(std::string variable) : variable_(std::move(variable)) {}
  std::vector<FancyId> evaluate(Input inp) const override {
    if (!inp.variableColumnMap_->contains(variable_)) {
      throw std::runtime_error("Variable " + variable_ + " could not be mapped to column. Please report this");
    }

    auto col = (*inp.variableColumnMap_).at(variable_);
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
  const std::string& variable() const {return variable_;}


 private:
  std::string variable_; // the variable's name
};

#endif  // QLEVER_EXPRESSIONTREE_H
