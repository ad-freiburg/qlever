//
// Created by johannes on 19.04.20.
//

#ifndef QLEVER_BIND_H
#define QLEVER_BIND_H

#include "engine/Operation.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "parser/ParsedQuery.h"

/// BIND operation, currently only supports a very limited subset of expressions
class Bind : public Operation {
 public:
  Bind(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> subtree,
       parsedQuery::Bind b)
      : Operation(qec), _subtree(std::move(subtree)), _bind(std::move(b)) {}

 private:
  std::shared_ptr<QueryExecutionTree> _subtree;
  parsedQuery::Bind _bind;
  // For the documentation of the overridden members, see Operation.h
 protected:
  [[nodiscard]] string asStringImpl(size_t indent) const override;

 public:
  [[nodiscard]] string getDescriptor() const override;
  [[nodiscard]] size_t getResultWidth() const override;
  std::vector<QueryExecutionTree*> getChildren() override;
  void setTextLimit(size_t limit) override;
  size_t getCostEstimate() override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  float getMultiplicity(size_t col) override;
  bool knownEmptyResult() override;

  // Returns the variable to which the expression will be bound
  [[nodiscard]] const string& targetVariable() const {
    return _bind._target.name();
  }

 protected:
  [[nodiscard]] vector<ColumnIndex> resultSortedOn() const override;

 private:
  ResultTable computeResult() override;

  // Implementation for the binding of arbitrary expressions.
  template <size_t IN_WIDTH, size_t OUT_WIDTH>
  void computeExpressionBind(
      IdTable* outputIdTable, LocalVocab* outputLocalVocab,
      const ResultTable& inputResultTable,
      sparqlExpression::SparqlExpression* expression) const;

  [[nodiscard]] VariableToColumnMap computeVariableToColumnMap() const override;
};

#endif  // QLEVER_BIND_H
