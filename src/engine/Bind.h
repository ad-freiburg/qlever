//
// Created by johannes on 19.04.20.
//

#ifndef QLEVER_BIND_H
#define QLEVER_BIND_H

#include "Operation.h"
#include "../parser/ParsedQuery.h"

class Bind : public Operation {
 public:
  Bind(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> subtree, GraphPatternOperation::Bind b) :
  Operation(qec), _subtree(std::move(subtree)), _bind(std::move(b)){
    AD_CHECK(!std::holds_alternative<GraphPatternOperation::Bind::Constant>(_bind._input));
  }

  // Get a unique, not ambiguous string representation for a subtree.
  // This should possible act like an ID for each subtree.
  [[nodiscard]] string asString(size_t indent) const override;

  // Gets a very short (one line without line ending) descriptor string for
  // this Operation.  This string is used in the RuntimeInformation
  [[nodiscard]] string getDescriptor() const override;
  [[nodiscard]] size_t getResultWidth() const override;
  std::vector<QueryExecutionTree*> getChildren() override;

  void setTextLimit(size_t limit) override;
  size_t getCostEstimate() override;
  size_t getSizeEstimate() override;
  float getMultiplicity(size_t col) override;
  bool knownEmptyResult() override;
  [[nodiscard]] ad_utility::HashMap<string, size_t> getVariableColumns() const override;

 protected:
  [[nodiscard]] vector<size_t> resultSortedOn() const override ;

 private:
  std::shared_ptr<QueryExecutionTree> _subtree;
  void computeResult(ResultTable* result) override ;
  GraphPatternOperation::Bind _bind;

  template <int IN_WIDTH, int OUT_WIDTH>
  static void computeBind(IdTable* dynRes, const IdTable& inputDyn,
                   std::array<size_t, 2> columns, array<ResultTable::ResultType, 2> inputType,
                          const Index& index);
};

#endif  // QLEVER_BIND_H
