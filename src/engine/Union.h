// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
#pragma once

#include <array>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../parser/ParsedQuery.h"
#include "../util/HashMap.h"
#include "Operation.h"
#include "QueryExecutionTree.h"

class Union : public Operation {
 private:
  /**
   * @brief This stores the input column from each of the two subtrees or
   * NO_COLUMN if the subtree does not have a matching column for each result
   * column.
   */
  std::vector<std::array<size_t, 2>> _columnOrigins;
  std::array<std::shared_ptr<QueryExecutionTree>, 2> _subtrees;

 public:
  Union(QueryExecutionContext* qec,
        const std::shared_ptr<QueryExecutionTree>& t1,
        const std::shared_ptr<QueryExecutionTree>& t2);

 protected:
  virtual string asStringImpl(size_t indent = 0) const override;

 public:
  virtual string getDescriptor() const override;

  virtual size_t getResultWidth() const override;

  virtual vector<ColumnIndex> resultSortedOn() const override;

  virtual void setTextLimit(size_t limit) override;

  virtual bool knownEmptyResult() override;

  virtual float getMultiplicity(size_t col) override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  virtual size_t getCostEstimate() override;

  const static size_t NO_COLUMN;

  // The method is declared here to make it unit testable
  void computeUnion(IdTable* inputTable, const IdTable& left,
                    const IdTable& right,
                    const std::vector<std::array<size_t, 2>>& columnOrigins);

  vector<QueryExecutionTree*> getChildren() override {
    return {_subtrees[0].get(), _subtrees[1].get()};
  }

 private:
  virtual ResultTable computeResult() override;

  VariableToColumnMap computeVariableToColumnMap() const override;
};
