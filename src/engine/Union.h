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
 public:
  Union(QueryExecutionContext* qec,
        const std::shared_ptr<QueryExecutionTree>& t1,
        const std::shared_ptr<QueryExecutionTree>& t2);

  // Create a very explicit way to create an invalid, uninitialized union. This
  // can only be used for certain unit tests of almost static member functions
  struct InvalidUnionOnlyUseForTestinTag {};
  explicit Union(InvalidUnionOnlyUseForTestinTag) {}

 protected:
  virtual string asStringImpl(size_t indent = 0) const override;

 public:
  virtual string getDescriptor() const override;

  virtual size_t getResultWidth() const override;

  virtual vector<size_t> resultSortedOn() const override;

  ad_utility::HashMap<string, size_t> getVariableColumns() const override;

  virtual void setTextLimit(size_t limit) override;

  virtual bool knownEmptyResult() override;

  virtual float getMultiplicity(size_t col) override;

  virtual size_t getSizeEstimate() override;

  virtual size_t getCostEstimate() override;

  const static size_t NO_COLUMN;

  // The method is declared here to make it unit testable
  template <int LEFT_WIDTH, int RIGHT_WIDTH, int OUT_WIDTH>
  void computeUnion(IdTable* inputTable, const IdTable& left,
                    const IdTable& right,
                    const std::vector<std::array<size_t, 2>>& columnOrigins);

  vector<QueryExecutionTree*> getChildren() override {
    return {_subtrees[0].get(), _subtrees[1].get()};
  }

 private:
  virtual void computeResult(ResultTable* result) override;

  /**
   * @brief This stores the input column from each of the two subtrees or
   * NO_COLUMN if the subtree does not have a matching column for each result
   * column.
   */
  std::vector<std::array<size_t, 2>> _columnOrigins;
  std::shared_ptr<QueryExecutionTree> _subtrees[2];
};
