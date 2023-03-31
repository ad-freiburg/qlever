//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/Operation.h"
#include "engine/QueryExecutionContext.h"
#include "engine/ResultTable.h"
#include "util/Random.h"

// An operation that yields a given `IdTable` as its result. It is used for
// unit testing purposes when we need to specify the subtrees of another
// operation.
class ValuesForTesting : public Operation {
 private:
  IdTable table_;
  std::vector<Variable> variables_;

 public:
  // Create an operation that has as its result the given `table` and the given
  // `variables`. The number of variables must be equal to the number
  // of columns in the table.
  ValuesForTesting(QueryExecutionContext* ctx, IdTable table,
                   std::vector<Variable> variables)
      : Operation{ctx},
        table_{std::move(table)},
        variables_{std::move(variables)} {
    AD_CONTRACT_CHECK(variables_.size() == table_.numColumns());
  }

  // ___________________________________________________________________________
  ResultTable computeResult() override {
    return {table_.clone(), resultSortedOn(), LocalVocab{}};
  }

 private:
  // ___________________________________________________________________________
  string asStringImpl([[maybe_unused]] size_t indent) const override {
    std::stringstream str;
    str << "Values for testing with " << table_.numColumns()
        << " columns and contents ";
    for (size_t i = 0; i < table_.numColumns(); ++i) {
      for (Id entry : table_.getColumn(i)) {
        str << entry << ' ';
      }
    }
    return std::move(str).str();
  }

 public:
  string getDescriptor() const override {
    return "explicit values for testing";
  }

  size_t getResultWidth() const override { return table_.numColumns(); }

  // TODO<joka921> Maybe we will need to store sorted tables for future unit
  // tests.
  vector<size_t> resultSortedOn() const override { return {}; }

  void setTextLimit(size_t limit) override { (void)limit; }

  size_t getCostEstimate() override { return table_.numRows(); }

  size_t getSizeEstimate() override { return table_.numRows(); }

  // For unit testing purposes it is useful that the columns have different
  // multiplicities to find bugs in functions that use the multiplicity.
  float getMultiplicity(size_t col) override {
    (void)col;
    return static_cast<float>(col + 1) * 42.0f;
  }

  vector<QueryExecutionTree*> getChildren() override { return {}; }

  bool knownEmptyResult() override { return table_.empty(); }

 private:
  VariableToColumnMap computeVariableToColumnMap() const override {
    VariableToColumnMap m;
    for (size_t i = 0; i < variables_.size(); ++i) {
      bool containsUndef =
          ad_utility::contains(table_.getColumn(i), Id::makeUndefined());
      using enum ColumnIndexAndTypeInfo::UndefStatus;
      m[variables_.at(i)] = ColumnIndexAndTypeInfo{
          i, containsUndef ? PossiblyUndefined : AlwaysDefined};
    }
    return m;
  }
};
