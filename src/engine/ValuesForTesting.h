//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/Operation.h"
#include "engine/QueryExecutionContext.h"
#include "engine/ResultTable.h"
#include "util/Algorithm.h"
#include "util/Random.h"

// An operation that yields a given `IdTable` as its result. It is used for
// unit testing purposes when we need to specify the subtrees of another
// operation.
class ValuesForTesting : public Operation {
 private:
  IdTable table_;
  std::vector<std::optional<Variable>> variables_;
  bool supportsLimit_;
  // Those can be manually overwritten for testing using the respective getters.
  size_t sizeEstimate_;
  size_t costEstimate_;

 public:
  // Create an operation that has as its result the given `table` and the given
  // `variables`. The number of variables must be equal to the number
  // of columns in the table.
  explicit ValuesForTesting(QueryExecutionContext* ctx, IdTable table,
                            std::vector<std::optional<Variable>> variables,
                            bool supportsLimit = false,
                            std::vector<ColumnIndex> sortedColumns = {},
                            LocalVocab localVocab = LocalVocab{},
                            std::optional<float> multiplicity = std::nullopt)
      : Operation{ctx},
        table_{std::move(table)},
        variables_{std::move(variables)},
        supportsLimit_{supportsLimit},
        sizeEstimate_{table_.numRows()},
        costEstimate_{table_.numRows()},
        resultSortedColumns_{std::move(sortedColumns)},
        localVocab_{std::move(localVocab)},
        multiplicity_{multiplicity} {
    AD_CONTRACT_CHECK(variables_.size() == table_.numColumns());
  }

  // Accessors for the estimates for manual testing.
  size_t& sizeEstimate() { return sizeEstimate_; }
  size_t& costEstimate() { return costEstimate_; }

  // ___________________________________________________________________________
  ResultTable computeResult() override {
    auto table = table_.clone();
    if (supportsLimit_) {
      table.erase(table.begin() + getLimit().upperBound(table.size()),
                  table.end());
      table.erase(table.begin(),
                  table.begin() + getLimit().actualOffset(table.size()));
    }
    return {std::move(table), resultSortedOn(), localVocab_.clone()};
  }
  bool supportsLimit() const override { return supportsLimit_; }

 private:
  // ___________________________________________________________________________
  string getCacheKeyImpl() const override {
    std::stringstream str;
    str << "Values for testing with " << table_.numColumns() << " columns. ";
    if (table_.numRows() > 1000) {
      str << ad_utility::FastRandomIntGenerator<int64_t>{}();
    } else {
      for (size_t i = 0; i < table_.numColumns(); ++i) {
        for (Id entry : table_.getColumn(i)) {
          str << entry << ' ';
        }
      }
    }
    str << " Supports limit: " << supportsLimit_;
    return std::move(str).str();
  }

 public:
  string getDescriptor() const override {
    return "explicit values for testing";
  }

  size_t getResultWidth() const override { return table_.numColumns(); }

  vector<ColumnIndex> resultSortedOn() const override {
    return resultSortedColumns_;
  }

  void setTextLimit(size_t limit) override { (void)limit; }

  size_t getCostEstimate() override { return costEstimate_; }

 private:
  uint64_t getSizeEstimateBeforeLimit() override { return sizeEstimate_; }

 public:
  // For unit testing purposes it is useful that the columns have different
  // multiplicities to find bugs in functions that use the multiplicity.
  float getMultiplicity(size_t col) override {
    if (multiplicity_.has_value()) return multiplicity_.value();
    (void)col;
    return static_cast<float>(col + 1) * 42.0f;
  }

  vector<QueryExecutionTree*> getChildren() override { return {}; }

  bool knownEmptyResult() override { return table_.empty(); }

 private:
  VariableToColumnMap computeVariableToColumnMap() const override {
    VariableToColumnMap m;
    for (auto i = ColumnIndex{0}; i < variables_.size(); ++i) {
      if (!variables_.at(i).has_value()) {
        continue;
      }
      bool containsUndef =
          ad_utility::contains(table_.getColumn(i), Id::makeUndefined());
      using enum ColumnIndexAndTypeInfo::UndefStatus;
      m[variables_.at(i).value()] = ColumnIndexAndTypeInfo{
          i, containsUndef ? PossiblyUndefined : AlwaysDefined};
    }
    return m;
  }

  std::vector<ColumnIndex> resultSortedColumns_;
  LocalVocab localVocab_;
  std::optional<float> multiplicity_;
};

// Similar to `ValuesForTesting` above, but `knownEmptyResult()` always returns
// false. This can be used for improved test coverage in cases where we want the
// empty result to be not optimized out by a check to `knownEmptyResult`.
class ValuesForTestingNoKnownEmptyResult : public ValuesForTesting {
 public:
  using ValuesForTesting::ValuesForTesting;
  bool knownEmptyResult() override { return false; }
};
