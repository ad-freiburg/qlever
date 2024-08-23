//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/Operation.h"
#include "engine/QueryExecutionContext.h"
#include "engine/Result.h"
#include "util/Algorithm.h"
#include "util/Random.h"

// An operation that yields a given `IdTable` as its result. It is used for
// unit testing purposes when we need to specify the subtrees of another
// operation.
class ValuesForTesting : public Operation {
 private:
  std::vector<IdTable> tables_;
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
        tables_{},
        variables_{std::move(variables)},
        supportsLimit_{supportsLimit},
        sizeEstimate_{table.numRows()},
        costEstimate_{table.numRows()},
        resultSortedColumns_{std::move(sortedColumns)},
        localVocab_{std::move(localVocab)},
        multiplicity_{multiplicity} {
    AD_CONTRACT_CHECK(variables_.size() == table.numColumns());
    tables_.push_back(std::move(table));
  }
  explicit ValuesForTesting(QueryExecutionContext* ctx,
                            std::vector<IdTable> tables,
                            std::vector<std::optional<Variable>> variables)
      : Operation{ctx},
        tables_{std::move(tables)},
        variables_{std::move(variables)},
        supportsLimit_{false},
        sizeEstimate_{0},
        costEstimate_{0},
        resultSortedColumns_{},
        localVocab_{LocalVocab{}},
        multiplicity_{std::nullopt} {
    AD_CONTRACT_CHECK(
        std::ranges::all_of(tables_, [this](const IdTable& table) {
          return variables_.size() == table.numColumns();
        }));
    size_t totalRows = 0;
    for (const IdTable& idTable : tables_) {
      totalRows += idTable.numRows();
    }
    sizeEstimate_ = totalRows;
    costEstimate_ = totalRows;
  }

  // Accessors for the estimates for manual testing.
  size_t& sizeEstimate() { return sizeEstimate_; }
  size_t& costEstimate() { return costEstimate_; }

  // ___________________________________________________________________________
  ProtoResult computeResult(bool requestLaziness) override {
    if (requestLaziness) {
      // Not implemented yet
      AD_CORRECTNESS_CHECK(!supportsLimit_);
      std::vector<IdTable> clones;
      clones.reserve(tables_.size());
      for (const IdTable& idTable : tables_) {
        clones.push_back(idTable.clone());
      }
      auto generator = [](auto idTables) -> cppcoro::generator<IdTable> {
        for (IdTable& idTable : idTables) {
          co_yield std::move(idTable);
        }
      }(std::move(clones));
      return {std::move(generator), resultSortedOn(), localVocab_.clone()};
    }
    std::optional<IdTable> optionalTable;
    if (tables_.size() > 1) {
      IdTable aggregateTable{tables_.at(0).numColumns(),
                             tables_.at(0).getAllocator()};
      for (const IdTable& idTable : tables_) {
        aggregateTable.insertAtEnd(idTable);
      }
      optionalTable = std::move(aggregateTable);
    }
    auto table = optionalTable.has_value() ? std::move(optionalTable).value()
                                           : tables_.at(0).clone();
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
    auto numRowsView = tables_ | std::views::transform(&IdTable::numRows);
    auto totalNumRows = std::reduce(numRowsView.begin(), numRowsView.end(), 0);
    auto numCols = tables_.empty() ? 0 : tables_.at(0).numColumns();
    str << "Values for testing with " << numCols << " columns and "
        << totalNumRows << " rows. ";
    if (totalNumRows > 1000) {
      str << ad_utility::FastRandomIntGenerator<int64_t>{}();
    } else {
      for (const IdTable& idTable : tables_) {
        for (size_t i = 0; i < idTable.numColumns(); ++i) {
          for (Id entry : idTable.getColumn(i)) {
            str << entry << ' ';
          }
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

  size_t getResultWidth() const override {
    return tables_.empty() ? 0 : tables_.at(0).numColumns();
  }

  vector<ColumnIndex> resultSortedOn() const override {
    return resultSortedColumns_;
  }

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

  bool knownEmptyResult() override {
    return std::ranges::all_of(
        tables_, [](const IdTable& table) { return table.empty(); });
  }

 private:
  VariableToColumnMap computeVariableToColumnMap() const override {
    VariableToColumnMap m;
    for (auto i = ColumnIndex{0}; i < variables_.size(); ++i) {
      if (!variables_.at(i).has_value()) {
        continue;
      }
      bool containsUndef =
          std::ranges::any_of(tables_, [&i](const IdTable& table) {
            return std::ranges::any_of(table.getColumn(i),
                                       [](Id id) { return id.isUndefined(); });
          });
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
