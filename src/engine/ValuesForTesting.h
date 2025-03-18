//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/Operation.h"
#include "engine/QueryExecutionContext.h"
#include "engine/Result.h"
#include "util/Algorithm.h"
#include "util/Random.h"
#include "util/TransparentFunctors.h"

// An operation that yields a given `IdTable` as its result. It is used for
// unit testing purposes when we need to specify the subtrees of another
// operation.
namespace detail {
auto getTables(const auto& tables) {
  return ql::views::transform(tables, ad_utility::dereference);
}
}  // namespace detail
class ValuesForTesting : public Operation {
 public:
  using VarVector = std::vector<std::optional<Variable>>;

 private:
  std::vector<std::shared_ptr<const IdTable>> tables_;
  VariableToColumnMap variables_;
  bool supportsLimit_;
  // Those can be manually overwritten for testing using the respective getters.
  size_t sizeEstimate_;
  size_t costEstimate_;
  bool unlikelyToFitInCache_ = false;
  ad_utility::MemorySize* cacheSizeStorage_ = nullptr;

 public:
  // Create an operation that has as its result the given `table` and the given
  // `variables`. The number of variables must be equal to the number
  // of columns in the table.
  explicit ValuesForTesting(QueryExecutionContext* ctx, IdTable table,
                            const VarVector& variables,
                            bool supportsLimit = false,
                            std::vector<ColumnIndex> sortedColumns = {},
                            LocalVocab localVocab = LocalVocab{},
                            std::optional<float> multiplicity = std::nullopt,
                            bool forceFullyMaterialized = false)
      : Operation{ctx},
        supportsLimit_{supportsLimit},
        sizeEstimate_{table.numRows()},
        costEstimate_{table.numRows()},
        resultSortedColumns_{std::move(sortedColumns)},
        localVocab_{std::move(localVocab)},
        multiplicity_{multiplicity},
        forceFullyMaterialized_{forceFullyMaterialized} {
    AD_CONTRACT_CHECK(variables.size() == table.numColumns());
    tables_.push_back(std::make_shared<const IdTable>(std::move(table)));
    variables_ = computeVarMapFromVector(variables);
  }

  ValuesForTesting(QueryExecutionContext* ctx,
                   std::shared_ptr<const IdTable> table,
                   VariableToColumnMap variables,
                   std::vector<ColumnIndex> sortedColumns = {},
                   LocalVocab localVocab = LocalVocab{})
      : Operation{ctx},
        variables_{std::move(variables)},
        supportsLimit_{false},
        sizeEstimate_{table->numRows()},
        costEstimate_{0},
        resultSortedColumns_{std::move(sortedColumns)},
        localVocab_{std::move(localVocab)},
        multiplicity_{},
        forceFullyMaterialized_{false} {
    tables_.push_back(std::move(table));
  }

  ValuesForTesting(QueryExecutionContext* ctx, IdTable table,
                   VariableToColumnMap variables,
                   std::vector<ColumnIndex> sortedColumns = {},
                   LocalVocab localVocab = LocalVocab{})
      : ValuesForTesting{ctx, std::make_shared<const IdTable>(std::move(table)),
                         std::move(variables), std::move(sortedColumns),
                         std::move(localVocab)} {}

  explicit ValuesForTesting(QueryExecutionContext* ctx,
                            std::vector<IdTable> tables, VarVector variables,
                            bool unlikelyToFitInCache = false,
                            std::vector<ColumnIndex> sortedColumns = {},
                            LocalVocab localVocab = LocalVocab{})
      : Operation{ctx},
        tables_{},
        supportsLimit_{false},
        sizeEstimate_{0},
        costEstimate_{0},
        unlikelyToFitInCache_{unlikelyToFitInCache},
        resultSortedColumns_{std::move(sortedColumns)},
        localVocab_{std::move(localVocab)},
        multiplicity_{std::nullopt} {
    tables_.reserve(tables.size());
    for (auto& table : tables) {
      tables_.push_back(std::make_shared<const IdTable>(std::move(table)));
    }
    AD_CONTRACT_CHECK(ql::ranges::all_of(
        detail::getTables(tables_), [&variables](const IdTable& table) {
          return variables.size() == table.numColumns();
        }));
    size_t totalRows = 0;
    for (const IdTable& idTable : detail::getTables(tables_)) {
      totalRows += idTable.numRows();
    }
    sizeEstimate_ = totalRows;
    costEstimate_ = totalRows;
    variables_ = computeVarMapFromVector(variables);
  }

  ValuesForTesting(ValuesForTesting&&) = default;
  ValuesForTesting& operator=(ValuesForTesting&&) = default;

  // Accessors for the estimates for manual testing.
  size_t& sizeEstimate() { return sizeEstimate_; }
  size_t& costEstimate() { return costEstimate_; }

  // ___________________________________________________________________________
  Result computeResult(bool requestLaziness) override {
    if (requestLaziness && !forceFullyMaterialized_ && tables_.size() != 1) {
      // Not implemented yet
      AD_CORRECTNESS_CHECK(!supportsLimit_);
      std::vector<IdTable> clones;
      clones.reserve(tables_.size());
      for (const IdTable& idTable : detail::getTables(tables_)) {
        clones.push_back(idTable.clone());
      }
      auto generator = [](auto idTables,
                          LocalVocab localVocab) -> Result::Generator {
        for (IdTable& idTable : idTables) {
          co_yield {std::move(idTable), localVocab.clone()};
        }
      }(std::move(clones), localVocab_.clone());
      return {std::move(generator), resultSortedOn()};
    }

    if (tables_.size() == 1 && getLimit().isUnconstrained()) {
      return {tables_.at(0), resultSortedOn(), localVocab_.clone()};
    }
    std::optional<IdTable> optionalTable;
    if (detail::getTables(tables_).size() > 1) {
      IdTable aggregateTable{tables_.at(0)->numColumns(),
                             tables_.at(0)->getAllocator()};
      for (const IdTable& idTable : detail::getTables(tables_)) {
        aggregateTable.insertAtEnd(idTable);
      }
      optionalTable = std::move(aggregateTable);
    }
    auto table = optionalTable.has_value() ? std::move(optionalTable).value()
                                           : tables_.at(0)->clone();
    if (supportsLimit_) {
      table.erase(table.begin() + getLimit().upperBound(table.size()),
                  table.end());
      table.erase(table.begin(),
                  table.begin() + getLimit().actualOffset(table.size()));
    }
    return {std::move(table), resultSortedOn(), localVocab_.clone()};
  }
  bool unlikelyToFitInCache(ad_utility::MemorySize cacheSize) const override {
    if (cacheSizeStorage_ != nullptr) {
      *cacheSizeStorage_ = cacheSize;
    }
    return unlikelyToFitInCache_;
  }

  void setCacheSizeStorage(ad_utility::MemorySize* cacheSizeStorage) {
    cacheSizeStorage_ = cacheSizeStorage;
  }

  bool supportsLimit() const override { return supportsLimit_; }

  bool& forceFullyMaterialized() { return forceFullyMaterialized_; }

 private:
  // ___________________________________________________________________________
  string getCacheKeyImpl() const override {
    std::stringstream str;
    auto numRowsView =
        detail::getTables(tables_) | ql::views::transform(&IdTable::numRows);
    auto totalNumRows = std::reduce(numRowsView.begin(), numRowsView.end(), 0);
    auto numCols = tables_.empty() ? 0 : tables_.at(0)->numColumns();
    str << "Values for testing with " << numCols << " columns and "
        << totalNumRows << " rows. ";
    if (totalNumRows > 1000) {
      str << ad_utility::FastRandomIntGenerator<int64_t>{}();
    } else {
      for (const IdTable& idTable : detail::getTables(tables_)) {
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
    // Assume a width of 1 if we have no tables and no other information to base
    // it on because 0 would otherwise cause stuff to break.
    return tables_.empty() ? 1 : tables_.at(0)->numColumns();
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
    return ql::ranges::all_of(
        detail::getTables(tables_),
        [](const IdTable& table) { return table.empty(); });
  }

 private:
  VariableToColumnMap computeVarMapFromVector(const VarVector& vars) const {
    VariableToColumnMap m;
    for (auto i = ColumnIndex{0}; i < vars.size(); ++i) {
      if (!vars.at(i).has_value()) {
        continue;
      }
      bool containsUndef = ql::ranges::any_of(
          detail::getTables(tables_), [&i](const IdTable& table) {
            return ql::ranges::any_of(table.getColumn(i),
                                      [](Id id) { return id.isUndefined(); });
          });
      using enum ColumnIndexAndTypeInfo::UndefStatus;
      m[vars.at(i).value()] = ColumnIndexAndTypeInfo{
          i, containsUndef ? PossiblyUndefined : AlwaysDefined};
    }
    return m;
  }
  VariableToColumnMap computeVariableToColumnMap() const override {
    return variables_;
  }

  // _____________________________________________________________________________
  ValuesForTesting(const ValuesForTesting& other)
      : Operation{other._executionContext},
        variables_{other.variables_},
        supportsLimit_{other.supportsLimit_},
        sizeEstimate_{other.sizeEstimate_},
        costEstimate_{other.costEstimate_},
        unlikelyToFitInCache_{other.unlikelyToFitInCache_},
        resultSortedColumns_{other.resultSortedColumns_},
        localVocab_{other.localVocab_.clone()},
        multiplicity_{other.multiplicity_},
        forceFullyMaterialized_{other.forceFullyMaterialized_} {
    for (const auto& idTable : other.tables_) {
      tables_.push_back(std::make_shared<IdTable>(idTable->clone()));
    }
  }

  ValuesForTesting& operator=(const ValuesForTesting&) = delete;

  std::unique_ptr<Operation> cloneImpl() const override {
    return std::make_unique<ValuesForTesting>(ValuesForTesting{*this});
  }

  std::vector<ColumnIndex> resultSortedColumns_;
  LocalVocab localVocab_;
  std::optional<float> multiplicity_;
  bool forceFullyMaterialized_ = false;
};

// Similar to `ValuesForTesting` above, but `knownEmptyResult()` always returns
// false. This can be used for improved test coverage in cases where we want the
// empty result to be not optimized out by a check to `knownEmptyResult`.
class ValuesForTestingNoKnownEmptyResult : public ValuesForTesting {
 public:
  using ValuesForTesting::ValuesForTesting;
  bool knownEmptyResult() override { return false; }
  uint64_t getSizeEstimateBeforeLimit() override { return 1; }
};
