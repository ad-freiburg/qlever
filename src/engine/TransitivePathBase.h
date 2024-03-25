// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#pragma once

#include <functional>
#include <memory>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

using TreeAndCol = std::pair<std::shared_ptr<QueryExecutionTree>, size_t>;
struct TransitivePathSide {
  // treeAndCol contains the QueryExecutionTree of this side and the column
  // where the Ids of this side are located. This member only has a value if
  // this side was bound.
  std::optional<TreeAndCol> treeAndCol_;
  // Column of the sub table where the Ids of this side are located
  size_t subCol_;
  std::variant<Id, Variable> value_;
  // The column in the ouput table where this side Ids are written to.
  // This member is set by the TransitivePath class
  size_t outputCol_ = 0;

  bool isVariable() const { return std::holds_alternative<Variable>(value_); };

  bool isBoundVariable() const { return treeAndCol_.has_value(); };

  std::string getCacheKey() const {
    std::ostringstream os;
    if (!isVariable()) {
      os << "Id: " << std::get<Id>(value_);
    }

    os << ", subColumn: " << subCol_ << "to " << outputCol_;

    if (treeAndCol_.has_value()) {
      const auto& [tree, col] = treeAndCol_.value();
      os << ", Subtree:\n";
      os << tree->getCacheKey() << "with join column " << col << "\n";
    }
    return std::move(os).str();
  }

  bool isSortedOnInputCol() const {
    if (!treeAndCol_.has_value()) {
      return false;
    }

    auto [tree, col] = treeAndCol_.value();
    const std::vector<ColumnIndex>& sortedOn =
        tree->getRootOperation()->getResultSortedOn();
    // TODO<C++23> use std::ranges::starts_with
    return (!sortedOn.empty() && sortedOn[0] == col);
  }
};

class TransitivePathBase : public Operation {
 protected:
  // We deliberately use the `std::` variants of a hash set and hash map because
  // `absl`s types are not exception safe.
  constexpr static auto hash = [](Id id) {
    return std::hash<uint64_t>{}(id.getBits());
  };
  using Set = std::unordered_set<Id, decltype(hash), std::equal_to<Id>,
                                 ad_utility::AllocatorWithLimit<Id>>;
  using Map = std::unordered_map<
      Id, Set, decltype(hash), std::equal_to<Id>,
      ad_utility::AllocatorWithLimit<std::pair<const Id, Set>>>;

  std::shared_ptr<QueryExecutionTree> subtree_;
  TransitivePathSide lhs_;
  TransitivePathSide rhs_;
  size_t resultWidth_ = 2;
  size_t minDist_;
  size_t maxDist_;
  VariableToColumnMap variableColumns_;

 public:
  TransitivePathBase(QueryExecutionContext* qec,
                     std::shared_ptr<QueryExecutionTree> child,
                     TransitivePathSide leftSide, TransitivePathSide rightSide,
                     size_t minDist, size_t maxDist);

  virtual ~TransitivePathBase() {}

  /**
   * Returns a new TransitivePath operation that uses the fact that leftop
   * generates all possible values for the left side of the paths. If the
   * results of leftop is smaller than all possible values this will result in a
   * faster transitive path operation (as the transitive paths has to be
   * computed for fewer elements).
   */
  std::shared_ptr<TransitivePathBase> bindLeftSide(
      std::shared_ptr<QueryExecutionTree> leftop, size_t inputCol) const;

  /**
   * Returns a new TransitivePath operation that uses the fact that rightop
   * generates all possible values for the right side of the paths. If the
   * results of rightop is smaller than all possible values this will result in
   * a faster transitive path operation (as the transitive paths has to be
   * computed for fewer elements).
   */
  std::shared_ptr<TransitivePathBase> bindRightSide(
      std::shared_ptr<QueryExecutionTree> rightop, size_t inputCol) const;

  bool isBoundOrId() const;

  /**
   * Getters, mainly necessary for testing
   */
  size_t getMinDist() const { return minDist_; }
  size_t getMaxDist() const { return maxDist_; }
  const TransitivePathSide& getLeft() const { return lhs_; }
  const TransitivePathSide& getRight() const { return rhs_; }

 protected:
  std::string getCacheKeyImpl() const override;

 public:
  std::string getDescriptor() const override;

  size_t getResultWidth() const override;

  vector<ColumnIndex> resultSortedOn() const override;

  void setTextLimit(size_t limit) override;

  bool knownEmptyResult() override;

  float getMultiplicity(size_t col) override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  size_t getCostEstimate() override;

  /**
   * @brief Make a concrete TransitivePath object using the given parameters.
   * The concrete object will either be TransitivePathFallback or
   * TransitivePathBinSearch, depending on the useBinSearch flag.
   *
   * @param qec QueryExecutionContext for the TransitivePath Operation
   * @param child QueryExecutionTree for the subquery of the TransitivePath
   * @param leftSide Settings for the left side of the TransitivePath
   * @param rightSide Settings for the right side of the TransitivePath
   * @param minDist Minimum distance a resulting path may have (distance =
   * number of nodes)
   * @param maxDist Maximum distance a resulting path may have (distance =
   * number of nodes)
   * @param useBinSearch If true, the returned object will be a
   * TransitivePathBinSearch. Else it will be a TransitivePathFallback
   */
  static std::shared_ptr<TransitivePathBase> makeTransitivePath(
      QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> child,
      const TransitivePathSide& leftSide, const TransitivePathSide& rightSide,
      size_t minDist, size_t maxDist, bool useBinSearch);

  /**
   * @brief Make a concrete TransitivePath object using the given parameters.
   * The concrete object will either be TransitivePathFallback or
   * TransitivePathBinSearch, depending on the runtime constant "use-binsearch".
   *
   * @param qec QueryExecutionContext for the TransitivePath Operation
   * @param child QueryExecutionTree for the subquery of the TransitivePath
   * @param leftSide Settings for the left side of the TransitivePath
   * @param rightSide Settings for the right side of the TransitivePath
   * @param minDist Minimum distance a resulting path may have (distance =
   * number of nodes)
   * @param maxDist Maximum distance a resulting path may have (distance =
   * number of nodes)
   */
  static std::shared_ptr<TransitivePathBase> makeTransitivePath(
      QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> child,
      const TransitivePathSide& leftSide, const TransitivePathSide& rightSide,
      size_t minDist, size_t maxDist);

  vector<QueryExecutionTree*> getChildren() override {
    std::vector<QueryExecutionTree*> res;
    auto addChildren = [](std::vector<QueryExecutionTree*>& res,
                          TransitivePathSide side) {
      if (side.treeAndCol_.has_value()) {
        res.push_back(side.treeAndCol_.value().first.get());
      }
    };
    addChildren(res, lhs_);
    addChildren(res, rhs_);
    res.push_back(subtree_.get());
    return res;
  }

  VariableToColumnMap computeVariableToColumnMap() const override;

  // The internal implementation of `bindLeftSide` and `bindRightSide` which
  // share a lot of code.
  std::shared_ptr<TransitivePathBase> bindLeftOrRightSide(
      std::shared_ptr<QueryExecutionTree> leftOrRightOp, size_t inputCol,
      bool isLeft) const;
};
