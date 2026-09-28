// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2025 Jonathan Zeller github@Jonathan24680, UFR
// 2024 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
// 2025        Patrick Brosi <brosi@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPATIALJOIN_H
#define QLEVER_SRC_ENGINE_SPATIALJOIN_H

#include <memory>
#include <optional>
#include <variant>

#include "engine/Operation.h"
#include "engine/SpatialJoinConfig.h"
#include "global/Id.h"
#include "rdfTypes/Variable.h"
#include "util/ContainersWithAllocator.h"

using SpatialJoinBoundingBoxColumns =
    qlm::optional<qlm::pair<ColumnIndex, ColumnIndex>>;

// Helper providing the left and right input tables and column indices to
// each spatial join algorithm.
struct PreparedSpatialJoinParams {
  const IdTableView<0>* const idTableLeft_;
  qlm::shared_ptr<const Result> resultLeft_;
  const IdTableView<0>* const idTableRight_;
  qlm::shared_ptr<const Result> resultRight_;
  ColumnIndex leftJoinCol_;
  ColumnIndex rightJoinCol_;
  qlm::vector<ColumnIndex> leftSelectedCols_;
  qlm::vector<ColumnIndex> rightSelectedCols_;
  size_t numColumns_;
};

// This class is implementing a SpatialJoin operation. This operations joins
// two tables, using their positional column. It supports nearest neighbor
// search as well as search of all points within a given range.
class SpatialJoin : public Operation {
 public:
  // creates a SpatialJoin operation: the required configuration object
  // can for example be obtained from the SpatialQuery class. Children are
  // optional, because they may be added later using the addChild method.
  // The substitutesFilterOp parameter indicates whether the spatial join
  // was explicitly requested by the user (false) or has been created to
  // implicitly rewrite a cartesian product with a geo filter (true).
  SpatialJoin(QueryExecutionContext* qec, SpatialJoinConfiguration config,
              qlm::optional<qlm::shared_ptr<QueryExecutionTree>> childLeft,
              qlm::optional<qlm::shared_ptr<QueryExecutionTree>> childRight,
              bool substitutesFilterOp = false);

 private:
  qlm::vector<QueryExecutionTree*> getChildrenImpl() const override;

 public:
  qlm::string getCacheKeyImpl() const override;
  qlm::string getDescriptor() const override;
  size_t getResultWidth() const override;
  size_t getCostEstimate() override;
  uint64_t getSizeEstimateBeforeLimit() override;

  // this function assumes, that the complete cross product is build and
  // returned. If the SpatialJoin does not have both children yet, it just
  // returns one as a dummy return. As no column gets joined in the
  // SpatialJoin (both point columns stay in the result table in their row)
  // each row can have at most the same number of distinct elements as it had
  // before. If the size increases, the multiplicity increases. The assumption
  // is, that the distinctness doesn't change and only the multiplicity
  // changes.
  float getMultiplicity(size_t col) override;

  bool knownEmptyResult() override;
  [[nodiscard]] qlm::vector<ColumnIndex> resultSortedOn() const override;
  Result computeResult(bool requestLaziness) override;

  // Depending on the amount of children the operation returns a different
  // VariableToColumnMap. If the operation doesn't have both children it needs
  // to aggressively push the queryplanner to add the children, because the
  // operation can't exist without them. If it has both children, it can
  // return the variable to column map, which will be present, after the
  // operation has computed its result
  VariableToColumnMap computeVariableToColumnMap() const override;

  // this method creates a new SpatialJoin object, to which the child gets
  // added. The reason for this behavior is, that the QueryPlanner can then
  // still use the existing SpatialJoin object, to try different orders
  qlm::shared_ptr<SpatialJoin> addChild(
      qlm::shared_ptr<QueryExecutionTree> child,
      const Variable& varOfChild) const;

  // if the spatialJoin has both children its construction is done. Then true
  // is returned. This function is needed in the QueryPlanner, so that the
  // QueryPlanner stops trying to add children, after the SpatialJoin is
  // already constructed
  bool isConstructed() const;

  // this function is used to give the maximum distance for internal purposes
  qlm::optional<double> getMaxDist() const;

  // this function is used to give the maximum number of results
  qlm::optional<size_t> getMaxResults() const;

  // this function is used to give the DE-9IM filter pattern, if the task is a
  // `LibSpatialJoinConfig` with one set (only relevant for the `DE9IM` join
  // type)
  qlm::optional<De9imFilterString> getDe9imFilter() const;

  // switch the algorithm set in the config parameter at construction time
  void selectAlgorithm(SpatialJoinAlgorithm algo) { config_.algo_ = algo; }

  // retrieve the currently selected algorithm
  SpatialJoinAlgorithm getAlgorithm() const { return config_.algo_; }

  // retrieve the currently selected spatial join type
  qlm::optional<SpatialJoinType> getJoinType() const {
    return config_.getJoinType();
  }

  // retrieve the variables the spatial join is joining on
  qlm::pair<Variable, Variable> getSpatialJoinVariables() const {
    return {config_.left_, config_.right_};
  }

  // get the boolean flag if this spatial join operation is used to substitute a
  // GeoSPARQL filter operation
  bool getSubstitutesFilterOp() const { return substitutesFilterOp_; }

  // Helper functions for unit tests
  qlm::pair<double, size_t> onlyForTestingGetTask() const {
    return qlm::pair{getMaxDist().value_or(-1.0), getMaxResults().value_or(-1)};
  }

  const SpatialJoinConfiguration& onlyForTestingGetConfig() const {
    return config_;
  }

  qlm::pair<Variable, Variable> onlyForTestingGetVariables() const {
    return qlm::pair{config_.left_, config_.right_};
  }

  qlm::optional<Variable> onlyForTestingGetDistanceVariable() const {
    return config_.distanceVariable_;
  }

  PayloadVariables onlyForTestingGetPayloadVariables() const {
    return config_.payloadVariables_;
  }

  qlm::shared_ptr<QueryExecutionTree> onlyForTestingGetLeftChild() const {
    return childLeft_;
  }

  qlm::shared_ptr<QueryExecutionTree> onlyForTestingGetRightChild() const {
    return childRight_;
  }

  PreparedSpatialJoinParams onlyForTestingGetPrepareJoin() const {
    return prepareJoin();
  }

  qlm::pair<SpatialJoinBoundingBoxColumns, SpatialJoinBoundingBoxColumns>
  onlyForTestingGetLibspatialjoinBoundingBoxCols() const {
    return prepareLibspatialjoinBoundingBoxCols();
  }

  void checkCancellationWrapperForSpatialJoinAlgorithms() const {
    checkCancellation();
  }

  qlm::optional<qlm::shared_ptr<QueryExecutionTree>> makeTreeWithBindColumn(
      const parsedQuery::Bind& bind) const override;

  // Get the internal variable names of bounding box columns.
  static qlm::pair<Variable, Variable> getBoundingBoxColumnNames(
      const Variable& joinVar);

  // Check if the child provides bounding boxes and if yes, return the column
  // indices.
  SpatialJoinBoundingBoxColumns getBoundingBoxColumnIndices(
      qlm::shared_ptr<QueryExecutionTree> child, const Variable& joinVar) const;

  // Make a clone of this `SpatialJoin` which uses precomputed bounding boxes of
  // the geometries from an underlying `MaterializedView` if possible.
  qlm::optional<qlm::shared_ptr<SpatialJoin>> cloneWithBoundingBoxColumns()
      const;

 private:
  [[nodiscard]] bool isDeterministicImpl() const override { return true; }

  std::unique_ptr<Operation> cloneImpl() const override;

  // helper function to generate a variable to column map from `childRight_`
  // that only contains the columns selected by `config_.payloadVariables_`
  // and (automatically added) the `config_.right_` variable.
  VariableToColumnMap getVarColMapPayloadVars() const;

  // The left/right children and join variables, swapped for a `WITHIN` join
  // (which is computed using `CONTAINS` on swapped tables, see `prepareJoin()`
  // and `prepareLibspatialjoinBoundingBoxCols()`, the two places that need
  // this swap).
  struct SwappedJoinSides {
    qlm::shared_ptr<QueryExecutionTree> childLeft_;
    qlm::shared_ptr<QueryExecutionTree> childRight_;
    Variable joinVarLeft_;
    Variable joinVarRight_;
  };
  SwappedJoinSides getSwappedJoinSides() const;

  // helper function, to initialize various required objects for all algorithms
  PreparedSpatialJoinParams prepareJoin() const;

  // Column indices of precomputed bounding boxes for both sides of the join,
  // only needed by `LibspatialjoinAlgorithm`. Computed separately from
  // `prepareJoin()` so the other algorithms don't pay for it.
  qlm::pair<SpatialJoinBoundingBoxColumns, SpatialJoinBoundingBoxColumns>
  prepareLibspatialjoinBoundingBoxCols() const;

  qlm::shared_ptr<QueryExecutionTree> childLeft_ = nullptr;
  qlm::shared_ptr<QueryExecutionTree> childRight_ = nullptr;

  SpatialJoinConfiguration config_;

  bool substitutesFilterOp_ = false;
};

#endif  // QLEVER_SRC_ENGINE_SPATIALJOIN_H
