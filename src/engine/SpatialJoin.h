//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: @Jonathan24680
//  Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#pragma once

#include <memory>
#include <optional>
#include <variant>

#include "engine/Operation.h"
#include "global/Id.h"
#include "parser/PayloadVariables.h"
#include "parser/data/Variable.h"

// A nearest neighbor search with optionally a maximum distance.
struct NearestNeighborsConfig {
  size_t maxResults_;
  std::optional<size_t> maxDist_ = std::nullopt;
};

// A spatial search limited only by a maximum distance.
struct MaxDistanceConfig {
  size_t maxDist_;
};

// Configuration to restrict the results provided by the SpatialJoin
using SpatialJoinTask = std::variant<NearestNeighborsConfig, MaxDistanceConfig>;

// Selection of a SpatialJoin algorithm
enum class SpatialJoinAlgorithm { BASELINE, S2_GEOMETRY, BOUNDING_BOX };
const SpatialJoinAlgorithm SPATIAL_JOIN_DEFAULT_ALGORITHM =
    SpatialJoinAlgorithm::S2_GEOMETRY;

// The configuration object that will be provided by the special SERVICE.
struct SpatialJoinConfiguration {
  // The task defines search parameters
  SpatialJoinTask task_;

  // The variables for the two tables to be joined
  Variable left_;
  Variable right_;

  // If given, the distance will be added to the result and be bound to this
  // variable.
  std::optional<Variable> distanceVariable_ = std::nullopt;

  // If given a vector of variables, the selected variables will be part of the
  // result table - the join column will automatically be part of the result.
  // You may use PayloadAllVariables to select all columns of the right table.
  PayloadVariables payloadVariables_ = PayloadVariables::all();

  // Choice of algorithm. Both algorithms have equal results, but different
  // runtime characteristics.
  SpatialJoinAlgorithm algo_ = SPATIAL_JOIN_DEFAULT_ALGORITHM;
};

// helper struct to improve readability in prepareJoin()
struct PreparedSpatialJoinParams {
  const IdTable* const idTableLeft_;
  std::shared_ptr<const Result> resultLeft_;
  const IdTable* const idTableRight_;
  std::shared_ptr<const Result> resultRight_;
  ColumnIndex leftJoinCol_;
  ColumnIndex rightJoinCol_;
  std::vector<ColumnIndex> rightSelectedCols_;
  size_t numColumns_;
  std::optional<size_t> maxDist_;
  std::optional<size_t> maxResults_;
};

// The spatial join operation without a limit on the maximum number of results
// can, in the worst case have a square number of results, but usually this is
// not the case. 1 divided by this constant is the damping factor for the
// estimated number of results.
static const size_t SPATIAL_JOIN_MAX_DIST_SIZE_ESTIMATE = 1000;

// This class is implementing a SpatialJoin operation. This operations joins
// two tables, using their positional column. It supports nearest neighbor
// search as well as search of all points within a given range.
class SpatialJoin : public Operation {
 public:
  // creates a SpatialJoin operation. The triple is needed, to get the
  // variable names. Those are the names of the children, which get added
  // later. In addition to that, the SpatialJoin operation needs a maximum
  // distance, which two objects can be apart, which will still be accepted
  // as a match and therefore be part of the result table. The distance is
  // parsed from the triple.
  SpatialJoin(QueryExecutionContext* qec, SpatialJoinConfiguration config,
              std::optional<std::shared_ptr<QueryExecutionTree>> childLeft_,
              std::optional<std::shared_ptr<QueryExecutionTree>> childRight_);

  std::vector<QueryExecutionTree*> getChildren() override;
  string getCacheKeyImpl() const override;
  string getDescriptor() const override;
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
  [[nodiscard]] vector<ColumnIndex> resultSortedOn() const override;
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
  std::shared_ptr<SpatialJoin> addChild(
      std::shared_ptr<QueryExecutionTree> child,
      const Variable& varOfChild) const;

  // if the spatialJoin has both children its construction is done. Then true
  // is returned. This function is needed in the QueryPlanner, so that the
  // QueryPlanner stops trying to add children, after the SpatialJoin is
  // already constructed
  bool isConstructed() const;

  // this function is used to give the maximum distance for internal purposes
  std::optional<size_t> getMaxDist() const;

  // this function is used to give the maximum number of results
  std::optional<size_t> getMaxResults() const;

  // switch the algorithm set in the config parameter at construction time
  void selectAlgorithm(SpatialJoinAlgorithm algo) { config_.algo_ = algo; }

  // retrieve the currently selected algorithm
  SpatialJoinAlgorithm getAlgorithm() const { return config_.algo_; }

  // Helper functions for unit tests
  std::pair<size_t, size_t> onlyForTestingGetTask() const {
    return std::pair{getMaxDist().value_or(-1), getMaxResults().value_or(-1)};
  }

  const SpatialJoinConfiguration& onlyForTestingGetConfig() const {
    return config_;
  }

  std::pair<Variable, Variable> onlyForTestingGetVariables() const {
    return std::pair{config_.left_, config_.right_};
  }

  std::optional<Variable> onlyForTestingGetDistanceVariable() const {
    return config_.distanceVariable_;
  }

  PayloadVariables onlyForTestingGetPayloadVariables() const {
    return config_.payloadVariables_;
  }

  std::shared_ptr<QueryExecutionTree> onlyForTestingGetLeftChild() const {
    return childLeft_;
  }

  std::shared_ptr<QueryExecutionTree> onlyForTestingGetRightChild() const {
    return childRight_;
  }

 private:
  // helper function to generate a variable to column map from `childRight_`
  // that only contains the columns selected by `config_.payloadVariables_`
  // and (automatically added) the `config_.right_` variable.
  VariableToColumnMap getVarColMapPayloadVars() const;

  // helper function, to initialize various required objects for both algorithms
  PreparedSpatialJoinParams prepareJoin() const;

  std::shared_ptr<QueryExecutionTree> childLeft_ = nullptr;
  std::shared_ptr<QueryExecutionTree> childRight_ = nullptr;

  SpatialJoinConfiguration config_;
};
