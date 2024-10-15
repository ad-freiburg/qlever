//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: @Jonathan24680
//  Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#pragma once

#include <optional>

#include "engine/Operation.h"
#include "global/Id.h"
#include "parser/ParsedQuery.h"

// Configuration to restrict the results provided by the SpatialJoin
struct NearestNeighborsConfig {
  size_t maxResults_;
  std::optional<size_t> maxDist_ = std::nullopt;
};
struct MaxDistanceConfig {
  size_t maxDist_;
};

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
  SpatialJoin(QueryExecutionContext* qec, SparqlTriple triple,
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

  // this function is used to give the maximum number of results for internal
  // purposes
  std::optional<size_t> getMaxResults() const;

  void selectAlgorithm(bool useBaselineAlgorithm) {
    useBaselineAlgorithm_ = useBaselineAlgorithm;
  }

  std::pair<size_t, size_t> onlyForTestingGetConfig() const {
    return std::pair{getMaxDist().value_or(-1), getMaxResults().value_or(-1)};
  }

  std::shared_ptr<QueryExecutionTree> onlyForTestingGetLeftChild() const {
    return childLeft_;
  }

  std::shared_ptr<QueryExecutionTree> onlyForTestingGetRightChild() const {
    return childRight_;
  }

  const string& getInternalDistanceName() const {
    return nameDistanceInternal_;
  }

 private:
  // helper function, which parses a triple and populates config_
  void parseConfigFromTriple();

  // helper function which returns a GeoPoint if the element of the given table
  // represents a GeoPoint
  std::optional<GeoPoint> getPoint(const IdTable* restable, size_t row,
                                   ColumnIndex col) const;

  // helper function, which computes the distance of two points, where each
  // point comes from a different result table
  Id computeDist(const IdTable* resLeft, const IdTable* resRight,
                 size_t rowLeft, size_t rowRight, ColumnIndex leftPointCol,
                 ColumnIndex rightPointCol) const;

  // Helper function, which adds a row, which belongs to the result to the
  // result table. As inputs it uses a row of the left and a row of the right
  // child result table.
  void addResultTableEntry(IdTable* result, const IdTable* resultLeft,
                           const IdTable* resultRight, size_t rowLeft,
                           size_t rowRight, Id distance) const;

  // helper struct to improve readability in prepareJoin()
  struct PreparedJoinParams {
    const IdTable* const idTableLeft_;
    std::shared_ptr<const Result> resultLeft_;
    const IdTable* const idTableRight_;
    std::shared_ptr<const Result> resultRight_;
    ColumnIndex leftJoinCol_;
    ColumnIndex rightJoinCol_;
    size_t numColumns_;
  };

  // helper function, to initialize various required objects for both algorithms
  PreparedJoinParams prepareJoin() const;

  // the baseline algorithm, which just checks every combination
  Result baselineAlgorithm();

  // the improved algorithm, which constructs a S2PointIndex, which is usually
  // much faster, however requires that the right relation fits into memory
  Result s2geometryAlgorithm();

  SparqlTriple triple_;
  Variable leftChildVariable_;
  Variable rightChildVariable_;
  std::shared_ptr<QueryExecutionTree> childLeft_ = nullptr;
  std::shared_ptr<QueryExecutionTree> childRight_ = nullptr;

  std::variant<NearestNeighborsConfig, MaxDistanceConfig> config_;

  // adds an extra column to the result, which contains the actual distance,
  // between the two objects
  bool addDistToResult_ = true;
  const string nameDistanceInternal_ = "?distOfTheTwoObjectsAddedInternally";
  bool useBaselineAlgorithm_ = false;
};
