#pragma once

#include "engine/Operation.h"
#include "parser/ParsedQuery.h"

// This class is implementing a SpatialJoin operation. This operations joins
// two tables, using their positional column. If the distance of the two
// positions is less than a given maximum distance, the pair will be in the
// result table.
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
  bool isConstructed();

  // this function is used to give the maximum distance for testing purposes
  long long getMaxDist() const;

  const std::shared_ptr<QueryExecutionTree> onlyForTestingGetLeftChild() {
    return childLeft_;
  }

  const std::shared_ptr<QueryExecutionTree> onlyForTestingGetRightChild() {
    return childRight_;
  }

  void onlyForTestingSetAddDistToResult(bool addDistToResult) {
    addDistToResult_ = addDistToResult;
  }

  const string& getInternalDistanceName() const {
    return nameDistanceInternal_;
  }

 private:
  void parseMaxDistance();
  std::optional<Variable> leftChildVariable_ = std::nullopt;
  std::optional<Variable> rightChildVariable_ = std::nullopt;
  std::shared_ptr<QueryExecutionTree> childLeft_ = nullptr;
  std::shared_ptr<QueryExecutionTree> childRight_ = nullptr;
  std::optional<SparqlTriple> triple_ = std::nullopt;
  long long maxDist_ = 0;  // max distance in meters

  // adds an extra column to the result, which contains the actual distance,
  // between the two objects
  bool addDistToResult_ = true;
  const string nameDistanceInternal_ = "?distOfTheTwoObjectsAddedInternally";
};
