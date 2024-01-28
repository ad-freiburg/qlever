#pragma once

#include "Operation.h"
#include "parser/ParsedQuery.h"

class SpatialJoin : public Operation {
  public:
    SpatialJoin(QueryExecutionContext* qec, SparqlTriple triple,
        std::optional<std::shared_ptr<QueryExecutionTree>> childLeft_,
        std::optional<std::shared_ptr<QueryExecutionTree>> childRight_,
        int maxDist);
    std::vector<QueryExecutionTree*> getChildren() override;
    string getCacheKeyImpl() const override; 
    string getDescriptor() const override;
    size_t getResultWidth() const override;
    size_t getCostEstimate() override;
    uint64_t getSizeEstimateBeforeLimit() override;
    float getMultiplicity(size_t col) override;
    bool knownEmptyResult() override;
    [[nodiscard]] vector<ColumnIndex> resultSortedOn() const override;
    ResultTable computeResult() override;

    /* Depending on the amount of children the operation returns a different
    VariableToColumnMap. If the operation doesn't have both children it needs
    to aggressively push the queryplanner to add the children, because the
    operation can't exist without them. If it has both children, it can return
    the variable to column map, which will be present, after the operation has
    computed its result */
    VariableToColumnMap computeVariableToColumnMap() const override;

    /* this method creates a new SpatialJoin object, to which the child gets
    added. The reason for this behavior is, that the QueryPlanner can then
    still use the existing SpatialJoin object, to try different orders
    */
    std::shared_ptr<SpatialJoin> addChild(
          std::shared_ptr<QueryExecutionTree> child, Variable varOfChild);
    
    /* if the spatialJoin has both children its construction is done. Then true
    is returned. This function is needed in the QueryPlanner, so that the
    QueryPlanner stops trying to add children, after it is already constructed
    */
    bool isConstructed();

  private:
    ad_utility::MemorySize _limit = ad_utility::MemorySize::bytes(100000000);
    ad_utility::AllocatorWithLimit<ValueId> _allocator =
            ad_utility::makeAllocatorWithLimit<ValueId>(_limit);
    std::optional<Variable> leftChildVariable = std::nullopt;
    std::optional<Variable> rightChildVariable = std::nullopt;
    std::shared_ptr<QueryExecutionTree> childLeft = nullptr;
    std::shared_ptr<QueryExecutionTree> childRight = nullptr;
    std::optional<SparqlTriple> triple = std::nullopt;
    int maxDist = 0;  // max distance in meters, 0 encodes an infinite distance
    // adds an extra column to the result, which contains the actual distance,
    // between the two objects
    bool addDistToResult = true;
};