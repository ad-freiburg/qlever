#pragma once

#include "Operation.h"
#include "parser/ParsedQuery.h"

class SpatialJoin : public Operation {
  public:
    /*SpatialJoin();
    SpatialJoin(QueryExecutionContext* qec, SparqlTriple triple);
    SpatialJoin(QueryExecutionContext* qec,
      std::shared_ptr<QueryExecutionTree> t1,
      std::shared_ptr<QueryExecutionTree> t2,
      ColumnIndex t1JoinCol, ColumnIndex t2JoinCol,
      bool keepJoinColumn);
    SpatialJoin(QueryExecutionContext* qec,
      std::shared_ptr<QueryExecutionTree> t1,
      std::shared_ptr<QueryExecutionTree> t2,
      ColumnIndex t1JoinCol, ColumnIndex t2JoinCol,
      bool keepJoinColumn, IdTable leftChildTable,
      std::vector<std::optional<Variable>> variablesLeft,
      IdTable rightChildTable,
      std::vector<std::optional<Variable>> variablesRight);*/
    SpatialJoin(QueryExecutionContext* qec, SparqlTriple triple,
        std::optional<std::shared_ptr<QueryExecutionTree>> childLeft_,
        std::optional<std::shared_ptr<QueryExecutionTree>> childRight_,
        int maxDist);
    // shared_ptr<const ResultTable> geoJoinTest();
    std::vector<QueryExecutionTree*> getChildren() override;
    // eindeutiger String für die Objekte der Klasse:
    string getCacheKeyImpl() const override; 
    string getDescriptor() const override; // für Menschen
    size_t getResultWidth() const override; // wie viele Variablen man zurückgibt
    size_t getCostEstimate() override;
    uint64_t getSizeEstimateBeforeLimit() override;
    float getMultiplicity(size_t col) override;
    bool knownEmptyResult() override;
    [[nodiscard]] vector<ColumnIndex> resultSortedOn() const override;
    ResultTable computeResult() override;
    VariableToColumnMap computeVariableToColumnMap() const override;
    std::shared_ptr<SpatialJoin> addChild(
          std::shared_ptr<QueryExecutionTree> child, Variable varOfChild);
    bool isConstructed();
  private:
    std::shared_ptr<QueryExecutionTree> _left;
    std::shared_ptr<QueryExecutionTree> _right;
    ColumnIndex _leftJoinCol;
    ColumnIndex _rightJoinCol;
    bool _keepJoinColumn;
    std::vector<std::optional<Variable>> _variablesLeft;
    std::vector<std::optional<Variable>> _variablesRight;
    bool _verbose_init = false;
    ad_utility::MemorySize _limit = ad_utility::MemorySize::bytes(100000000);
    ad_utility::AllocatorWithLimit<ValueId> _allocator =
            ad_utility::makeAllocatorWithLimit<ValueId>(_limit);
    std::optional<Variable> leftChildVariable = std::nullopt;
    std::optional<Variable> rightChildVariable = std::nullopt;
    std::shared_ptr<QueryExecutionTree> childLeft = nullptr;
    std::shared_ptr<QueryExecutionTree> childRight = nullptr;
    std::optional<SparqlTriple> triple = std::nullopt;
    int maxDist = 0;
    bool addDistToResult = true;
};