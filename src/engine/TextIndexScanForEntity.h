//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#pragma once

#include <string>

#include "./Operation.h"
#include "parser/MagicServiceQuery.h"
#include "parser/TextSearchQuery.h"

// This operation retrieves all text records and their corresponding
// entities from the fulltext index that contain a certain word or prefix.
// The entities are saved to the entityVar_. If the operation is called on a
// fixed entity instead, it only returns entries that contain this entity.
class TextIndexScanForEntity : public Operation {
  TextIndexScanForEntityConfiguration config_;

 public:
  TextIndexScanForEntity(QueryExecutionContext* qec,
                         TextIndexScanForEntityConfiguration config);

  TextIndexScanForEntity(QueryExecutionContext* qec, Variable textRecordVar,
                         std::variant<Variable, std::string> entity,
                         string word);
  ~TextIndexScanForEntity() override = default;

  bool hasFixedEntity() const {
    return config_.varOrFixed_.value().hasFixedEntity();
  }

  const std::string& fixedEntity() const {
    AD_CONTRACT_CHECK(hasFixedEntity());
    return std::get<FixedEntity>(config_.varOrFixed_.value().entity_).first;
  }

  const Variable& entityVariable() const {
    AD_CONTRACT_CHECK(!hasFixedEntity());
    return std::get<Variable>(config_.varOrFixed_.value().entity_);
  }

  const Variable& textRecordVar() const { return config_.varToBindText_; }

  const std::string& word() const { return config_.word_; }

  string getCacheKeyImpl() const override;

  string getDescriptor() const override;

  size_t getResultWidth() const override;

  size_t getCostEstimate() override;

  uint64_t getSizeEstimateBeforeLimit() override;

  float getMultiplicity(size_t col) override {
    (void)col;
    return 1;
  }

  bool knownEmptyResult() override;

  vector<ColumnIndex> resultSortedOn() const override;

  void setVariableToColumnMap();

  VariableToColumnMap computeVariableToColumnMap() const override;

 private:
  const VocabIndex& getVocabIndexOfFixedEntity() const {
    AD_CONTRACT_CHECK(hasFixedEntity());
    return std::get<FixedEntity>(config_.varOrFixed_.value().entity_).second;
  }

  ProtoResult computeResult([[maybe_unused]] bool requestLaziness) override;

  vector<QueryExecutionTree*> getChildren() override { return {}; }
};
