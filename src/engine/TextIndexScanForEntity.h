//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_TEXTINDEXSCANFORENTITY_H
#define QLEVER_SRC_ENGINE_TEXTINDEXSCANFORENTITY_H

#include <string>

#include "engine/Operation.h"
#include "parser/TextSearchQuery.h"

// This operation retrieves all text records and their corresponding
// entities from the fulltext index that contain a certain word or prefix.
// The entities are saved to the entityVar_. If the operation is called on a
// fixed entity instead, it only returns entries that contain this entity.
// In detail, it retrieves all blocks the word or prefix touches. No filtering
// happens why it is necessary to join this with a TextIndexScanForWord on the
// textVar. During tests where this join doesn't happen, this can lead to
// unexpected behavior.
class TextIndexScanForEntity : public Operation {
  TextIndexScanForEntityConfiguration config_;

 public:
  TextIndexScanForEntity(QueryExecutionContext* qec,
                         TextIndexScanForEntityConfiguration config);

  TextIndexScanForEntity(QueryExecutionContext* qec, Variable textRecordVar,
                         std::variant<Variable, std::string> entity,
                         std::string word);
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

  std::string getCacheKeyImpl() const override;

  std::string getDescriptor() const override;

  size_t getResultWidth() const override;

  size_t getCostEstimate() override;

  uint64_t getSizeEstimateBeforeLimit() override;

  float getMultiplicity(size_t) override { return 1; }

  bool knownEmptyResult() override;

  vector<ColumnIndex> resultSortedOn() const override;

  VariableToColumnMap computeVariableToColumnMap() const override;

  const TextIndexScanForEntityConfiguration& getConfig() const {
    return config_;
  }

 private:
  std::unique_ptr<Operation> cloneImpl() const override;

  const VocabIndex& getVocabIndexOfFixedEntity() const {
    AD_CONTRACT_CHECK(hasFixedEntity());
    return std::get<FixedEntity>(config_.varOrFixed_.value().entity_).second;
  }

  Result computeResult([[maybe_unused]] bool requestLaziness) override;

  vector<QueryExecutionTree*> getChildren() override { return {}; }

  void setVariableToColumnMap();
};

#endif  // QLEVER_SRC_ENGINE_TEXTINDEXSCANFORENTITY_H
