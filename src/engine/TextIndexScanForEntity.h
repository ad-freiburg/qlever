//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#pragma once

#include <string>

#include "./Operation.h"

// This operation retrieves all text records and their corresponding
// entities from the fulltext index that contain a certain word or prefix.
// The entities are saved to the entityVar_. If the operation is called on a
// fixed entity instead, it only returns entries that contain this entity.
class TextIndexScanForEntity : public Operation {
 private:
  struct VarOrFixedEntity {
    VarOrFixedEntity(const QueryExecutionContext* qec,
                     std::variant<Variable, std::string> entity) {
      if (std::holds_alternative<std::string>(entity)) {
        VocabIndex index;
        std::string fixedEntity = std::move(*std::get_if<std::string>(&entity));
        bool success = qec->getIndex().getVocab().getId(fixedEntity, &index);
        if (!success) {
          throw std::runtime_error(
              "The entity " + fixedEntity +
              " is not part of the underlying knowledge graph and can "
              "therefore not be used as the object of ql:contains-entity");
        }
        entity_ = std::pair<std::string, VocabIndex>(std::move(fixedEntity),
                                                     std::move(index));
      } else {
        entity_ = std::move(*std::get_if<Variable>(&entity));
      }
    }
    ~VarOrFixedEntity() = default;

    bool hasFixedEntity() const {
      return std::holds_alternative<std::pair<std::string, VocabIndex>>(
          entity_);
    }

    std::variant<Variable, std::pair<std::string, VocabIndex>> entity_ =
        std::pair<std::string, VocabIndex>{std::string{}, VocabIndex{}};
  };

  const Variable textRecordVar_;
  const VarOrFixedEntity varOrFixed_;
  const string word_;

 public:
  TextIndexScanForEntity(QueryExecutionContext* qec, Variable textRecordVar,
                         std::variant<Variable, std::string> entity,
                         string word);
  ~TextIndexScanForEntity() override = default;

  bool hasFixedEntity() const { return varOrFixed_.hasFixedEntity(); }

  const std::string& getFixedEntity() const {
    if (!hasFixedEntity()) {
      AD_CONTRACT_CHECK(
          "getFixedEntity called on an instance that does not have a fixed "
          "entity.");
    }
    return std::get<std::pair<std::string, VocabIndex>>(varOrFixed_.entity_)
        .first;
  }

  const Variable& getEntityVariable() const {
    if (hasFixedEntity()) {
      AD_CONTRACT_CHECK(
          "getEntityVariable called on an instance that does not have a entity "
          "variable.");
    }
    return std::get<Variable>(varOrFixed_.entity_);
  }

  const Variable& getTextRecordVar() const { return textRecordVar_; }

  const std::string& getWord() const { return word_; }

  string getCacheKeyImpl() const override;

  string getDescriptor() const override;

  size_t getResultWidth() const override;

  void setTextLimit(size_t) override {
    // TODO: implement textLimit
  }

  size_t getCostEstimate() override;

  uint64_t getSizeEstimateBeforeLimit() override;

  float getMultiplicity(size_t col) override {
    (void)col;
    return 1;
  }

  bool knownEmptyResult() override;

  vector<ColumnIndex> resultSortedOn() const override;

  VariableToColumnMap computeVariableToColumnMap() const override;

 private:
  const VocabIndex& getVocabIndexOfFixedEntity() const {
    if (!hasFixedEntity()) {
      AD_CONTRACT_CHECK(
          "getVocabIndexOfFixedEntity called on an instance that does not have "
          "a fixed entity and therefore also no VocabIndex.");
    }
    return std::get<std::pair<std::string, VocabIndex>>(varOrFixed_.entity_)
        .second;
  }

  ResultTable computeResult() override;

  vector<QueryExecutionTree*> getChildren() override { return {}; }
};
