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
  using FixedEntity = std::pair<std::string, VocabIndex>;

  struct VarOrFixedEntity {
    std::variant<Variable, FixedEntity> entity_;

    static std::variant<Variable, FixedEntity> makeEntityVariant(
        const QueryExecutionContext* qec,
        std::variant<Variable, std::string> entity) {
      if (std::holds_alternative<std::string>(entity)) {
        VocabIndex index;
        std::string fixedEntity = std::move(std::get<std::string>(entity));
        bool success = qec->getIndex().getVocab().getId(fixedEntity, &index);
        if (!success) {
          throw std::runtime_error(
              "The entity " + fixedEntity +
              " is not part of the underlying knowledge graph and can "
              "therefore not be used as the object of ql:contains-entity");
        }
        return FixedEntity(std::move(fixedEntity), std::move(index));
      } else {
        return std::get<Variable>(entity);
      }
    };

    VarOrFixedEntity(const QueryExecutionContext* qec,
                     std::variant<Variable, std::string> entity)
        : entity_(makeEntityVariant(qec, std::move(entity))) {}

    ~VarOrFixedEntity() = default;

    bool hasFixedEntity() const {
      return std::holds_alternative<FixedEntity>(entity_);
    }
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

  const std::string& fixedEntity() const {
    AD_CONTRACT_CHECK(hasFixedEntity());
    return std::get<FixedEntity>(varOrFixed_.entity_).first;
  }

  const Variable& entityVariable() const {
    AD_CONTRACT_CHECK(!hasFixedEntity());
    return std::get<Variable>(varOrFixed_.entity_);
  }

  const Variable& textRecordVar() const { return textRecordVar_; }

  const std::string& word() const { return word_; }

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
    AD_CONTRACT_CHECK(hasFixedEntity());
    return std::get<FixedEntity>(varOrFixed_.entity_).second;
  }

  ResultTable computeResult() override;

  vector<QueryExecutionTree*> getChildren() override { return {}; }
};
