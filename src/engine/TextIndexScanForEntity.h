#pragma once

#include <string>

#include "./Operation.h"

// This operation retrieves all text records and their corresponding
// entities from the fulltext index that contain a certain word or prefix.
// The entities are saved to the entityVar_. If the operation is called on a
// fixed entity instead, it only returns entries that contain this entity.
class TextIndexScanForEntity : public Operation {
 public:
  union VarOrFixedEntity {
    VarOrFixedEntity(const QueryExecutionContext* qec,
                     std::variant<Variable, std::string> entity) {
      if (std::holds_alternative<std::string>(entity)) {
        fixedEntity_.first = std::get<std::string>(entity);
        bool success = qec->getIndex().getVocab().getId(fixedEntity_.first,
                                                        &fixedEntity_.second);
        if (!success) {
          throw std::runtime_error(
              "The entity " + fixedEntity_.first +
              " is not part of the underlying knowledge graph and can "
              "therefore "
              "not be used as the object of ql:contains-entity");
        }
      } else {
        entityVar_ = std::get<Variable>(entity);
      }
    }
    ~VarOrFixedEntity() {}

    std::pair<std::string, VocabIndex> fixedEntity_;
    Variable entityVar_;
  };

 private:
  const Variable textRecordVar_;
  const VarOrFixedEntity entity_;
  const string word_;
  const bool hasFixedEntity_;

 public:
  TextIndexScanForEntity(QueryExecutionContext* qec, Variable textRecordVar,
                         std::variant<Variable, std::string> entity_,
                         string word);
  virtual ~TextIndexScanForEntity() = default;

  vector<QueryExecutionTree*> getChildren() override { return {}; }

  Variable textRecordVar() const { return textRecordVar_; }

  std::variant<Variable, std::string> entity() const {
    if (hasFixedEntity_) {
      return entity_.fixedEntity_.first;
    } else {
      return entity_.entityVar_;
    }
  }

  std::string word() const { return word_; }

  string getCacheKeyImpl() const override;

  string getDescriptor() const override;

  size_t getResultWidth() const override;

  void setTextLimit(size_t) override {}

  size_t getCostEstimate() override;

  uint64_t getSizeEstimateBeforeLimit() override;

  float getMultiplicity(size_t col) override {
    (void)col;
    return 1;
  }

  bool knownEmptyResult() override;

  vector<ColumnIndex> resultSortedOn() const override;

  ResultTable computeResult() override;

  VariableToColumnMap computeVariableToColumnMap() const override;
};
