#pragma once

#include <string>

#include "./Operation.h"

// This operation retrieves all text records and their corresponding
// entities from the fulltext index that contain a certain word or prefix.
// The entities are saved to the entityVar_. If the operation is called on a
// fixed entity instead, it only returns entries that contain this entity.
class TextIndexScanForEntity : public Operation {
 public:
  struct VarOrFixedEntity {
    VarOrFixedEntity(const QueryExecutionContext* qec,
                     const std::variant<Variable, std::string>& entity) {
      if (std::holds_alternative<std::string>(entity)) {
        fixedEntity_.emplace(std::get<std::string>(entity));
        bool success =
            qec->getIndex().getVocab().getId(fixedEntity_.value(), &index_);
        if (!success) {
          throw std::runtime_error(
              "The entity " + fixedEntity_.value() +
              " is not part of the underlying knowledge graph and can "
              "therefore "
              "not be used as the object of ql:contains-entity");
        }
      } else {
        entityVar_.emplace(std::get<Variable>(entity));
      }
    }
    ~VarOrFixedEntity() = default;

    std::optional<std::string> fixedEntity_ = std::nullopt;
    VocabIndex index_;
    std::optional<Variable> entityVar_ = std::nullopt;
  };

 private:
  const Variable textRecordVar_;
  const VarOrFixedEntity entity_;
  const string word_;

 public:
  TextIndexScanForEntity(QueryExecutionContext* qec, Variable textRecordVar,
                         const std::variant<Variable, std::string>& entity_,
                         string word);
  ~TextIndexScanForEntity() override = default;

  bool hasFixedEntity() const { return entity_.fixedEntity_.has_value(); };

  Variable textRecordVar() const { return textRecordVar_; }

  std::variant<Variable, std::string> entity() const {
    if (hasFixedEntity()) {
      return entity_.fixedEntity_.value();
    } else {
      return entity_.entityVar_.value();
    }
  }

  std::string word() const { return word_; }

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
  ResultTable computeResult() override;

  vector<QueryExecutionTree*> getChildren() override { return {}; }
};
