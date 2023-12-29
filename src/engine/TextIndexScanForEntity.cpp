#include "engine/TextIndexScanForEntity.h"

// _____________________________________________________________________________
TextIndexScanForEntity::TextIndexScanForEntity(
    QueryExecutionContext* qec, Variable textRecordVar,
    std::variant<Variable, std::string> entity, string word)
    : Operation(qec),
      textRecordVar_(std::move(textRecordVar)),
      entity_(VarOrFixedEntity(qec, entity)),
      word_(std::move(word)),
      hasFixedEntity_(std::holds_alternative<std::string>(entity)) {}

// _____________________________________________________________________________
ResultTable TextIndexScanForEntity::computeResult() {
  IdTable idTable = getExecutionContext()->getIndex().getEntityMentionsForWord(
      word_, getExecutionContext()->getAllocator());

  if (hasFixedEntity_) {
    auto beginErase =
        remove_if(idTable.begin(), idTable.end(), [this](const auto& row) {
          return row[1].getVocabIndex() != entity_.fixedEntity_.second;
        });
    idTable.erase(beginErase, idTable.end());
    idTable.setColumnSubset(std::vector<ColumnIndex>{0, 2});
  }

  return {std::move(idTable), resultSortedOn(), LocalVocab{}};
}

// _____________________________________________________________________________
VariableToColumnMap TextIndexScanForEntity::computeVariableToColumnMap() const {
  VariableToColumnMap vcmap;
  auto addDefinedVar = [&vcmap,
                        index = ColumnIndex{0}](const Variable& var) mutable {
    vcmap[var] = makeAlwaysDefinedColumn(index);
    ++index;
  };
  addDefinedVar(textRecordVar_);
  if (hasFixedEntity_) {
    addDefinedVar(textRecordVar_.getScoreVariable(
        entity_.fixedEntity_.first));
  } else {
    addDefinedVar(entity_.entityVar_);
    addDefinedVar(textRecordVar_.getScoreVariable(entity_.entityVar_));
  }
  return vcmap;
}

// _____________________________________________________________________________
size_t TextIndexScanForEntity::getResultWidth() const {
  return 2 + !hasFixedEntity_;
}

// _____________________________________________________________________________
size_t TextIndexScanForEntity::getCostEstimate() {
  if (hasFixedEntity_) {
    return 2 * getExecutionContext()->getIndex().getEntitySizeEstimate(word_);
  } else {
    return getExecutionContext()->getIndex().getEntitySizeEstimate(word_);
  }
}

// _____________________________________________________________________________
size_t TextIndexScanForEntity::getSizeEstimateBeforeLimit() {
  if (hasFixedEntity_) {
    return getExecutionContext()->getIndex().getAverageNofEntityContexts();
  } else {
    return getExecutionContext()->getIndex().getEntitySizeEstimate(word_);
  }
}

// _____________________________________________________________________________
bool TextIndexScanForEntity::knownEmptyResult() {
  return getExecutionContext()->getIndex().getSizeEstimate(word_) == 0;
}

// _____________________________________________________________________________
vector<ColumnIndex> TextIndexScanForEntity::resultSortedOn() const {
  return {ColumnIndex(0)};
}

// _____________________________________________________________________________
string TextIndexScanForEntity::getDescriptor() const {
  return absl::StrCat("TextIndexScanForEntity on ", textRecordVar_.name());
}

// _____________________________________________________________________________
string TextIndexScanForEntity::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "ENTITY INDEX SCAN FOR WORD: "
     << " with word: \"" << word_ << "\" and fixed-entity: \""
     << (hasFixedEntity_ ? entity_.fixedEntity_.first
                          : "no fixed-entity")
     << " \"";
  ;
  return std::move(os).str();
}
