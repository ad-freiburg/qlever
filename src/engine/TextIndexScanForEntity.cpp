#include "engine/TextIndexScanForEntity.h"

// _____________________________________________________________________________
TextIndexScanForEntity::TextIndexScanForEntity(
    QueryExecutionContext* qec, Variable textRecordVar,
    const std::variant<Variable, std::string>& entity, string word)
    : Operation(qec),
      textRecordVar_(std::move(textRecordVar)),
      entity_(VarOrFixedEntity(qec, entity)),
      word_(std::move(word)) {}

// _____________________________________________________________________________
ResultTable TextIndexScanForEntity::computeResult() {
  IdTable idTable = getExecutionContext()->getIndex().getEntityMentionsForWord(
      word_, getExecutionContext()->getAllocator());

  if (hasFixedEntity()) {
    auto beginErase = std::ranges::remove_if(
        idTable.begin(), idTable.end(), [this](const auto& row) {
          return row[1].getVocabIndex() != entity_.index_;
        });
    idTable.erase(beginErase.begin(), idTable.end());
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
  if (hasFixedEntity()) {
    addDefinedVar(
        textRecordVar_.getScoreVariable(entity_.fixedEntity_.value()));
  } else {
    addDefinedVar(entity_.entityVar_.value());
    addDefinedVar(textRecordVar_.getScoreVariable(entity_.entityVar_.value()));
  }
  return vcmap;
}

// _____________________________________________________________________________
size_t TextIndexScanForEntity::getResultWidth() const {
  return 2 + (hasFixedEntity() ? 0 : 1);
}

// _____________________________________________________________________________
size_t TextIndexScanForEntity::getCostEstimate() {
  if (hasFixedEntity()) {
    return 2 * getExecutionContext()->getIndex().getEntitySizeEstimate(word_);
  } else {
    return getExecutionContext()->getIndex().getEntitySizeEstimate(word_);
  }
}

// _____________________________________________________________________________
uint64_t TextIndexScanForEntity::getSizeEstimateBeforeLimit() {
  if (hasFixedEntity()) {
    return uint64_t(
        getExecutionContext()->getIndex().getAverageNofEntityContexts());
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
     << (hasFixedEntity() ? entity_.fixedEntity_.value() : "no fixed-entity")
     << " \"";
  return std::move(os).str();
}
