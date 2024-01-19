//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#include "engine/TextIndexScanForEntity.h"

// _____________________________________________________________________________
TextIndexScanForEntity::TextIndexScanForEntity(
    QueryExecutionContext* qec, Variable textRecordVar,
    std::variant<Variable, std::string> entity, string word)
    : Operation(qec),
      textRecordVar_(std::move(textRecordVar)),
      varOrFixed_(qec, std::move(entity)),
      word_(std::move(word)) {}

// _____________________________________________________________________________
ResultTable TextIndexScanForEntity::computeResult() {
  IdTable idTable = getExecutionContext()->getIndex().getEntityMentionsForWord(
      word_, getExecutionContext()->getAllocator());

  if (hasFixedEntity()) {
    auto beginErase = std::ranges::remove_if(idTable, [this](const auto& row) {
      return row[1].getVocabIndex() != getVocabIndexOfFixedEntity();
    });
    idTable.erase(beginErase.begin(), idTable.end());
    idTable.setColumnSubset(std::vector<ColumnIndex>{0, 2});
  }

  // Add details to the runtimeInfo. This is has no effect on the result.
  if (hasFixedEntity()) {
    runtimeInfo().addDetail("fixed entity: ", fixedEntity());
  } else {
    runtimeInfo().addDetail("entity var: ", entityVariable().name());
  }
  runtimeInfo().addDetail("word: ", word_);

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
    addDefinedVar(textRecordVar_.getScoreVariable(fixedEntity()));
  } else {
    addDefinedVar(entityVariable());
    addDefinedVar(textRecordVar_.getScoreVariable(entityVariable()));
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
    // We currently have to first materialize and then filter the complete list
    // for the fixed entity
    return 2 * getExecutionContext()->getIndex().getSizeOfTextBlockForEntities(
                   word_);
  } else {
    return getExecutionContext()->getIndex().getSizeOfTextBlockForEntities(
        word_);
  }
}

// _____________________________________________________________________________
uint64_t TextIndexScanForEntity::getSizeEstimateBeforeLimit() {
  if (hasFixedEntity()) {
    return static_cast<uint64_t>(
        getExecutionContext()->getIndex().getAverageNofEntityContexts());
  } else {
    return getExecutionContext()->getIndex().getSizeOfTextBlockForEntities(
        word_);
  }
}

// _____________________________________________________________________________
bool TextIndexScanForEntity::knownEmptyResult() {
  return getExecutionContext()->getIndex().getSizeOfTextBlockForEntities(
             word_) == 0;
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
     << (hasFixedEntity() ? fixedEntity() : "no fixed-entity") << " \"";
  return std::move(os).str();
}
