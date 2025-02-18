//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#include "engine/TextIndexScanForEntity.h"

// _____________________________________________________________________________
TextIndexScanForEntity::TextIndexScanForEntity(
    QueryExecutionContext* qec, TextIndexScanForEntityConfiguration config)
    : Operation(qec), config_(std::move(config)) {
  config_.varOrFixed_ = VarOrFixedEntity(qec, config_.entity_);
  setVariableToColumnMap();
}

// _____________________________________________________________________________
TextIndexScanForEntity::TextIndexScanForEntity(
    QueryExecutionContext* qec, Variable textRecordVar,
    std::variant<Variable, std::string> entity, string word)
    : Operation(qec),
      config_(TextIndexScanForEntityConfiguration{
          std::move(textRecordVar), std::move(entity), std::move(word)}) {
  config_.varToBindScore_ =
      config_.varToBindText_.getEntityScoreVariable(config_.entity_);
  config_.varOrFixed_ = VarOrFixedEntity(qec, config_.entity_);
  setVariableToColumnMap();
}

// _____________________________________________________________________________
ProtoResult TextIndexScanForEntity::computeResult(
    [[maybe_unused]] bool requestLaziness) {
  IdTable idTable = getExecutionContext()->getIndex().getEntityMentionsForWord(
      config_.word_, getExecutionContext()->getAllocator());

  if (hasFixedEntity()) {
    auto beginErase = ql::ranges::remove_if(idTable, [this](const auto& row) {
      return row[1].getVocabIndex() != getVocabIndexOfFixedEntity();
    });
#ifdef QLEVER_CPP_17
    idTable.erase(beginErase, idTable.end());
#else
    idTable.erase(beginErase.begin(), idTable.end());
#endif
    if (config_.varToBindScore_.has_value()) {
      idTable.setColumnSubset(std::vector<ColumnIndex>{0, 2});
    } else {
      idTable.setColumnSubset(std::vector<ColumnIndex>{0});
    }
  } else {
    if (!config_.varToBindScore_.has_value()) {
      idTable.setColumnSubset(std::vector<ColumnIndex>{0, 1});
    }
  }

  // Add details to the runtimeInfo. This is has no effect on the result.
  if (hasFixedEntity()) {
    runtimeInfo().addDetail("fixed entity: ", fixedEntity());
  } else {
    runtimeInfo().addDetail("entity var: ", entityVariable().name());
  }
  runtimeInfo().addDetail("word: ", config_.word_);

  return {std::move(idTable), resultSortedOn(), LocalVocab{}};
}

// _____________________________________________________________________________
void TextIndexScanForEntity::setVariableToColumnMap() {
  config_.variableColumns_ = VariableToColumnMap{};
  ColumnIndex index = ColumnIndex{0};
  config_.variableColumns_.value()[config_.varToBindText_] =
      makeAlwaysDefinedColumn(index);
  index++;
  if (!hasFixedEntity()) {
    config_.variableColumns_.value()[entityVariable()] =
        makeAlwaysDefinedColumn(index);
    index++;
  }
  if (config_.varToBindScore_.has_value()) {
    config_.variableColumns_.value()[config_.varToBindScore_.value()] =
        makeAlwaysDefinedColumn(index);
  }
}

// _____________________________________________________________________________
VariableToColumnMap TextIndexScanForEntity::computeVariableToColumnMap() const {
  return config_.variableColumns_.value();
}

// _____________________________________________________________________________
size_t TextIndexScanForEntity::getResultWidth() const {
  return 1 + (hasFixedEntity() ? 0 : 1) +
         (config_.varToBindScore_.has_value() ? 1 : 0);
}

// _____________________________________________________________________________
size_t TextIndexScanForEntity::getCostEstimate() {
  if (hasFixedEntity()) {
    // We currently have to first materialize and then filter the complete list
    // for the fixed entity
    return 2 * getExecutionContext()->getIndex().getSizeOfTextBlockForEntities(
                   config_.word_);
  } else {
    return getExecutionContext()->getIndex().getSizeOfTextBlockForEntities(
        config_.word_);
  }
}

// _____________________________________________________________________________
uint64_t TextIndexScanForEntity::getSizeEstimateBeforeLimit() {
  if (hasFixedEntity()) {
    return static_cast<uint64_t>(
        getExecutionContext()->getIndex().getAverageNofEntityContexts());
  } else {
    return getExecutionContext()->getIndex().getSizeOfTextBlockForEntities(
        config_.word_);
  }
}

// _____________________________________________________________________________
bool TextIndexScanForEntity::knownEmptyResult() {
  return getExecutionContext()->getIndex().getSizeOfTextBlockForEntities(
             config_.word_) == 0;
}

// _____________________________________________________________________________
vector<ColumnIndex> TextIndexScanForEntity::resultSortedOn() const {
  return {ColumnIndex(0)};
}

// _____________________________________________________________________________
string TextIndexScanForEntity::getDescriptor() const {
  return absl::StrCat("TextIndexScanForEntity on ",
                      config_.varToBindText_.name());
}

// _____________________________________________________________________________
string TextIndexScanForEntity::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "ENTITY INDEX SCAN FOR WORD: "
     << " with word: \"" << config_.word_ << "\" and fixed-entity: \""
     << (hasFixedEntity() ? fixedEntity() : "no fixed-entity") << " \"";
  return std::move(os).str();
}
