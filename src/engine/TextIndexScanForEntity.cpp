#include "engine/TextIndexScanForEntity.h"

// _____________________________________________________________________________
TextIndexScanForEntity::TextIndexScanForEntity(
    QueryExecutionContext* qec, Variable textRecordVar,
    std::variant<Variable, std::string> entity, string word)
    : Operation(qec),
      textRecordVar_(std::move(textRecordVar)),
      entity_(std::move(entity)),
      word_(std::move(word)),
      hasFixedEntity_(std::holds_alternative<std::string>(entity_)) {}

// _____________________________________________________________________________
ResultTable TextIndexScanForEntity::computeResult() {
  IdTable idTable = getExecutionContext()->getIndex().getEntityMentionsForWord(
      word_, getExecutionContext()->getAllocator());

  if (hasFixedEntity_) {
    VocabIndex idx;
    bool success = getExecutionContext()->getIndex().getVocab().getId(
        std::get<std::string>(entity_), &idx);
    if (!success) {
      throw std::runtime_error(
          "The entity " + std::get<string>(entity_) +
          " is not part of the underlying knowledge graph and can therefore "
          "not be used as the object of ql:contains-entity");
    }
    size_t i = 0;
    // Behaviour of IdTable::erase(iterator) is undefined, if the iterator
    // points to the last element. So we have to check that case separatly.
    if (idTable.getColumn(1)[idTable.numRows() - 1].getVocabIndex() == idx) {
      i++;
    }
    idTable.erase(remove_if(idTable.begin(), idTable.end().operator--(),
                            [&](const auto& row) {
                              if (row[1].getVocabIndex() == idx) {
                                i++;
                                return false;
                              }
                              return true;
                              ;
                            }));
    idTable.setColumnSubset(std::vector<ColumnIndex>{0, 2});
    idTable.resize(i);
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
    string s = std::move(std::get<std::string>(entity_));
    s.erase(remove_if(s.begin(), s.end(), std::not1(std::ptr_fun(isalpha))),
            s.end());
    Variable fixedEntityVar{"?" + s};
    addDefinedVar(fixedEntityVar.getScoreVariable());
  } else {
    addDefinedVar(std::get<Variable>(entity_));
    addDefinedVar(std::get<Variable>(entity_).getScoreVariable());
  }
  return vcmap;
}

// _____________________________________________________________________________
size_t TextIndexScanForEntity::getResultWidth() const {
  return 2 + !hasFixedEntity_;
}

// _____________________________________________________________________________
vector<ColumnIndex> TextIndexScanForEntity::resultSortedOn() const {
  return {ColumnIndex(0)};
}

// _____________________________________________________________________________
string TextIndexScanForEntity::getDescriptor() const {
  return absl::StrCat(
      "TextIndexScanForEntity on text-variable ", textRecordVar_.name(),
      " and ",
      (hasFixedEntity_
           ? ("fixed-entity " + std::get<std::string>(entity_))
           : ("entity-variable " + std::get<Variable>(entity_).name())),
      " with word ", word_);
}

// _____________________________________________________________________________
string TextIndexScanForEntity::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "ENTITY INDEX SCAN FOR WORD: "
     << " with word: \"" << word_ << "\" and fixed-entity: \""
     << (hasFixedEntity_ ? std::get<std::string>(entity_) : "no fixed-entity")
     << " \"";
  ;
  return std::move(os).str();
}
