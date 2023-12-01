#include "engine/EntityIndexScanForWord.h"

// _____________________________________________________________________________
EntityIndexScanForWord::EntityIndexScanForWord(
    QueryExecutionContext* qec, Variable textRecordVar,
    std::optional<Variable> entityVar, string word,
    std::optional<string> fixedEntity /*=std::nullopt*/)
    : Operation(qec),
      qec_(std::move(qec)),
      textRecordVar_(std::move(textRecordVar)),
      entityVar_(std::move(entityVar)),
      word_(std::move(word)),
      fixedEntity_(std::move(fixedEntity)),
      hasFixedEntity_(fixedEntity.has_value()) {}

// _____________________________________________________________________________
ResultTable EntityIndexScanForWord::computeResult() {
  IdTable idTable = getExecutionContext()->getIndex().getEntityMentionsForWord(
      word_, getExecutionContext()->getAllocator());

  if (hasFixedEntity_) {
    VocabIndex idx;
    bool success =
        qec_->getIndex().getVocab().getId(fixedEntity_.value(), &idx);
    if (!success) {
      LOG(ERROR) << "ERROR: entity \"" << fixedEntity_.value() << "\" "
                 << "not found in Vocab. Terminating\n";
      AD_FAIL();
    }
    IdTable filteredIdTable{getExecutionContext()->getAllocator()};
    filteredIdTable.setNumColumns(2);
    filteredIdTable.resize(idTable.getColumn(1).size());
    size_t j = 0;
    for (size_t i = 0; i < idTable.getColumn(1).size(); i++) {
      if (idTable.getColumn(1)[i].getVocabIndex() == idx) {
        filteredIdTable.getColumn(0)[j] = idTable.getColumn(0)[i];
        filteredIdTable.getColumn(1)[j] = idTable.getColumn(2)[i];
        j++;
      }
    }
    filteredIdTable.resize(j);
    idTable = std::move(filteredIdTable);
  }

  return {std::move(idTable), resultSortedOn(), LocalVocab{}};
}

// _____________________________________________________________________________
VariableToColumnMap EntityIndexScanForWord::computeVariableToColumnMap() const {
  VariableToColumnMap vcmap;
  auto addDefinedVar = [&vcmap,
                        index = ColumnIndex{0}](const Variable& var) mutable {
    vcmap[var] = makeAlwaysDefinedColumn(index);
    ++index;
  };
  addDefinedVar(textRecordVar_);
  if (hasFixedEntity_) {
    string s = std::move(fixedEntity_.value());
    s.erase(remove_if(s.begin(), s.end(), std::not1(std::ptr_fun(isalpha))),
            s.end());
    Variable fixedEntityVar{"?" + s};
    addDefinedVar(fixedEntityVar.getScoreVariable());
  } else {
    addDefinedVar(entityVar_.value());
    addDefinedVar(entityVar_.value().getScoreVariable());
  }
  return vcmap;
}

// _____________________________________________________________________________
size_t EntityIndexScanForWord::getResultWidth() const {
  return 2 + !hasFixedEntity_;
}

// _____________________________________________________________________________
vector<ColumnIndex> EntityIndexScanForWord::resultSortedOn() const {
  return {ColumnIndex(0)};
}

// _____________________________________________________________________________
string EntityIndexScanForWord::getDescriptor() const {
  return "EntityIndexScanForWord on text-variable " + textRecordVar_.name() +
         " and " +
         (hasFixedEntity_ ? ("fixed-entity " + fixedEntity_.value())
                          : ("entity-variable " + entityVar_.value().name())) +
         " with word " + word_;
}

// _____________________________________________________________________________
string EntityIndexScanForWord::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "ENTITY INDEX SCAN FOR WORD: "
     << " with word: \"" << word_ << "\" and fixed-entity: \""
     << (hasFixedEntity_ ? fixedEntity_.value() : "no fixed-entity") << " \"";
  ;
  return std::move(os).str();
}
