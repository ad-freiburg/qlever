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
  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(getResultWidth());
  Index::WordEntityPostings wep =
      getExecutionContext()->getIndex().getUnadjustedEntityPostingsForTerm(
          word_);

  if (hasFixedEntity_) {
    VocabIndex idx;
    bool success =
        qec_->getIndex().getVocab().getId(fixedEntity_.value(), &idx);
    if (!success) {
      LOG(ERROR) << "ERROR: entity \"" << fixedEntity_.value() << "\" "
                 << "not found in Vocab. Terminating\n";
      AD_FAIL();
    }
    Index::WordEntityPostings filteredWep;
    for (size_t i = 0; i < wep.eids_.size(); i++) {
      if (wep.eids_[i].getVocabIndex() == idx) {
        filteredWep.cids_.push_back(wep.cids_[i]);
        filteredWep.eids_.push_back(wep.eids_[i]);
        filteredWep.scores_.push_back(wep.scores_[i]);
      }
    }
    wep = std::move(filteredWep);
  }
  idTable.resize(wep.cids_.size());
  decltype(auto) cidColumn = idTable.getColumn(0);
  std::ranges::transform(wep.cids_, cidColumn.begin(),
                         &Id::makeFromTextRecordIndex);
  if (!hasFixedEntity_) {
    decltype(auto) eidColumn = idTable.getColumn(1);
    std::ranges::copy(wep.eids_, eidColumn.begin());
  }
  decltype(auto) scoreColumn = idTable.getColumn(1 + !hasFixedEntity_);
  std::ranges::transform(wep.scores_, scoreColumn.begin(), &Id::makeFromInt);

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
    s.erase(remove_if(s.begin(), s.end(), isalpha), s.end());
    Variable fixedEntityVar{s};
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
