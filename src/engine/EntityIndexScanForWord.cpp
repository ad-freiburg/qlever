#include "engine/EntityIndexScanForWord.h"

// _____________________________________________________________________________
EntityIndexScanForWord::EntityIndexScanForWord(QueryExecutionContext* qec,
                                               Variable cvar, Variable evar,
                                               string word)
    : Operation(qec),
      cvar_(std::move(cvar)),
      evar_(std::move(evar)),
      word_(std::move(word)) {}

// _____________________________________________________________________________
ResultTable EntityIndexScanForWord::computeResult() {
  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(getResultWidth());
  Index::WordEntityPostings wep =
      getExecutionContext()->getIndex().getUnadjustedEntityPostingsForTerm(
          word_);

  idTable.resize(wep.cids_.size());
  decltype(auto) cidColumn = idTable.getColumn(0);
  std::ranges::transform(wep.cids_, cidColumn.begin(),
                         &Id::makeFromTextRecordIndex);
  decltype(auto) eidColumn = idTable.getColumn(1);
  std::ranges::copy(wep.eids_.begin(), wep.eids_.end(), eidColumn.begin());

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
  addDefinedVar(cvar_);
  addDefinedVar(evar_);
  return vcmap;
}

// _____________________________________________________________________________
size_t EntityIndexScanForWord::getResultWidth() const { return 2; }

// _____________________________________________________________________________
vector<ColumnIndex> EntityIndexScanForWord::resultSortedOn() const {
  return {ColumnIndex(0)};
}

// _____________________________________________________________________________
string EntityIndexScanForWord::getDescriptor() const {
  return "EntityIndexScanForWord on text-variable " + cvar_.name() +
         " and entity-variable " + evar_.name() + " with word " + word_;
}

// _____________________________________________________________________________
string EntityIndexScanForWord::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "ENTITY INDEX SCAN FOR WORD: "
     << " with word: \"" << word_ << "\" and text-variable: \"" << cvar_.name()
     << "\" and entity-variable: \"" << evar_.name() << " \"";
  ;
  return std::move(os).str();
}
