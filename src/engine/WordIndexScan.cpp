#include "engine/WordIndexScan.h"

// _____________________________________________________________________________
WordIndexScan::WordIndexScan(QueryExecutionContext* qec, Variable var,
                             string word)
    : Operation(qec), var_(var), word_(word) {
  if (word_.ends_with('*')) {
    isPrefix_ = true;
  }
}

// _____________________________________________________________________________
ResultTable WordIndexScan::computeResult() {
  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(3 + isPrefix_);
  Index::WordEntityPostings wep =
      getExecutionContext()->getIndex().getEntityPostingsForTerm(word_);
  idTable.resize(wep.cids_.size());
  int i = 0;
  decltype(auto) cidColumn = idTable.getColumn(i++);
  decltype(auto) widColumn = idTable.getColumn(i);
  i += isPrefix_;
  decltype(auto) eidColumn = idTable.getColumn(i++);
  decltype(auto) scoreColumn = idTable.getColumn(i++);

  eidColumn = wep.eids_;
  for (size_t k = 0; k < wep.cids_.size(); k++) {
    cidColumn[k] = Id::makeFromTextRecordIndex(wep.cids_[k]);
    scoreColumn[k] = Id::makeFromInt(wep.scores_[k]);
    if (isPrefix_) {
      widColumn[k] = Id::makeFromWordVocabIndex(WordVocabIndex::make(wep.wids_[0][k]));
    }
  }

  return {std::move(idTable), resultSortedOn(), LocalVocab{}};;
}

// _____________________________________________________________________________
VariableToColumnMap WordIndexScan::computeVariableToColumnMap() const {
  VariableToColumnMap vcmap;
  auto addDefinedVar = [&vcmap,
                        index = ColumnIndex{0}](const Variable& var) mutable {
    vcmap[var] = makeAlwaysDefinedColumn(index);
    ++index;
  };
  addDefinedVar(var_);
  if (isPrefix_) {
    addDefinedVar(var_.getMatchingWordVariable(word_));
  }
  addDefinedVar(var_.getEntityVariable());
  addDefinedVar(var_.getScoreVariable());
  return vcmap;
}

// _____________________________________________________________________________
size_t WordIndexScan::getResultWidth() const { return 3 + isPrefix_; }

// _____________________________________________________________________________
vector<ColumnIndex> WordIndexScan::resultSortedOn() const {
  return {ColumnIndex(0)};
}

// _____________________________________________________________________________
string WordIndexScan::getDescriptor() const {
  return "WordIndexScan on " + var_.name() + " with word " + word_;
}

// _____________________________________________________________________________
string WordIndexScan::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "WORD INDEX SCAN: "
     << " with word: \"" << word_ << "\" and variable: \"" << var_.name()
     << " \"";
  ;
  return std::move(os).str();
}
