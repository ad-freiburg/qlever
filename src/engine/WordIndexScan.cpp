#include "engine/WordIndexScan.h"

// _____________________________________________________________________________
WordIndexScan::WordIndexScan(QueryExecutionContext* qec,
                             SetOfVariables variables, Variable cvar,
                             string word)
    : Operation(qec),
      variables_(std::move(variables)),
      cvar_(std::move(cvar)),
      word_(word) {
  if (word_.ends_with('*')) {
    isPrefix_ = true;
  }
}

// _____________________________________________________________________________
ResultTable WordIndexScan::computeResult() {
  LOG(INFO) << "words compute: " << word_ << endl;
  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(1 + variables_.size() + isPrefix_);
  Index::WordEntityPostings wep =
      getExecutionContext()->getIndex().getEntityPostingsForTerm(word_);
  idTable.resize(wep.cids_.size());
  int i = 0;
  decltype(auto) cidColumn = idTable.getColumn(i++);
  std::ranges::transform(wep.cids_, cidColumn.begin(),
                         &Id::makeFromTextRecordIndex);
  if (isPrefix_) {
    decltype(auto) widColumn = idTable.getColumn(i++);
    std::ranges::transform(wep.wids_[0], widColumn.begin(), [](WordIndex id) {
      return Id::makeFromWordVocabIndex(WordVocabIndex::make(id));
    });
  }
  decltype(auto) scoreColumn = idTable.getColumn(i++);
  std::ranges::transform(wep.scores_, scoreColumn.begin(), &Id::makeFromInt);
  if (variables_.size() - 1 == 1) {
    decltype(auto) eidColumn = idTable.getColumn(i++);
    std::ranges::copy(wep.eids_.begin(), wep.eids_.end(), eidColumn.begin());
  } else if (variables_.size() - 1 >= 2) {
    // TODO: implement mult var case
  }

  return {std::move(idTable), resultSortedOn(), LocalVocab{}};
}

// _____________________________________________________________________________
VariableToColumnMap WordIndexScan::computeVariableToColumnMap() const {
  VariableToColumnMap vcmap;
  auto addDefinedVar = [&vcmap,
                        index = ColumnIndex{0}](const Variable& var) mutable {
    vcmap[var] = makeAlwaysDefinedColumn(index);
    ++index;
  };
  addDefinedVar(cvar_);
  if (isPrefix_) {
    addDefinedVar(
        cvar_.getMatchingWordVariable(word_.substr(0, word_.size() - 1)));
  }
  addDefinedVar(cvar_.getScoreVariable());
  for (const auto& var : variables_) {
    if (var != cvar_) {
      addDefinedVar(var);
    }
  }
  return vcmap;
}

// _____________________________________________________________________________
size_t WordIndexScan::getResultWidth() const {
  return 1 + variables_.size() + isPrefix_;
}

// _____________________________________________________________________________
vector<ColumnIndex> WordIndexScan::resultSortedOn() const {
  return {ColumnIndex(0)};
}

// _____________________________________________________________________________
string WordIndexScan::getDescriptor() const {
  return "WordIndexScan on " + cvar_.name() + " with word " + word_;
}

// _____________________________________________________________________________
string WordIndexScan::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "WORD INDEX SCAN: "
     << " with word: \"" << word_ << "\" and variable: \"" << cvar_.name()
     << " \"";
  ;
  return std::move(os).str();
}
