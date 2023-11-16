#include "engine/TextIndexScanForWord.h"

// _____________________________________________________________________________
TextIndexScanForWord::TextIndexScanForWord(QueryExecutionContext* qec,
                                           Variable cvar, string word)
    : Operation(qec),
      textRecordVar_(std::move(cvar)),
      word_(std::move(word)),
      isPrefix_(word_.ends_with('*')) {}

// _____________________________________________________________________________
ResultTable TextIndexScanForWord::computeResult() {
  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(getResultWidth());
  Index::WordEntityPostings wep =
      getExecutionContext()->getIndex().getWordPostingsForTerm(word_);
  idTable.resize(wep.cids_.size());
  decltype(auto) cidColumn = idTable.getColumn(0);
  std::ranges::transform(wep.cids_, cidColumn.begin(),
                         &Id::makeFromTextRecordIndex);
  if (isPrefix_) {
    decltype(auto) widColumn = idTable.getColumn(1);
    std::ranges::transform(wep.wids_[0], widColumn.begin(), [](WordIndex id) {
      return Id::makeFromWordVocabIndex(WordVocabIndex::make(id));
    });
  }

  return {std::move(idTable), resultSortedOn(), LocalVocab{}};
}

// _____________________________________________________________________________
VariableToColumnMap TextIndexScanForWord::computeVariableToColumnMap() const {
  VariableToColumnMap vcmap;
  auto addDefinedVar = [&vcmap,
                        index = ColumnIndex{0}](const Variable& var) mutable {
    vcmap[var] = makeAlwaysDefinedColumn(index);
    ++index;
  };
  addDefinedVar(textRecordVar_);
  if (isPrefix_) {
    addDefinedVar(
        textRecordVar_.getMatchingWordVariable(word_.substr(0, word_.size() - 1)));
  }
  return vcmap;
}

// _____________________________________________________________________________
size_t TextIndexScanForWord::getResultWidth() const { return 1 + isPrefix_; }

// _____________________________________________________________________________
vector<ColumnIndex> TextIndexScanForWord::resultSortedOn() const {
  return {ColumnIndex(0)};
}

// _____________________________________________________________________________
string TextIndexScanForWord::getDescriptor() const {
  return "TextIndexScanForWord on " + textRecordVar_.name() + " with word " + word_;
}

// _____________________________________________________________________________
string TextIndexScanForWord::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "WORD INDEX SCAN: "
     << " with word: \"" << word_ << "\" and variable: \"" << textRecordVar_.name()
     << " \"";
  ;
  return std::move(os).str();
}
