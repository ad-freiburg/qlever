#include "engine/TextIndexScanForWord.h"

// _____________________________________________________________________________
TextIndexScanForWord::TextIndexScanForWord(QueryExecutionContext* qec,
                                           Variable textRecordVar, string word)
    : Operation(qec),
      textRecordVar_(std::move(textRecordVar)),
      word_(std::move(word)),
      isPrefix_(word_.ends_with('*')) {}

// _____________________________________________________________________________
ResultTable TextIndexScanForWord::computeResult() {
  IdTable idTable = getExecutionContext()->getIndex().getWordPostingsForTerm(
      word_, getExecutionContext()->getAllocator());

  if (!isPrefix_) {
    IdTable smallIdTable{getExecutionContext()->getAllocator()};
    smallIdTable.setNumColumns(1);
    smallIdTable.resize(idTable.numRows());
    std::ranges::copy(idTable.getColumn(0), smallIdTable.getColumn(0).begin());

    return {std::move(smallIdTable), resultSortedOn(), LocalVocab{}};
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
    addDefinedVar(textRecordVar_.getMatchingWordVariable(
        word_.substr(0, word_.size() - 1)));
  }
  return vcmap;
}

// _____________________________________________________________________________
size_t TextIndexScanForWord::getResultWidth() const { return 1 + isPrefix_; }

// _____________________________________________________________________________
size_t TextIndexScanForWord::getSizeEstimateBeforeLimit() {
  return getExecutionContext()->getIndex().getTextRecordSizeEstimate(word_);
}

// _____________________________________________________________________________
vector<ColumnIndex> TextIndexScanForWord::resultSortedOn() const {
  return {ColumnIndex(0)};
}

// _____________________________________________________________________________
string TextIndexScanForWord::getDescriptor() const {
  return absl::StrCat("TextIndexScanForWord on ", textRecordVar_.name(),
                      " with word ", word_);
}

// _____________________________________________________________________________
string TextIndexScanForWord::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "WORD INDEX SCAN: "
     << " with word: \"" << word_ << "\"";
  return std::move(os).str();
}
