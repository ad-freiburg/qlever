//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#include "engine/TextIndexScanForWord.h"

// _____________________________________________________________________________
TextIndexScanForWord::TextIndexScanForWord(
    QueryExecutionContext* qec, TextIndexScanForWordConfiguration config)
    : Operation(qec), config_(std::move(config)) {
  config_.isPrefix_ = config_.word_.ends_with('*');
  setVariableToColumnMap();
}

// _____________________________________________________________________________
TextIndexScanForWord::TextIndexScanForWord(QueryExecutionContext* qec,
                                           Variable textRecordVar, string word)
    : Operation(qec),
      config_(TextIndexScanForWordConfiguration{std::move(textRecordVar),
                                                std::move(word)}) {
  config_.isPrefix_ = config_.word_.ends_with('*');
  config_.varToBindScore_ = config_.varToBindText_.getWordScoreVariable(
      config_.word_, config_.isPrefix_);
  setVariableToColumnMap();
}

// _____________________________________________________________________________
ProtoResult TextIndexScanForWord::computeResult(
    [[maybe_unused]] bool requestLaziness) {
  IdTable idTable = getExecutionContext()->getIndex().getWordPostingsForTerm(
      config_.word_, getExecutionContext()->getAllocator());

  // This filters out the word column. When the searchword is a prefix this
  // column shows the word the prefix got extended to
  using CI = ColumnIndex;
  if (!config_.isPrefix_) {
    if (config_.varToBindScore_.has_value()) {
      idTable.setColumnSubset(std::array{CI{0}, CI{2}});
    } else {
      idTable.setColumnSubset(std::array{CI{0}});
    }
    return {std::move(idTable), resultSortedOn(), LocalVocab{}};
  }
  if (!config_.varToBindScore_.has_value()) {
    idTable.setColumnSubset(std::array{CI{0}, CI{1}});
  }

  // Add details to the runtimeInfo. This is has no effect on the result.
  runtimeInfo().addDetail("word: ", config_.word_);

  return {std::move(idTable), resultSortedOn(), LocalVocab{}};
}

// _____________________________________________________________________________
void TextIndexScanForWord::setVariableToColumnMap() {
  config_.variableColumns_ = VariableToColumnMap{};
  ColumnIndex index = ColumnIndex{0};
  config_.variableColumns_.value()[config_.varToBindText_] =
      makeAlwaysDefinedColumn(index);
  index++;
  if (config_.isPrefix_) {
    if (!config_.varToBindMatch_.has_value()) {
      config_.varToBindMatch_ = config_.varToBindText_.getMatchingWordVariable(
          std::string_view(config_.word_).substr(0, config_.word_.size() - 1));
    }
    config_.variableColumns_.value()[config_.varToBindMatch_.value()] =
        makeAlwaysDefinedColumn(index);
    index++;
  }
  if (config_.varToBindScore_.has_value()) {
    config_.variableColumns_.value()[config_.varToBindScore_.value()] =
        makeAlwaysDefinedColumn(index);
  }
}

// _____________________________________________________________________________
VariableToColumnMap TextIndexScanForWord::computeVariableToColumnMap() const {
  return config_.variableColumns_.value();
}

// _____________________________________________________________________________
size_t TextIndexScanForWord::getResultWidth() const {
  return 1 + config_.isPrefix_ + config_.varToBindScore_.has_value();
}

// _____________________________________________________________________________
size_t TextIndexScanForWord::getCostEstimate() {
  return getExecutionContext()->getIndex().getSizeOfTextBlockForWord(
      config_.word_);
}

// _____________________________________________________________________________
uint64_t TextIndexScanForWord::getSizeEstimateBeforeLimit() {
  return getExecutionContext()->getIndex().getSizeOfTextBlockForWord(
      config_.word_);
}

// _____________________________________________________________________________
vector<ColumnIndex> TextIndexScanForWord::resultSortedOn() const {
  return {ColumnIndex(0)};
}

// _____________________________________________________________________________
string TextIndexScanForWord::getDescriptor() const {
  return absl::StrCat("TextIndexScanForWord on ",
                      config_.varToBindText_.name());
}

// _____________________________________________________________________________
string TextIndexScanForWord::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "WORD INDEX SCAN: "
     << " with word: \"" << config_.word_ << "\"";
  return std::move(os).str();
}
