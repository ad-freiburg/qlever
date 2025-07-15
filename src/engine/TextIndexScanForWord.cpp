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
                                           Variable textRecordVar,
                                           std::string word)
    : Operation(qec),
      config_(TextIndexScanForWordConfiguration{std::move(textRecordVar),
                                                std::move(word)}) {
  config_.isPrefix_ = config_.word_.ends_with('*');
  config_.scoreVar_ = config_.varToBindText_.getWordScoreVariable(
      config_.word_, config_.isPrefix_);
  setVariableToColumnMap();
}

// _____________________________________________________________________________
Result TextIndexScanForWord::computeResult(
    [[maybe_unused]] bool requestLaziness) {
  std::ostringstream oss;
  oss << config_;
  runtimeInfo().addDetail("text-index-scan-for-word-config", oss.str());
  IdTable idTable = getExecutionContext()->getIndex().getWordPostingsForTerm(
      config_.word_, getExecutionContext()->getAllocator());

  // This filters out the word column. When the searchword is a prefix this
  // column shows the word the prefix got extended to
  std::vector<ColumnIndex> cols{0};
  if (config_.isPrefix_) {
    cols.push_back(1);
  }
  if (config_.scoreVar_.has_value()) {
    cols.push_back(2);
  }
  idTable.setColumnSubset(cols);

  // Add details to the runtimeInfo. This is has no effect on the result.
  runtimeInfo().addDetail("word: ", config_.word_);

  return {std::move(idTable), resultSortedOn(), LocalVocab{}};
}

// _____________________________________________________________________________
void TextIndexScanForWord::setVariableToColumnMap() {
  config_.variableColumns_ = VariableToColumnMap{};
  auto index = ColumnIndex{0};
  config_.variableColumns_.value()[config_.varToBindText_] =
      makeAlwaysDefinedColumn(index);
  index++;
  if (config_.isPrefix_) {
    if (!config_.matchVar_.has_value()) {
      config_.matchVar_ = config_.varToBindText_.getMatchingWordVariable(
          std::string_view(config_.word_).substr(0, config_.word_.size() - 1));
    }
    config_.variableColumns_.value()[config_.matchVar_.value()] =
        makeAlwaysDefinedColumn(index);
    index++;
  }
  AD_CORRECTNESS_CHECK(config_.isPrefix_ || !config_.matchVar_.has_value(),
                       "Text index scan for word shouldn't have a variable to "
                       "bind match defined when the word is not a prefix.");
  if (config_.scoreVar_.has_value()) {
    config_.variableColumns_.value()[config_.scoreVar_.value()] =
        makeAlwaysDefinedColumn(index);
  }
}

// _____________________________________________________________________________
VariableToColumnMap TextIndexScanForWord::computeVariableToColumnMap() const {
  return config_.variableColumns_.value();
}

// _____________________________________________________________________________
size_t TextIndexScanForWord::getResultWidth() const {
  return 1 + static_cast<size_t>(config_.isPrefix_) +
         static_cast<size_t>(config_.scoreVar_.has_value());
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
std::string TextIndexScanForWord::getDescriptor() const {
  return absl::StrCat("TextIndexScanForWord on ",
                      config_.varToBindText_.name());
}

// _____________________________________________________________________________
std::string TextIndexScanForWord::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "WORD INDEX SCAN: "
     << " with word: \"" << config_.word_ << "\"";
  return std::move(os).str();
}

// _____________________________________________________________________________
std::unique_ptr<Operation> TextIndexScanForWord::cloneImpl() const {
  return std::make_unique<TextIndexScanForWord>(*this);
}
