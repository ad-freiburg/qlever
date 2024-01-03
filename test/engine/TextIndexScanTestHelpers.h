#pragma once

namespace textIndexScanTestHelpers {
// NOTE: this function exploits a "lucky accident" that allows us to
// obtain the textRecord using idToOptionalString.
// TODO: Implement a more elegant/stable version
inline string getTextRecordFromResultTable(const QueryExecutionContext* qec,
                                           const ResultTable& result,
                                           const size_t& rowIndex) {
  return qec->getIndex()
      .idToOptionalString(
          result.idTable().getColumn(0)[rowIndex].getVocabIndex())
      .value();
}

inline string getEntityFromResultTable(const QueryExecutionContext* qec,
                                       const ResultTable& result,
                                       const size_t& rowIndex) {
  return qec->getIndex()
      .idToOptionalString(
          result.idTable().getColumn(1)[rowIndex].getVocabIndex())
      .value();
}

inline string getWordFromResultTable(const QueryExecutionContext* qec,
                                     const ResultTable& result,
                                     const size_t& rowIndex) {
  return qec->getIndex()
      .idToOptionalString(
          result.idTable().getColumn(1)[rowIndex].getWordVocabIndex())
      .value();
}

inline string combineToString(const string& text, const string& word) {
  std::stringstream ss;
  ss << "Text: " << text << ", Word: " << word << endl;
  return ss.str();
}
}  // namespace textIndexScanTestHelpers
