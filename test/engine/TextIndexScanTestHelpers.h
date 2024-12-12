//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#pragma once

namespace textIndexScanTestHelpers {
// NOTE: this function exploits a "lucky accident" that allows us to
// obtain the textRecord using indexToString.
// TODO: Implement a more elegant/stable version
// Idea for a more stable version is to add the literals to the docsfile
// which is later parsed and written to the docsDB. This would lead to a
// possible retrieval of the literals text with the getTextExcerpt function.
// The only problem is the increased size of the docsDB and the double saving
// of the literals.
inline string getTextRecordFromResultTable(const QueryExecutionContext* qec,
                                           const ProtoResult& result,
                                           const size_t& rowIndex) {
  uint64_t offset = qec->getIndex().getNofNonLiterals();
  uint64_t shiftedTextRecordId =
      result.idTable().getColumn(0)[rowIndex].getTextRecordIndex().get() -
      offset;
  return qec->getIndex().indexToString(VocabIndex::make(shiftedTextRecordId));
}

inline string getTextExcerptFromResultTable(const QueryExecutionContext* qec,
                                            const ProtoResult& result,
                                            const size_t& rowIndex) {
  return qec->getIndex().getTextExcerpt(
      result.idTable().getColumn(0)[rowIndex].getTextRecordIndex());
}

inline string getEntityFromResultTable(const QueryExecutionContext* qec,
                                       const ProtoResult& result,
                                       const size_t& rowIndex) {
  return qec->getIndex().indexToString(
      result.idTable().getColumn(1)[rowIndex].getVocabIndex());
}

inline string getWordFromResultTable(const QueryExecutionContext* qec,
                                     const ProtoResult& result,
                                     const size_t& rowIndex) {
  return std::string{qec->getIndex().indexToString(
      result.idTable().getColumn(1)[rowIndex].getWordVocabIndex())};
}

inline string combineToString(const string& text, const string& word) {
  std::stringstream ss;
  ss << "Text: " << text << ", Word: " << word << std::endl;
  return ss.str();
}
}  // namespace textIndexScanTestHelpers
