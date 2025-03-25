//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#pragma once

#include "global/IndexTypes.h"
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
                                           const Result& result,
                                           const size_t& rowIndex) {
  size_t nofNonLiterals = qec->getIndex().getNofNonLiteralsInTextIndex();
  uint64_t textRecordIdFromTable =
      result.idTable().getColumn(0)[rowIndex].getTextRecordIndex().get();
  if (nofNonLiterals <= textRecordIdFromTable) {
    // Return when from Literals
    return qec->getIndex().indexToString(
        VocabIndex::make(textRecordIdFromTable - nofNonLiterals));
  } else {
    // Return when from DocsDB
    return qec->getIndex().getTextExcerpt(
        result.idTable().getColumn(0)[rowIndex].getTextRecordIndex());
  }
}

inline const TextRecordIndex getTextRecordIdFromResultTable(
    [[maybe_unused]] const QueryExecutionContext* qec, const Result& result,
    const size_t& rowIndex) {
  return result.idTable().getColumn(0)[rowIndex].getTextRecordIndex();
}

// Only use on prefix search results
inline string getEntityFromResultTable(const QueryExecutionContext* qec,
                                       const Result& result,
                                       const size_t& rowIndex) {
  return qec->getIndex().indexToString(
      result.idTable().getColumn(1)[rowIndex].getVocabIndex());
}

// Only use on prefix search results
inline string getWordFromResultTable(const QueryExecutionContext* qec,
                                     const Result& result,
                                     const size_t& rowIndex) {
  return std::string{qec->getIndex().indexToString(
      result.idTable().getColumn(1)[rowIndex].getWordVocabIndex())};
}

inline size_t getScoreFromResultTable(
    [[maybe_unused]] const QueryExecutionContext* qec, const Result& result,
    const size_t& rowIndex, bool wasPrefixSearch) {
  size_t colToRetrieve = wasPrefixSearch ? 2 : 1;
  return static_cast<size_t>(
      result.idTable().getColumn(colToRetrieve)[rowIndex].getInt());
}

inline string combineToString(const string& text, const string& word) {
  std::stringstream ss;
  ss << "Text: " << text << ", Word: " << word << std::endl;
  return ss.str();
}
}  // namespace textIndexScanTestHelpers
