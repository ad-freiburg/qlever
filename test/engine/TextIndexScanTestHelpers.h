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
                                           const ProtoResult& result,
                                           const size_t& rowIndex) {
  uint64_t nofLiterals = qec->getIndex().getNofLiteralsInTextIndex();
  uint64_t nofContexts = qec->getIndex().getNofTextRecords();
  uint64_t textRecordIdFromTable =
      result.idTable().getColumn(0)[rowIndex].getTextRecordIndex().get();
  if ((nofContexts - nofLiterals) <= textRecordIdFromTable) {
    // Return when from Literals
    return qec->getIndex().indexToString(
        VocabIndex::make(textRecordIdFromTable - (nofContexts - nofLiterals)));
  } else {
    // Return when from DocsDB
    return qec->getIndex().getTextExcerpt(
        result.idTable().getColumn(0)[rowIndex].getTextRecordIndex());
  }
}

inline const TextRecordIndex getTextRecordIdFromResultTable(
    [[maybe_unused]] const QueryExecutionContext* qec,
    const ProtoResult& result, const size_t& rowIndex) {
  return result.idTable().getColumn(0)[rowIndex].getTextRecordIndex();
}

// Only use on prefix search results
inline string getEntityFromResultTable(const QueryExecutionContext* qec,
                                       const ProtoResult& result,
                                       const size_t& rowIndex) {
  return qec->getIndex().indexToString(
      result.idTable().getColumn(1)[rowIndex].getVocabIndex());
}

// Only use on prefix search results
inline string getWordFromResultTable(const QueryExecutionContext* qec,
                                     const ProtoResult& result,
                                     const size_t& rowIndex) {
  return std::string{qec->getIndex().indexToString(
      result.idTable().getColumn(1)[rowIndex].getWordVocabIndex())};
}

inline Score getScoreFromResultTable(
    [[maybe_unused]] const QueryExecutionContext* qec,
    const ProtoResult& result, const size_t& rowIndex, bool wasPrefixSearch) {
  size_t colToRetrieve = wasPrefixSearch ? 2 : 1;
  return static_cast<Score>(
      result.idTable().getColumn(colToRetrieve)[rowIndex].getDouble());
}

inline float calculateBM25FromParameters(size_t tf, size_t df, size_t nofDocs,
                                         size_t avdl, size_t dl, float b,
                                         float k) {
  float idf = log2f(nofDocs / df);
  float alpha = 1 - b + b * dl / avdl;
  float tf_star = (tf * (k + 1)) / (k * alpha + tf);
  return tf_star * idf;
}

inline float calculateTFIDFFromParameters(size_t tf, size_t df,
                                          size_t nofDocs) {
  float idf = log2f(nofDocs / df);
  return tf * idf;
}

inline string combineToString(const string& text, const string& word) {
  std::stringstream ss;
  ss << "Text: " << text << ", Word: " << word << std::endl;
  return ss.str();
}

std::string inlineSeperator = "\t";
std::string lineSeperator = "\n";

inline string createWordsFileLine(std::string word, bool isEntity,
                                  size_t contextId, size_t score) {
  return word + inlineSeperator + (isEntity ? "1" : "0") + inlineSeperator +
         std::to_string(contextId) + inlineSeperator + std::to_string(score) +
         lineSeperator;
};

inline string createDocsFileLine(size_t docId, std::string docContent) {
  return std::to_string(docId) + inlineSeperator + docContent + lineSeperator;
};
}  // namespace textIndexScanTestHelpers
