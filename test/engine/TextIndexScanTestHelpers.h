//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#ifndef QLEVER_TEST_ENGINE_TEXTINDEXSCANTESTHELPERS_H
#define QLEVER_TEST_ENGINE_TEXTINDEXSCANTESTHELPERS_H

#include "engine/QueryExecutionContext.h"
#include "engine/Result.h"
#include "global/Id.h"
#include "global/IndexTypes.h"

namespace textIndexScanTestHelpers {
constexpr std::string_view inlineSeparator = "\t";
constexpr std::string_view lineSeparator = "\n";

inline std::string createWordsFileLineAsString(std::string_view word,
                                               bool isEntity, size_t contextId,
                                               size_t score) {
  return absl::StrCat(word, inlineSeparator, isEntity, inlineSeparator,
                      contextId, inlineSeparator, score, lineSeparator);
};

inline std::string createDocsFileLineAsString(size_t docId,
                                              std::string_view docContent) {
  return absl::StrCat(docId, inlineSeparator, docContent, lineSeparator);
};

// NOTE: this function exploits a "lucky accident" that allows us to
// obtain the textRecord using indexToString.
// TODO: Implement a more elegant/stable version
// Idea for a more stable version is to add the literals to the docsfile
// which is later parsed and written to the docsDB. This would lead to a
// possible retrieval of the literals text with the getTextExcerpt function.
// The only problem is the increased size of the docsDB and the double saving
// of the literals.
inline std::string getTextRecordFromResultTable(
    const QueryExecutionContext* qec, const Result& result,
    const size_t& rowIndex) {
  size_t nofNonLiterals = qec->getIndex().getNofNonLiteralsInTextIndex();
  uint64_t textRecordIdFromTable =
      result.idTable().getColumn(0)[rowIndex].getTextRecordIndex().get();
  if (nofNonLiterals <= textRecordIdFromTable) {
    // Return when from Literals
    // Note: the return type of `indexToString` might be `string_view` if the
    // vocabulary is stored uncompressed in memory, hence the explicit cast to
    // `std::string`.
    return std::string{qec->getIndex().indexToString(
        VocabIndex::make(textRecordIdFromTable - nofNonLiterals))};
  } else {
    // Return when from DocsDB
    return std::string{qec->getIndex().getTextExcerpt(
        result.idTable().getColumn(0)[rowIndex].getTextRecordIndex())};
  }
}

inline const TextRecordIndex getTextRecordIdFromResultTable(
    [[maybe_unused]] const QueryExecutionContext* qec, const Result& result,
    const size_t& rowIndex) {
  return result.idTable().getColumn(0)[rowIndex].getTextRecordIndex();
}

// Only use on prefix search results
inline std::string getEntityFromResultTable(const QueryExecutionContext* qec,
                                            const Result& result,
                                            const size_t& rowIndex) {
  // We need the explicit cast to `std::string` because the return type of
  // `indexToString` might be `string_view` if the vocabulary is stored
  // uncompressed in memory.
  return std::string{qec->getIndex().indexToString(
      result.idTable().getColumn(1)[rowIndex].getVocabIndex())};
}

// Only use on prefix search results
inline std::string getWordFromResultTable(const QueryExecutionContext* qec,
                                          const Result& result,
                                          const size_t& rowIndex) {
  return std::string{qec->getIndex().indexToString(
      result.idTable().getColumn(1)[rowIndex].getWordVocabIndex())};
}

inline Score getScoreFromResultTable(
    [[maybe_unused]] const QueryExecutionContext* qec, const Result& result,
    const size_t& rowIndex, bool wasPrefixSearch, bool scoreIsInt = true) {
  size_t colToRetrieve = wasPrefixSearch ? 2 : 1;
  if (scoreIsInt) {
    return static_cast<Score>(
        result.idTable().getColumn(colToRetrieve)[rowIndex].getInt());
  } else {
    return static_cast<Score>(
        result.idTable().getColumn(colToRetrieve)[rowIndex].getDouble());
  }
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

inline std::string combineToString(const std::string& text,
                                   const std::string& word) {
  std::stringstream ss;
  ss << "Text: " << text << ", Word: " << word << std::endl;
  return ss.str();
}

// Struct to reduce code duplication
struct TextResult {
  QueryExecutionContext* qec_;
  const Result& result_;
  bool isPrefixSearch_ = true;
  bool scoreIsInt_ = true;

  auto getRow(size_t row) const {
    return combineToString(getTextRecordFromResultTable(qec_, result_, row),
                           getWordFromResultTable(qec_, result_, row));
  }

  auto getId(size_t row) const {
    return getTextRecordIdFromResultTable(qec_, result_, row);
  }

  auto getEntity(size_t row) const {
    return getEntityFromResultTable(qec_, result_, row);
  }

  auto getTextRecord(size_t row) const {
    return getTextRecordFromResultTable(qec_, result_, row);
  }

  auto getWord(size_t row) const {
    return getWordFromResultTable(qec_, result_, row);
  }

  auto getScore(size_t row) const {
    return getScoreFromResultTable(qec_, result_, row, isPrefixSearch_,
                                   scoreIsInt_);
  }

  void checkListOfWords(const std::vector<std::string>& expectedWords,
                        size_t& startingIndex) const {
    for (const auto& word : expectedWords) {
      ASSERT_EQ(word, getWord(startingIndex));
      ++startingIndex;
    }
  }
};
}  // namespace textIndexScanTestHelpers

#endif  // QLEVER_TEST_ENGINE_TEXTINDEXSCANTESTHELPERS_H
