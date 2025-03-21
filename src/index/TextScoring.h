// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#pragma once

#include "index/Index.h"
#include "parser/WordsAndDocsFileParser.h"

// This class is used to calculate tf-idf and bm25 scores and use them in the
// building of the text index.
class ScoreData {
 public:
  ScoreData() = default;

  ScoreData(LocaleManager localeManager) : localeManager_(localeManager){};

  ScoreData(LocaleManager localeManager, TextScoringMetric scoringMetric)
      : scoringMetric_(scoringMetric), localeManager_(localeManager){};

  ScoreData(LocaleManager localeManager, TextScoringMetric scoringMetric,
            std::pair<float, float> bAndKParam)
      : scoringMetric_(scoringMetric),
        b_(bAndKParam.first),
        k_(bAndKParam.second),
        localeManager_(localeManager){};

  TextScoringMetric getScoringMetric() const { return scoringMetric_; }

  // Retrieves score from filled InvertedIndex
  float getScore(WordIndex wordIndex, TextRecordIndex contextId);

  // Parses docsFile and if true literals to fill the InvertedIndex and the
  // extra values needed to calculate scores
  void calculateScoreData(const string& docsFileName, bool addWordsFromLiterals,
                          const Index::TextVocab& textVocab,
                          const Index::Vocab& vocab);

 private:
  TextScoringMetric scoringMetric_ = TextScoringMetric::EXPLICIT;
  float b_ = 0.75;
  float k_ = 1.75;

  LocaleManager localeManager_;

  // The invertedIndex_ connects words to documents (docIds) and the term
  // frequency of  those words inside the documents
  InvertedIndex invertedIndex_;

  // The docLengthMap_ connects documents (docIds) to their length measured in
  // words. Important: Some words are filtered out. For details see
  // addDocumentOrLiteralToScoreDataInvertedIndex
  DocLengthMap docLengthMap_;

  // The docIdSet_ keeps track of all docIds encountered in docsfile.tsv. This
  // is necessary to later convert contextIds to docIds and link contexts to
  // docs
  std::set<DocumentIndex> docIdSet_;

  // Values needed to calculate tf-idf and bm25
  size_t nofDocuments_ = 0;
  size_t totalDocumentLength_ = 0;
  float averageDocumentLength_;

  // Used to fill the InvertedIndex given a document (or literal), the docId
  // and the current TextVocab
  void addDocumentOrLiteralToScoreDataInvertedIndex(
      std::string_view text, DocumentIndex docId,
      const Index::TextVocab& textVocab, size_t& wordNotFoundErrorMsgCount);

  void calculateAVDL() {
    averageDocumentLength_ =
        nofDocuments_ ? (totalDocumentLength_ / nofDocuments_) : 0;
  }
};
