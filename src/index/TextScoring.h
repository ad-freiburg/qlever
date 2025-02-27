// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#pragma once

#include "index/Index.h"
#include "parser/WordsAndDocsFileParser.h"

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

  float getScore(WordIndex wordIndex, TextRecordIndex contextId);

  void calculateScoreData(const string& docsFileName, bool addWordsFromLiterals,
                          const Index::TextVocab& textVocab,
                          const Index::Vocab& vocab);

 private:
  //
  TextScoringMetric scoringMetric_ = TextScoringMetric::EXPLICIT;
  float b_ = 0.75;
  float k_ = 1.75;

  //
  LocaleManager localeManager_;

  //
  InvertedIndex invertedIndex_;
  DocLengthMap docLengthMap_;
  std::set<DocumentIndex> docIdSet_;

  //
  size_t nofDocuments_ = 0;
  size_t totalDocumentLength_ = 0;
  float averageDocumentLength_;

  //
  void addDocumentOrLiteralToScoreDataInvertedIndex(
      std::string_view text, DocumentIndex docId,
      const Index::TextVocab& textVocab);

  void calculateAVDL() {
    averageDocumentLength_ =
        nofDocuments_ ? (totalDocumentLength_ / nofDocuments_) : 0;
  }
};
