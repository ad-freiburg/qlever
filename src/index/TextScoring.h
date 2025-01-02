// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#pragma once

#include "global/Id.h"
#include "global/IndexTypes.h"
#include "index/Index.h"
#include "parser/WordsAndDocsFileParser.h"
#include "util/HashMap.h"

// Inverted index mapping WordIndex to a list of pairs of TextRecordIndex, and
// a TermFrequency
using TermFrequency = uint32_t;
using InnerMap = ad_utility::HashMap<DocumentIndex, TermFrequency>;
using InvertedIndex = ad_utility::HashMap<WordIndex, InnerMap>;
using DocLengthMap = ad_utility::HashMap<DocumentIndex, size_t>;

enum class ScoringMetric { COUNT, TFIDF, BM25 };

class ScoreData {
 public:
  ScoreData(LocaleManager localeManager)
      : scoringMetric_(ScoringMetric::COUNT),
        b_(0.75),
        k_(1.75),
        localeManager_(localeManager){};

  ScoreData(LocaleManager localeManager, ScoringMetric scoringMetric)
      : scoringMetric_(scoringMetric),
        b_(0.75),
        k_(1.75),
        localeManager_(localeManager){};

  ScoreData(LocaleManager localeManager, ScoringMetric scoringMetric,
            std::pair<float, float> bAndKParam)
      : scoringMetric_(scoringMetric),
        b_(bAndKParam.first),
        k_(bAndKParam.second),
        localeManager_(localeManager){};

  // Getters
  ScoringMetric getScoringMetric() { return scoringMetric_; }

  // Functions
  void calculateScoreData(const string& docsFileName, bool addWordsFromLiterals,
                          const Index::TextVocab& textVocab,
                          const Index::Vocab& vocab);

  void addWordToScoreDataInvertedIndex(std::string_view word,
                                       DocumentIndex docId,
                                       const Index::TextVocab& textVocab);

  float getScore(WordIndex wordIndex, TextRecordIndex contextId);

 private:
  //
  ScoringMetric scoringMetric_;
  float b_;
  float k_;

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
  void calculateAVDL() {
    averageDocumentLength_ =
        nofDocuments_ ? (totalDocumentLength_ / nofDocuments_) : 0;
  }
};
