// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "index/TextScoring.h"

#include "index/Index.h"

// ____________________________________________________________________________
void ScoreData::calculateScoreData(const string& docsFileName,
                                   bool addWordsFromLiterals,
                                   const Index::TextVocab& textVocab,
                                   const Index::Vocab& vocab) {
  // Skip calculation if scoring mode is set to count
  if (scoringMetric_ == TextScoringMetric::EXPLICIT) {
    return;
  }

  invertedIndex_.reserve(textVocab.size());
  DocumentIndex docId = DocumentIndex::make(0);
  DocsFileParser docsFileParser(docsFileName, textVocab.getLocaleManager());

  // Parse the docsfile first if it exists
  for (auto line : docsFileParser) {
    // Set docId
    docId = line.docId_;
    // Parse docText for invertedIndex
    addDocumentOrLiteralToScoreDataInvertedIndex(line.docContent_, docId,
                                                 textVocab);
  }

  // Parse literals if added
  if (!addWordsFromLiterals) {
    return;
  }
  for (VocabIndex index = VocabIndex::make(0); index.get() < vocab.size();
       index = index.incremented()) {
    auto text = vocab[index];
    if (!vocab.isLiteral(index)) {
      continue;
    }
    // Reset parameters for loop
    docId = docId.incremented();
    std::string_view textView = text;

    // Parse words in literal
    addDocumentOrLiteralToScoreDataInvertedIndex(textView, docId, textVocab);
  }
}

// ____________________________________________________________________________
void ScoreData::addDocumentOrLiteralToScoreDataInvertedIndex(
    std::string_view text, DocumentIndex docId,
    const Index::TextVocab& textVocab) {
  WordVocabIndex wvi;
  for (auto word : tokenizeAndNormalizeText(text, localeManager_)) {
    // Check if word exists and retrieve wordId
    if (!textVocab.getId(word, &wvi)) {
      continue;
    }
    WordIndex currentWordId = wvi.get();
    // Emplaces or increases the docLengthMap
    if (docLengthMap_.contains(docId)) {
      ++docLengthMap_[docId];
    } else {
      docLengthMap_[docId] = 1;
    }
    ++totalDocumentLength_;
    // Check if wordId already is a key in the InvertedIndex
    if (invertedIndex_.contains(currentWordId)) {
      // Check if the word is seen in a new context or not
      InnerMap& innerMap = invertedIndex_.find(currentWordId)->second;
      auto ret = innerMap.find(docId);
      // Seen in new context
      if (ret == innerMap.end()) {
        innerMap.emplace(docId, 1);
        // Seen in known context
      } else {
        ret->second += 1;
      }
      // New word never seen before
    } else {
      invertedIndex_.emplace(currentWordId, InnerMap{{docId, 1}});
    }
  }
  ++nofDocuments_;
  docIdSet_.insert(docId);
}

// ____________________________________________________________________________
float ScoreData::getScore(WordIndex wordIndex, TextRecordIndex contextId) {
  // Retrieve inner map
  if (!invertedIndex_.contains(wordIndex)) {
    LOG(DEBUG) << "Didn't find word in Inverted Scoring Index. WordId: "
               << wordIndex << std::endl;
    return 0;
  }
  calculateAVDL();
  InnerMap& innerMap = invertedIndex_.find(wordIndex)->second;
  size_t df = innerMap.size();
  float idf = std::log2f(nofDocuments_ / df);

  // Retrieve the matching docId for contextId. Since contextId are continuous
  // or at least increased in smaller steps than the docIds but the
  // InvertedIndex uses the docIds one has to find the next biggest docId
  // or the matching docId
  DocumentIndex docId;
  DocumentIndex convertedContextId = DocumentIndex::make(contextId.get());
  if (docIdSet_.contains(convertedContextId)) {
    docId = DocumentIndex::make(contextId.get());
  } else {
    auto it = docIdSet_.upper_bound(convertedContextId);
    if (it == docIdSet_.end()) {
      if (docIdSet_.empty()) {
        AD_THROW("docIdSet is empty and shouldn't be");
      }
      LOG(DEBUG) << "Requesting a contextId that is bigger than the largest "
                    "docId. contextId: "
                 << contextId.get() << " Largest docId: " << *docIdSet_.rbegin()
                 << std::endl;
      return 0;
    } else {
      docId = *it;
    }
  }
  auto ret1 = innerMap.find(docId);
  if (ret1 == innerMap.end()) {
    LOG(DEBUG) << "The calculated docId doesn't exist in the inner Map. docId: "
               << docId << std::endl;
    return 0;
  }
  TermFrequency tf = ret1->second;

  if (scoringMetric_ == TextScoringMetric::TFIDF) {
    return tf * idf;
  }

  auto ret2 = docLengthMap_.find(docId);
  if (ret2 == docLengthMap_.end()) {
    LOG(DEBUG)
        << "The calculated docId doesn't exist in the dochLengthMap. docId: "
        << docId << std::endl;
    return 0;
  }
  size_t dl = ret2->second;
  float alpha = (1 - b_ + b_ * (dl / averageDocumentLength_));
  float tf_star = (tf * (k_ + 1)) / (k_ * alpha + tf);
  return tf_star * idf;
}
