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
  for (auto& line : docsFileParser) {
    // Set docId
    docId = line.docId_;
    // Parse docText for invertedIndex
    addDocumentOrLiteralToScoreDataInvertedIndex(std::move(line.docContent_),
                                                 docId, textVocab);
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
  for (const auto& word : tokenizeAndNormalizeText(text, localeManager_)) {
    // Check if word exists and retrieve wordId
    if (!textVocab.getId(word, &wvi)) {
      continue;
    }
    WordIndex currentWordId = wvi.get();
    // Increases the docLength of document with is nofWords of a document
    ++docLengthMap_[docId];
    ++totalDocumentLength_;
    // Increase the TermFrequency for a word in the doc
    ++invertedIndex_[currentWordId][docId];
  }
  ++nofDocuments_;
  docIdSet_.insert(docId);
}

// ____________________________________________________________________________
float ScoreData::getScore(WordIndex wordIndex, TextRecordIndex contextId) {
  AD_CORRECTNESS_CHECK(!(scoringMetric_ == TextScoringMetric::EXPLICIT),
                       "This method shouldn't be called for explicit scores.");
  // Retrieve inner map
  auto it = invertedIndex_.find(wordIndex);
  if (it == invertedIndex_.end()) {
    LOG(DEBUG) << "Didn't find word in Inverted Scoring Index. WordId: "
               << wordIndex << std::endl;
    return 0;
  }
  calculateAVDL();
  InnerMap& innerMap = it->second;
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
    AD_CORRECTNESS_CHECK(!docIdSet_.empty(),
                         "docIdSet is empty and shouldn't be.");
    AD_CORRECTNESS_CHECK(
        !(it == docIdSet_.end()),
        absl::StrCat("Requesting a contextId that is bigger than the largest "
                     "docId. Requested contextId: ",
                     contextId.get(),
                     " Largest docId: ", docIdSet_.rbegin()->get(),
                     " This hints on faulty input data for wordsfile.tsv and "
                     "or docsfile.tsv"));
    docId = *it;
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
  AD_CORRECTNESS_CHECK(
      !(ret2 == docLengthMap_.end()),
      absl::StrCat("The calculated docId doesn't exist in the docLengthMap. "
                   "The requested contextId was: ",
                   contextId.get(), " The calculated docId was: ", docId.get(),
                   " This hints on faulty input data for wordsfile.tsv and or "
                   "docsfile.tsv"));
  size_t dl = ret2->second;
  float alpha = (1 - b_ + b_ * (dl / averageDocumentLength_));
  float tf_star = (tf * (k_ + 1)) / (k_ * alpha + tf);
  return tf_star * idf;
}
