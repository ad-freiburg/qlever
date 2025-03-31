// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "index/TextScoring.h"

#include "index/Index.h"

// ____________________________________________________________________________
static void logWordNotFound(const string& word,
                            size_t& wordNotFoundErrorMsgCount) {
  if (wordNotFoundErrorMsgCount < 20) {
    LOG(WARN) << "The following word was found in the docsfile but not in "
                 "the wordsfile: "
              << word << '\n';
    ++wordNotFoundErrorMsgCount;
    if (wordNotFoundErrorMsgCount == 1) {
      LOG(WARN) << "Note that this might be intentional if for example stop "
                   "words from the documents where omitted in the wordsfile to "
                   "make the text index more efficient and effective. \n";
    } else if (wordNotFoundErrorMsgCount == 20) {
      LOG(WARN) << "There are more words not in the KB during score "
                   "calculation..."
                << " suppressing further warnings...\n";
    }
  } else {
    wordNotFoundErrorMsgCount++;
  }
}

// ____________________________________________________________________________
void ScoreData::calculateScoreData(const string& docsFileName,
                                   bool addWordsFromLiterals,
                                   const Index::TextVocab& textVocab,
                                   const Index::Vocab& vocab) {
  // Skip calculation if scoring mode is set to count
  if (scoringMetric_ == TextScoringMetric::EXPLICIT) {
    return;
  }

  size_t wordsNotFoundFromDocuments = 0;
  invertedIndex_.reserve(textVocab.size());
  DocumentIndex docId = DocumentIndex::make(0);
  DocsFileParser docsFileParser(docsFileName, textVocab.getLocaleManager());

  // Parse the docsfile first if it exists
  for (const DocsFileLine& line : docsFileParser) {
    // Set docId
    docId = line.docId_;
    // Parse docText for invertedIndex
    addDocumentOrLiteralToScoreDataInvertedIndex(std::move(line.docContent_),
                                                 docId, textVocab,
                                                 wordsNotFoundFromDocuments);
  }

  // Parse literals if added
  if (!addWordsFromLiterals) {
    return;
  }
  if (wordsNotFoundFromDocuments > 0) {
    LOG(WARN)
        << "Number of words not found in vocabulary during score calculation: "
        << wordsNotFoundFromDocuments << std::endl;
  }
  size_t wordsNotFoundFromLiterals = 0;
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
    addDocumentOrLiteralToScoreDataInvertedIndex(textView, docId, textVocab,
                                                 wordsNotFoundFromLiterals);
  }
  AD_CORRECTNESS_CHECK(wordsNotFoundFromLiterals == 0, "There were",
                       wordsNotFoundFromLiterals,
                       " words from literals not found in the inverted scoring "
                       "index. One reason may be the tokenizer for creating "
                       "the text vocab from literals and the one used during "
                       "score calculation being different which shouldn't be.");
}

// ____________________________________________________________________________
void ScoreData::addDocumentOrLiteralToScoreDataInvertedIndex(
    std::string_view text, DocumentIndex docId,
    const Index::TextVocab& textVocab, size_t& wordNotFoundErrorMsgCount) {
  WordVocabIndex wvi;
  for (const auto& word : tokenizeAndNormalizeText(text, localeManager_)) {
    // Check if word exists and retrieve wordId
    if (!textVocab.getId(word, &wvi)) {
      logWordNotFound(word, wordNotFoundErrorMsgCount);
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
  float idf = std::log2f(static_cast<float>(nofDocuments_) / df);

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

    AD_CORRECTNESS_CHECK(!(it == docIdSet_.end()),
                         "Requesting a contextId that is bigger than the "
                         "largest docId. Requested contextId: ",
                         contextId.get(),
                         " Largest docId: ", docIdSet_.rbegin()->get(),
                         " This hints on faulty input data for wordsfile.tsv "
                         "and or docsfile.tsv");
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
    return static_cast<float>(tf) * idf;
  }

  auto ret2 = docLengthMap_.find(docId);
  AD_CORRECTNESS_CHECK(
      !(ret2 == docLengthMap_.end()),
      "The calculated docId doesn't exist in the docLengthMap. The requested "
      "contextId was: ",
      contextId.get(), " The calculated docId was: ", docId.get(),
      " This hints on faulty input data for wordsfile.tsv and or docsfile.tsv");
  size_t dl = ret2->second;
  float alpha =
      (1 - b_ + b_ * (static_cast<float>(dl) / averageDocumentLength_));
  float tfStar = (static_cast<float>(tf) * (k_ + 1)) /
                 (k_ * alpha + static_cast<float>(tf));
  return tfStar * idf;
}
