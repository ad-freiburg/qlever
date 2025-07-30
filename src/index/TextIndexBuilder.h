// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#ifndef QLEVER_SRC_INDEX_TEXTINDEXBUILDER_H
#define QLEVER_SRC_INDEX_TEXTINDEXBUILDER_H

#include "index/IndexImpl.h"
#include "index/TextIndexBuilderTypes.h"

// This class contains all the code that is only required when building the
// fulltext index
class TextIndexBuilder : public IndexImpl {
  using WordMap = ad_utility::HashMap<WordVocabIndex, Score>;
  using EntityMap = ad_utility::HashMap<VocabIndex, Score>;

  // A word posting is a combination of WordId, TextRecordId and Score. The
  // half-inverted text index uses those sorted by WordId to quickly look up
  // in which documents words occur. This parameter governs the number of
  // postings saved in one block written to disk. On retrieval blocks are read
  // as a whole.
  size_t nofWordPostingsInTextBlock_ = NOF_WORD_POSTINGS_IN_TEXT_BLOCK;

 public:
  explicit TextIndexBuilder(ad_utility::AllocatorWithLimit<Id> allocator,
                            const std::string& onDiskBase)
      : IndexImpl(allocator) {
    setOnDiskBase(onDiskBase);
  }

  // Adds a text index to a complete KB index. Reads words from the given
  // wordsfile and calculates bm25 scores with the docsfile if given.
  // Additionally adds words from literals of the existing KB. Can't be called
  // with only words or only docsfile, but with or without both. Also can't be
  // called with the pair empty and bool false
  void buildTextIndexFile(
      const std::optional<std::pair<std::string, std::string>>&
          wordsAndDocsFile,
      bool addWordsFromLiterals,
      TextScoringMetric textScoringMetric = TextScoringMetric::EXPLICIT,
      std::pair<float, float> bAndKForBM25 = {0.75f, 1.75f});

  // Build docsDB file from given file (one text record per line).
  void buildDocsDB(const std::string& docsFile) const;

  void setNofWordPostingsInTextBlock(size_t nofWordPostingsInTextBlock) {
    nofWordPostingsInTextBlock_ = nofWordPostingsInTextBlock;
  }

 private:
  size_t processWordsForVocabulary(const std::string& contextFile,
                                   bool addWordsFromLiterals);

  void processWordsForInvertedLists(const std::string& contextFile,
                                    bool addWordsFromLiterals,
                                    WordTextVec& wordTextVec,
                                    EntityTextVec& entityTextVec);

  // Generator that returns all words in the given context file (if not empty)
  // and then all words in all literals (if second argument is true).
  //
  // TODO: So far, this is limited to the internal vocabulary (still in the
  // testing phase, once it works, it should be easy to include the IRIs and
  // literals from the external vocabulary as well).
  cppcoro::generator<WordsFileLine> wordsInTextRecords(
      std::string contextFile, bool addWordsFromLiterals) const;

  void processEntityCaseDuringInvertedListProcessing(
      const WordsFileLine& line, EntityMap& entitiesInContext,
      size_t& nofLiterals, size_t& entityNotFoundErrorMsgCount) const;

  void processWordCaseDuringInvertedListProcessing(const WordsFileLine& line,
                                                   WordMap& wordsInContext,
                                                   ScoreData& scoreData) const;

  static void logEntityNotFound(const std::string& word,
                                size_t& entityNotFoundErrorMsgCount);

  static void addContextToVectors(WordTextVec& wordTextVec,
                                  EntityTextVec& entityTextVec,
                                  TextRecordIndex context, const WordMap& words,
                                  const EntityMap& entities);

  void createTextIndex(const std::string& filename, WordTextVec& wordTextVec,
                       EntityTextVec& entityTextVec);
};

#endif  // QLEVER_SRC_INDEX_TEXTINDEXBUILDER_H
