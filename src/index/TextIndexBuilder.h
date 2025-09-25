// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#ifndef QLEVER_SRC_INDEX_TEXTINDEXBUILDER_H
#define QLEVER_SRC_INDEX_TEXTINDEXBUILDER_H

#include "index/IndexImpl.h"

// This class contains all the code that is only required when building the
// fulltext index
class TextIndexBuilder : public IndexImpl {
  using TextIndexConfig = qlever::TextIndexConfig;

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
  void buildTextIndexFile(TextIndexConfig config);

  // Build docsDB file from given file (one text record per line).
  void buildDocsDB(const std::string& docsFileName) const;

 private:
  // Creates a vocabulary filled with the words given in the respective words-
  // or docsfile and/or the literals of the index (for details see
  // TextIndexConfig). Returns the number of words read (counting multiple
  // occurrences)
  size_t processWordsForVocabulary(const TextIndexConfig& textIndexConfig);

  // Fills the given 'vec' with all occurrences of entities and words in the
  // respective TextRecords. Depending on the configuration the words and
  // entities are read from different files and/or added from literals (for
  // details see TextIndexConfig).
  void processWordsForInvertedLists(const TextIndexConfig& textIndexConfig,
                                    TextVec& vec);

  // This function is used when the TextIndexConfig specifies to add words from
  // the docsfile but entities from the wordsfile. In this case both files are
  // parsed in parallel to ensure a correct TextRecordIndex for the entities.
  template <typename T>
  void wordsFromDocsFileEntitiesFromWordsFile(
      const std::string& wordsFile, const std::string& docsFile,
      const LocaleManager& localeManager, T processLine) const;

  // Generator that returns all words in all literals.
  //
  // TODO: So far, this is limited to the internal vocabulary (still in the
  // testing phase, once it works, it should be easy to include the IRIs and
  // literals from the external vocabulary as well).
  cppcoro::generator<WordsFileLine> wordsInLiterals(size_t startIndex) const;

  void processEntityCaseDuringInvertedListProcessing(
      const WordsFileLine& line,
      ad_utility::HashMap<Id, Score>& entitiesInContxt, size_t& nofLiterals,
      size_t& entityNotFoundErrorMsgCount) const;

  void processWordCaseDuringInvertedListProcessing(
      const WordsFileLine& line,
      ad_utility::HashMap<WordIndex, Score>& wordsInContext,
      ScoreData& scoreData) const;

  static void logEntityNotFound(const std::string& word,
                                size_t& entityNotFoundErrorMsgCount);

  void addContextToVector(TextVec& vec, TextRecordIndex context,
                          const ad_utility::HashMap<WordIndex, Score>& words,
                          const ad_utility::HashMap<Id, Score>& entities) const;

  void createTextIndex(const std::string& filename, TextVec& vec);

  /// Calculate the block boundaries for the text index. The boundary of a
  /// block is the index in the `textVocab_` of the last word that belongs
  /// to this block.
  /// This implementation takes a reference to an `IndexImpl` and a callable,
  /// that is called once for each blockBoundary, with the `size_t`
  /// blockBoundary as a parameter. Internally uses
  /// `calculateBlockBoundariesImpl`.
  template <typename I, typename BlockBoundaryAction>
  static void calculateBlockBoundariesImpl(
      I&& index, const BlockBoundaryAction& blockBoundaryAction);

  /// Calculate the block boundaries for the text index, and store them in the
  /// blockBoundaries_ member.
  void calculateBlockBoundaries();
};

#endif  // QLEVER_SRC_INDEX_TEXTINDEXBUILDER_H
