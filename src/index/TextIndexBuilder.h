// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#ifndef QLEVER_SRC_INDEX_TEXTINDEXBUILDER_H
#define QLEVER_SRC_INDEX_TEXTINDEXBUILDER_H

#include "index/IndexImpl.h"

// This class contains all the code that is only required when building the
// fulltext index
class TextIndexBuilder : public IndexImpl {
 public:
  explicit TextIndexBuilder(ad_utility::AllocatorWithLimit<Id> allocator,
                            const std::string& onDiskBase)
      : IndexImpl(allocator) {
    setOnDiskBase(onDiskBase);
    std::ifstream checkTextIndexIndicesFile(onDiskBase_ +
                                            TEXT_INDEX_LITERAL_IDS);
    if (checkTextIndexIndicesFile.good()) {
      textIndexIndices_ = ad_utility::MmapVector<VocabIndex>(
          onDiskBase_ + TEXT_INDEX_LITERAL_IDS, ad_utility::ReuseTag{});
    } else {
      textIndexIndices_ = std::nullopt;
      AD_LOG_INFO << "Text index literal indices file wasn't found. If "
                     "addWordsFromLiterals is set to true then the whole RDF "
                     "Vocabulary is parsed for literals."
                  << std::endl;
    }
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

 private:
  size_t processWordsForVocabulary(const std::string& contextFile,
                                   bool addWordsFromLiterals);

  void processWordsForInvertedLists(const std::string& contextFile,
                                    bool addWordsFromLiterals, TextVec& vec);

  // Generator that returns all words in the given context file (if not empty)
  // and then all words in all literals (if second argument is true).
  //
  // TODO: So far, this is limited to the internal vocabulary (still in the
  // testing phase, once it works, it should be easy to include the IRIs and
  // literals from the external vocabulary as well).
  cppcoro::generator<WordsFileLine> wordsInTextRecords(
      std::string contextFile, bool addWordsFromLiterals) const;

  // Retrieves all literals as entity WordsFileLine and all words of the
  // literals as WordsFileLine. Depending if file with textIndexIndices_ exists
  // either parses said file and uses the indices to retrieve literals or if
  // file doesn't exist then just parses the whole RDF vocabulary for its
  // literals.
  cppcoro::generator<WordsFileLine> wordsInLiterals(
      TextRecordIndex& contextId) const;

  // This is used in wordsInTextRecords to split up a literal given from a
  // text into the respective `WordsFileLines`. The first line is the
  // whole literal treated as entity and the following lines are the single
  // words of the literals.
  cppcoro::generator<WordsFileLine> literalToWordsFileLines(
      std::string text, TextRecordIndex& contextId) const;

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

#endif
#endif  // QLEVER_SRC_INDEX_TEXTINDEXBUILDER_H
