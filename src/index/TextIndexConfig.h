// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#ifndef QLEVER_SRC_INDEX_TEXTINDEXCONFIG_H
#define QLEVER_SRC_INDEX_TEXTINDEXCONFIG_H

#include <optional>
#include <string>

#include "index/TextScoringEnum.h"

/**
 * @brief A configuration that stores all parameters to build the text index.
 *        The correctness of all parameters is checked during construction.
 *
 * @details
 *
 * The text index needs a source to be built from. This can either be the
 * literals of the KB (specified by addWordsFromLiterals) or external files
 * (given by filenames with wordsFile_ and docsFile_). It is also possible to
 * use both. For the external files the standard usage is to specify both and
 * let useDocsFileForVocabulary and addOnlyEntitiesFromWordsFile be false.
 * Since the wordsFile can be quite complicated compared to the docsFile the
 * option useDocsFileForVocabulary makes it possible to only specify a
 * docsFile to build the text index from. This loses the function to check
 * if an entity is occurring in a text. To regain this functionality it is
 * possible to use the option addOnlyEntitiesFromWordsFile as well. This leads
 * to the wordsFile only being used to add entities to the text index.
 *
 * Fields:
 *  - std::optional<const std::string> wordsFile_: Specifies the path to the
 *                                                wordsFile.
 *  - std::optional<const std::string> docsFile_: Specifies the path to the
 *                                               docsFile.
 *  - bool addWordsFromLiterals: If set to true the literals from the KB are
 *                               used to build the text index. During retrieval
 *                               this leads to literals being returned if they
 *                               contain the search word.
 * - bool useDocsFileForVocabulary: If set to true the text index is build using
 *                                  the words from the documents given in the
 *                                  docsFile instead of the words from the
 *                                  wordsFile.
 * - bool addOnlyEntitiesFromWordsFile: If set to true the wordsFile is only
 *                                      parsed for entities and not words. This
 *                                      can be used in combination with
 *                                      useDocsFileForVocabulary to still have
 *                                      the functionality to check if a text
 *                                      contains a certain entity even if only
 *                                      the docsFile words were used to build.
 * - TextScoringMetric textScoringMetric: There are 3 scoring options for the
 *                                        text index. First EXPLICIT. Explicit
 *                                        scores are given in the wordsFile. If
 *                                        explicit is used with
 *                                        useDocsFileForVocabulary all scores
 *                                        are 0. Second TFIDF. If Tf-Idf is used
 *                                        an inverted index is build without
 *                                        needing any parameters. Third BM25. If
 *                                        BM25 is used the b and k parameters
 *                                        are used. For details see
 *                                        TextScoring.h.
 * - std::pair<float, float> bAndKForBM25: The b and k parameter used for the
 *                                         BM25 scores. Two things have to hold:
 *                                         0 <= b <= 1 and 0 <= k.
 */
class TextIndexConfig {
 public:
  explicit TextIndexConfig(
      std::optional<std::string> wordsFile = std::nullopt,
      std::optional<std::string> docsFile = std::nullopt,
      bool addWordsFromLiterals = true, bool useDocsFileForVocabulary = false,
      bool addOnlyEntitiesFromWordsFile = false,
      TextScoringConfig textScoringConfig = TextScoringConfig())
      : wordsFile_(std::move(wordsFile)),
        docsFile_(std::move(docsFile)),
        addWordsFromLiterals_(addWordsFromLiterals),
        useDocsFileForVocabulary_(useDocsFileForVocabulary),
        addOnlyEntitiesFromWordsFile_(addOnlyEntitiesFromWordsFile),
        textScoringConfig_(std::move(textScoringConfig)) {
    checkValid();
  };

  // This can be changed when necessary
  TextIndexConfig(TextIndexConfig&&) noexcept = default;
  TextIndexConfig& operator=(TextIndexConfig&&) noexcept = default;

  bool addWordsFromFiles() const {
    return docsFile_.has_value() &&
           (wordsFile_.has_value() || useDocsFileForVocabulary_);
  }

  // Returns the set wordsFile or if not set an empty string
  std::string getWordsFile() const { return wordsFile_.value_or(""); }
  // Returns the set docsFile or if not set an empty string
  std::string getDocsFile() const { return docsFile_.value_or(""); }
  bool getAddWordsFromLiterals() const { return addWordsFromLiterals_; }
  bool getUseDocsFileForVocabulary() const { return useDocsFileForVocabulary_; }
  bool getAddOnlyEntitiesFromWordsFile() const {
    return addOnlyEntitiesFromWordsFile_;
  }
  const TextScoringConfig& getTextScoringConfig() const {
    return textScoringConfig_;
  }

 private:
  std::optional<std::string> wordsFile_ = std::nullopt;
  std::optional<std::string> docsFile_ = std::nullopt;
  bool addWordsFromLiterals_ = true;
  bool useDocsFileForVocabulary_ = false;
  bool addOnlyEntitiesFromWordsFile_ = false;
  TextScoringConfig textScoringConfig_ = TextScoringConfig();

  void checkValid() const {
    AD_CONTRACT_CHECK(
        addWordsFromFiles() || addWordsFromLiterals_,
        "No source to build text index from specified. A TextIndexConfig needs "
        "a source to build the text index from. Either addWordsFromLiterals_ "
        "has to be true or external files to build from have to be given (or "
        "both). When using external files either the words- and docsFile have "
        "to be set or only the docsFile is set with the option "
        "useDocsFileForVocabulary_ true.");
    AD_CONTRACT_CHECK(
        !(!wordsFile_.has_value() && addOnlyEntitiesFromWordsFile_),
        "No wordsFile given while using option to add entities from wordsFile. "
        "If the option addOnlyEntitiesFromWordsFile_ is set to true a "
        "wordsFile "
        "is expected.");
    AD_CONTRACT_CHECK(
        !addOnlyEntitiesFromWordsFile_ ||
            (addOnlyEntitiesFromWordsFile_ && useDocsFileForVocabulary_),
        "If the option addOnlyEntitiesFromWordsFile_ is set to true the option "
        "useDocsFileForVocabulary_ needs to be set to true as well. The "
        "functionality of addOnlyEntitiesFromWordsFile_ is to add entities to "
        "the texts given by the docsFile during text index building.");
    if (textScoringConfig_.scoringMetric_ == TextScoringMetric::BM25) {
      float b = textScoringConfig_.bAndKParam_.first;
      float k = textScoringConfig_.bAndKParam_.second;
      AD_CONTRACT_CHECK(0 <= b && b <= 1 && 0 <= k,
                        "Invalid values given for BM25 score: `b=", b,
                        "` and `k=", k,
                        "`, `b` must be in [0, 1] and `k` must be >= 0 ");
    }
  }
};

#endif  // QLEVER_SRC_INDEX_TEXTINDEXCONFIG_H
