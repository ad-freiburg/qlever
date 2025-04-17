// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#ifndef TEXTINDEXCONFIG_H
#define TEXTINDEXCONFIG_H

#include <optional>
#include <string>

#include "index/TextScoringEnum.h"

/**
 * @brief A configuration that stores all parameters to build the text index.
 *        The correctness of all parameters can be checked by calling the
 *        checkValid function.
 *
 * @details
 *
 * The text index needs a source to be built from. This can either be the
 * literals of the KB (specified by addWordsFromLiterals) or external files
 * (given by filenames with wordsFile and docsFile). It is also possible to
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
 *  - std::optional<const std::string> wordsFile: Specifies the path to the
 *                                                wordsFile.
 *  - std::optional<const std::string> docsFile: Specifies the path to the
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
struct TextIndexConfig {
  std::optional<const std::string> wordsFile = std::nullopt;
  std::optional<const std::string> docsFile = std::nullopt;
  bool addWordsFromLiterals = true;
  bool useDocsFileForVocabulary = false;
  bool addOnlyEntitiesFromWordsFile = false;
  TextScoringMetric textScoringMetric = TextScoringMetric::EXPLICIT;
  std::pair<float, float> bAndKForBM25 = {0.75f, 1.75f};

  void checkValid() const {
    bool buildFromFiles = docsFile.has_value() &&
                          (wordsFile.has_value() || useDocsFileForVocabulary);
    AD_CONTRACT_CHECK(
        buildFromFiles || addWordsFromLiterals,
        "No source to build text index from specified. A TextIndexConfig needs "
        "a source to build the text index from. Either addWordsFromLiterals "
        "has to be true or external files to build from have to be given (or "
        "both). When using external files either the words- and docsFile have "
        "to be set or only the docsFile is set with the option "
        "useDocsFileForVocabulary true.");
    AD_CONTRACT_CHECK(
        !(!wordsFile.has_value() && addOnlyEntitiesFromWordsFile),
        "No wordsFile given while using option to add entities from wordsFile. "
        "If the option addOnlyEntitiesFromWordsFile is set to true a wordsFile "
        "is expected.");
    AD_CONTRACT_CHECK(
        !addOnlyEntitiesFromWordsFile ||
            (addOnlyEntitiesFromWordsFile && useDocsFileForVocabulary),
        "If the option addOnlyEntitiesFromWordsFile is set to true the option "
        "useDocsFileForVocabulary needs to be set to true as well. The "
        "functionality of addOnlyEntitiesFromWordsFile is to add entities to "
        "the texts given by the docsFile during text index building.");
    if (textScoringMetric == TextScoringMetric::BM25) {
      float b = bAndKForBM25.first;
      float k = bAndKForBM25.second;
      AD_CONTRACT_CHECK(0 <= b && b <= 1 && 0 <= k,
                        "Invalid values given for BM25 score: `b=", b,
                        "` and `k=", k,
                        "`, `b` must be in [0, 1] and `k` must be >= 0 ");
    }
  }
};

#endif  // TEXTINDEXCONFIG_H
