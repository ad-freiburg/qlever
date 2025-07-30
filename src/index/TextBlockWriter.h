// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#ifndef QLEVER_SRC_INDEX_TEXTBLOCKWRITER_H
#define QLEVER_SRC_INDEX_TEXTBLOCKWRITER_H

#include "TextIndexBuilderTypes.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "global/IndexTypes.h"
#include "index/ExternalSortFunctors.h"
#include "index/Postings.h"
#include "index/TextMetaData.h"
#include "index/TextScoringEnum.h"
#include "util/File.h"

/**
 * @brief This struct is used to calculate and write the text blocks.
 * @param wordTextVec: Contains all occurrences of words in text records
 *        with the respective word score. In other words, the cross-product of
 *        words and text records iff the words occur in the text record.
 * @param entityTextVec: Contains all occurrences of entities in text
 *        records with respective words and the respective entity score. In
 *        other words, the cross-product of entities and text records and words
 *        iff an entity occurs in a text record together with a word.
 * @param out: The file the blocks are written to.
 * @param textMeta: The TextMataData to write block info to.
 *
 * @detail The calculation and writing works as follows:
 *         Parse the wordTextVec and add the entries as postings to
 *         wordPostings_. If nofWordPostingsInTextBlock number of words were
 *         added, start parsing the entityTextVec. Since the entityTextVec is
 *         also sorted by word id, it can be parsed up to the last word id.
 *         This adds all entities that co-occur with any word of the block.
 *         Once both posting lists are collected they are written to disk in one
 *         text block.
 *         NOTE: Entity postings corresponding to a certain word are only
 *         added to the first block this word occurs in. This leads to less
 *         duplication. During retrieval all blocks containing the word
 *         are fetched. This guarantees the entityList to be fetched too.
 *
 */
struct TextBlockWriter {
  TextBlockWriter(WordTextVec& wordTextVec, EntityTextVec& entityTextVec,
                  ad_utility::File& out, TextScoringMetric textScoringMetric,
                  TextMetaData& textMeta)
      : wordTextVecView_(wordTextVec.sortedView()),
        entityTextVecView_(entityTextVec.sortedView()),
        out_(out),
        textScoringMetric_(textScoringMetric),
        textMeta_{textMeta},
        entityTextVecIterator_(entityTextVecView_.begin()),
        entityTextVecSentinel_(entityTextVecView_.end()) {}

  // Creates an object TextBlockWriter to calculate and write all blocks to the
  // text index file.
  static void writeTextIndexFile(const std::string& filename,
                                 WordTextVec& wordTextVec,
                                 EntityTextVec& entityTextVec,
                                 TextScoringMetric textScoringMetric,
                                 TextMetaData& textMeta,
                                 size_t nofWordPostingsInTextBlock);

 private:
  static void writeTextMetaDataToFile(ad_utility::File& out,
                                      const TextMetaData& textMeta);

  // Uses the text vecs given during construction to write text blocks to disk
  // that contain exactly nofWordPostingsInTextBlock word postings in the text
  // block except the last block which could potentially contain less
  void calculateAndWriteTextBlocks(size_t nofWordPostingsInTextBlock);

  WordTextVecView wordTextVecView_;
  EntityTextVecView entityTextVecView_;

  // File to write the blocks to
  ad_utility::File& out_;

  TextScoringMetric textScoringMetric_;
  TextMetaData& textMeta_;

  // Iterators
  ql::ranges::iterator_t<EntityTextVecView> entityTextVecIterator_;
  ql::ranges::sentinel_t<EntityTextVecView> entityTextVecSentinel_;

  std::vector<WordPosting> wordPostings_ = {};

  // Tracks the offset in the out_ file to write the blocks
  off_t currentOffset_ = 0;

  // Is called after block boundary is reached to add all co-occuring entities
  // up to and including highestWordInBlock to entityPostings_ and then write
  // the whole block to disk
  void finishBlock(WordVocabIndex highestWordInBlock);

  // Iterates over entityTextVec_ up to and including highestWordInBlock. All
  // entries are written to entityPostings_.
  std::vector<EntityPosting> getEntityPostingsForBlock(
      WordVocabIndex highestWordInBlock);

  template <typename WordRow>
  void addWordPosting(const WordRow& wordTextVecRow);

  template <typename EntityRow>
  static void addEntityPosting(std::vector<EntityPosting>& vecToAddTo,
                               const EntityRow& entityTextVecRow);

  // Does the actual writing to disk using the posting lists
  void writeTextBlockToFile(const std::vector<WordPosting>& wordPostings,
                            const std::vector<EntityPosting>& entityPostings,
                            ad_utility::File& out,
                            WordVocabIndex minWordIndexOfBlock,
                            WordVocabIndex maxWordIndexOfBlock);
};

#endif  // QLEVER_SRC_INDEX_TEXTBLOCKWRITER_H
