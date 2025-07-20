// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#ifndef QLEVER_SRC_INDEX_TEXTBLOCKWRITER_H
#define QLEVER_SRC_INDEX_TEXTBLOCKWRITER_H

#include "TextMetaData.h"
#include "TextScoringEnum.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/ExternalSortFunctors.h"
#include "index/Postings.h"
#include "util/File.h"

// Word Id (uint_64t), Context Id (TextRecordIndex), Score (float)
using WordTextVec = ad_utility::CompressedExternalIdTableSorter<SortText, 3>;
// Word Id (uint_64t), Context Id (TextRecordIndex), Entity Id (VocabIndex),
// Score (float)
using EntityTextVec = ad_utility::CompressedExternalIdTableSorter<SortText, 4>;

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
 *         This adds all entities that co-occur with any word of the block to
 *         entityPostings_. Once both posting lists are collected they are
 *         written to disk in one text block.
 *
 */
template <typename WordTextVecView, typename EntityTextVecView>
struct TextBlockWriter {
  TextBlockWriter(WordTextVec& wordTextVec, EntityTextVec& entityTextVec,
                  ad_utility::File& out, TextScoringMetric textScoringMetric,
                  TextMetaData& textMeta)
      : wordTextVecView_(wordTextVec.sortedView()),
        entityTextVecView_(entityTextVec.sortedView()),
        out_(out),
        textScoringMetric_(textScoringMetric),
        textMeta_(textMeta),
        entityTextVecIterator_(entityTextVecView_.begin()),
        entityStartWordIdIterator_(entityTextVecIterator_),
        entityTextVecSentinel_(entityTextVecView_.end()) {}

  // Uses the text vecs given during construction to write text blocks to disk
  // that contain exactly nofWordPostingsInTextBlock word postings in the text
  // block except the last block which could potentially contain less
  void calculateAndWriteTextBlocks(size_t nofWordPostingsInTextBlock);

 private:
  WordTextVecView wordTextVecView_;
  EntityTextVecView entityTextVecView_;

  // File to write the blocks to
  ad_utility::File& out_;

  TextScoringMetric textScoringMetric_;
  TextMetaData& textMeta_;

  // Iterators
  ql::ranges::iterator_t<EntityTextVecView> entityTextVecIterator_;

  // The reason and logic for this iterator is the following:
  // Both wordTextVec and entityTextVec are sorted by WordId.
  // If a WordId appears in a block, all entities it co-occurs with are added.
  // This is done through advancing entityIterator. entityStartWordIdIterator
  // stays at the beginning of the "batch" which is all entries in the
  // entityTextVec with the same WordId. This is necessary since the wordId
  // can appear again in the next block, and we have to add all those entities
  // again. For this reason we can't lose the iterator.
  ql::ranges::iterator_t<EntityTextVecView> entityStartWordIdIterator_;

  ql::ranges::sentinel_t<EntityTextVecView> entityTextVecSentinel_;

  std::vector<Posting> wordPostings_ = {};
  std::vector<Posting> entityPostings_ = {};

  // Tracks the offset in the out_ file to write the blocks
  off_t currentOffset_ = 0;

  // Tracks the current word index of wordTextVec_
  WordIndex currentWordTextVecWordIndex_ = 0;

  // Is called after block boundary is reached to add all co-occuring entities
  // to entityPostings_ and then write the whole block to disk
  void addBlock();

  // Iterates over entityTextVec_ up to and including
  // currentWordTextVecWordIndex_. All entries are written to entityPostings_.
  // Keeps track of the first
  void addEntityPostingsUpToWordIndex();

  template <typename WordRow>
  void addWordPosting(const WordRow& wordTextVecRow);

  template <typename EntityRow>
  void addEntityPosting(const EntityRow& entityTextVecRow);

  void writeTextBlockToFile(const std::vector<Posting>& wordPostings,
                            const std::vector<Posting>& entityPostings,
                            ad_utility::File& out,
                            WordIndex minWordIndexOfBlock,
                            WordIndex maxWordIndexOfBlock);
};

// Deduction guide
template <typename WordTextVec, typename EntityTextVec>
TextBlockWriter(WordTextVec&, EntityTextVec&, ad_utility::File&,
                TextScoringMetric&, TextMetaData&)
    -> TextBlockWriter<decltype(std::declval<WordTextVec&>().sortedView()),
                       decltype(std::declval<EntityTextVec&>().sortedView())>;

#endif  // QLEVER_SRC_INDEX_TEXTBLOCKWRITER_H
