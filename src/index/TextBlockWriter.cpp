#include "TextBlockWriter.h"

#include "index/TextIndexReadWrite.h"

template <typename WordTextVecView, typename EntityTextVecView>
void TextBlockWriter<WordTextVecView, EntityTextVecView>::
    calculateAndWriteTextBlocks(size_t nofWordPostingsInTextBlock) {
  AD_CONTRACT_CHECK(
      nofWordPostingsInTextBlock > 0,
      "Number of word postings in text block has to be larger than zero.");
  for (auto&& chunk :
       ::ranges::views::chunk(wordTextVecView_, nofWordPostingsInTextBlock)) {
    for (const auto& row : chunk) {
      currentWordTextVecWordIndex_ = row[0].getWordVocabIndex();
      addWordPosting(row);
    }
    addBlock();
  }
}

// _____________________________________________________________________________
template <typename WordTextVecView, typename EntityTextVecView>
void TextBlockWriter<WordTextVecView, EntityTextVecView>::addBlock() {
  // Add all co-occurring entities to entityPostings_
  addEntityPostingsUpToWordIndex();

  // This is possible since the wordPostings are filled in ascending WordIndex
  // order.
  WordVocabIndex minWordIndexOfBlock = std::get<1>(wordPostings_.front());
  WordVocabIndex maxWordIndexOfBlock = std::get<1>(wordPostings_.back());

  // Sort both posting vectors by TextRecordIndex, wordIndex (or entityIndex),
  // score
  ql::ranges::sort(wordPostings_);
  ql::ranges::sort(entityPostings_);

  // Filter out duplicate entity postings. The reason for duplicates is the
  // following case: Different words that co-occur with the same entities in
  // the same text record appear in the same text block.
  // An example would be a text record "He <Newton> helped" and the words
  // "he" and "helped" are put in the same block.
  entityPostings_.erase(std::ranges::unique(entityPostings_).begin(),
                        entityPostings_.end());

  writeTextBlockToFile(wordPostings_, entityPostings_, out_,
                       minWordIndexOfBlock, maxWordIndexOfBlock);

  // Reset Variables
  wordPostings_.clear();
  entityPostings_.clear();
}

// _____________________________________________________________________________
template <typename WordTextVecView, typename EntityTextVecView>
void TextBlockWriter<WordTextVecView,
                     EntityTextVecView>::addEntityPostingsUpToWordIndex() {
  entityTextVecIterator_ = entityStartWordIdIterator_;
  if (entityTextVecIterator_ == entityTextVecSentinel_) {
    return;
  }

  bool startOfLastWordIdSet = false;

  WordVocabIndex currentEntityTextVecWordIndex =
      ((*entityTextVecIterator_)[0].getWordVocabIndex());
  for (; entityTextVecIterator_ != entityTextVecSentinel_ &&
         currentEntityTextVecWordIndex < currentWordTextVecWordIndex_;
       ++entityTextVecIterator_) {
    currentEntityTextVecWordIndex =
        (*entityTextVecIterator_)[0].getWordVocabIndex();
    if (currentEntityTextVecWordIndex == currentWordTextVecWordIndex_ &&
        !startOfLastWordIdSet) {
      entityStartWordIdIterator_ = entityTextVecIterator_;
      startOfLastWordIdSet = true;
    }
    addEntityPosting(*entityTextVecIterator_);
  }
}

// _____________________________________________________________________________
template <typename WordTextVecView, typename EntityTextVecView>
template <typename EntityRow>
void TextBlockWriter<WordTextVecView, EntityTextVecView>::addEntityPosting(
    const EntityRow& entityTextVecRow) {
  entityPostings_.emplace_back(entityTextVecRow[1].getTextRecordIndex(),
                               entityTextVecRow[2].getVocabIndex(),
                               entityTextVecRow[3].getDouble());
}

// _____________________________________________________________________________
template <typename WordTextVecView, typename EntityTextVecView>
template <typename WordRow>
void TextBlockWriter<WordTextVecView, EntityTextVecView>::addWordPosting(
    const WordRow& wordTextVecRow) {
  wordPostings_.emplace_back(wordTextVecRow[1].getTextRecordIndex(),
                             wordTextVecRow[0].getWordVocabIndex(),
                             wordTextVecRow[2].getDouble());
}

// _____________________________________________________________________________
template <typename WordTextVecView, typename EntityTextVecView>
void TextBlockWriter<WordTextVecView, EntityTextVecView>::writeTextBlockToFile(
    const std::vector<WordPosting>& wordPostings,
    const std::vector<EntityPosting>& entityPostings, ad_utility::File& out,
    WordVocabIndex minWordIndexOfBlock, WordVocabIndex maxWordIndexOfBlock) {
  const bool scoreIsInt = textScoringMetric_ == TextScoringMetric::EXPLICIT;
  ContextListMetaData classic = textIndexReadWrite::writePostings(
      out, wordPostings, currentOffset_, scoreIsInt);
  ContextListMetaData entity = textIndexReadWrite::writePostings(
      out, entityPostings, currentOffset_, scoreIsInt);
  textMeta_.addBlock(TextBlockMetaData(minWordIndexOfBlock, maxWordIndexOfBlock,
                                       classic, entity));
}

// Explicit instantiation for the specific types used
template struct TextBlockWriter<
    decltype(std::declval<WordTextVec&>().sortedView()),
    decltype(std::declval<EntityTextVec&>().sortedView())>;
