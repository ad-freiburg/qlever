// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "index/TextBlockWriter.h"

#include "index/TextIndexReadWrite.h"

void TextBlockWriter::calculateAndWriteTextBlocks(
    size_t nofWordPostingsInTextBlock) {
  AD_CONTRACT_CHECK(
      nofWordPostingsInTextBlock > 0,
      "Number of word postings in text block has to be larger than zero.");
  auto currentWordTextVecWordIndex = WordVocabIndex::make(0);
  for (auto&& chunk :
       ::ranges::views::chunk(wordTextVecView_, nofWordPostingsInTextBlock)) {
    for (const auto& row : chunk) {
      currentWordTextVecWordIndex = row[0].getWordVocabIndex();
      addWordPosting(row);
    }
    finishBlock(currentWordTextVecWordIndex);
  }
}

// _____________________________________________________________________________
void TextBlockWriter::finishBlock(WordVocabIndex upperBoundWordVocabIndex) {
  // Add all co-occurring entities to entityPostings_
  auto entityPostings =
      addEntityPostingsUpToWordIndex(upperBoundWordVocabIndex);

  // This is possible since the wordPostings are filled in ascending WordIndex
  // order.
  WordVocabIndex minWordIndexOfBlock = std::get<1>(wordPostings_.front());

  // Sort both posting vectors by TextRecordIndex, wordIndex (or entityIndex),
  // score
  ql::ranges::sort(wordPostings_);
  ql::ranges::sort(entityPostings);

  // Filter out duplicate entity postings. The reason for duplicates is the
  // following case: Different words that co-occur with the same entities in
  // the same text record appear in the same text block.
  // An example would be a text record "He <Newton> helped" and the words
  // "he" and "helped" are put in the same block.
  entityPostings.erase(std::ranges::unique(entityPostings).begin(),
                       entityPostings.end());

  writeTextBlockToFile(wordPostings_, entityPostings, out_, minWordIndexOfBlock,
                       upperBoundWordVocabIndex);

  // Reset Variables
  wordPostings_.clear();
}

// _____________________________________________________________________________
std::vector<EntityPosting> TextBlockWriter::addEntityPostingsUpToWordIndex(
    WordVocabIndex upperBoundWordVocabIndex) {
  if (entityTextVecIterator_ == entityTextVecSentinel_) {
    return {};
  }

  std::vector<EntityPosting> entityPostings;

  auto getWVI = [](const auto& row) { return row[0].getWordVocabIndex(); };

  for (; entityTextVecIterator_ != entityTextVecSentinel_;
       ++entityTextVecIterator_) {
    WordVocabIndex currentEntityTextVecWordIndex =
        getWVI((*entityTextVecIterator_));
    if (currentEntityTextVecWordIndex > upperBoundWordVocabIndex) {
      break;
    }
    addEntityPosting(entityPostings, *entityTextVecIterator_);
  }
  return entityPostings;
}

// _____________________________________________________________________________
template <typename EntityRow>
void TextBlockWriter::addEntityPosting(std::vector<EntityPosting>& vecToAddTo,
                                       const EntityRow& entityTextVecRow) {
  vecToAddTo.emplace_back(entityTextVecRow[1].getTextRecordIndex(),
                          entityTextVecRow[2].getVocabIndex(),
                          entityTextVecRow[3].getDouble());
}

// _____________________________________________________________________________
template <typename WordRow>
void TextBlockWriter::addWordPosting(const WordRow& wordTextVecRow) {
  wordPostings_.emplace_back(wordTextVecRow[1].getTextRecordIndex(),
                             wordTextVecRow[0].getWordVocabIndex(),
                             wordTextVecRow[2].getDouble());
}

// _____________________________________________________________________________
void TextBlockWriter::writeTextBlockToFile(
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
