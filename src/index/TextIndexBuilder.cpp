// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "index/TextIndexBuilder.h"

#include "index/TextIndexReadWrite.h"

// _____________________________________________________________________________
void TextIndexBuilder::buildTextIndexFile(
    const std::optional<std::pair<string, string>>& wordsAndDocsFile,
    bool addWordsFromLiterals, TextScoringMetric textScoringMetric,
    std::pair<float, float> bAndKForBM25) {
  AD_CORRECTNESS_CHECK(wordsAndDocsFile.has_value() || addWordsFromLiterals);
  LOG(INFO) << std::endl;
  LOG(INFO) << "Adding text index ..." << std::endl;
  string indexFilename = onDiskBase_ + ".text.index";
  bool addFromWordAndDocsFile = wordsAndDocsFile.has_value();
  const auto& [wordsFile, docsFile] =
      !addFromWordAndDocsFile ? std::pair{"", ""} : wordsAndDocsFile.value();
  // Either read words from given files or consider each literal as text record
  // or both (but at least one of them, otherwise this function is not called)
  if (addFromWordAndDocsFile) {
    AD_CORRECTNESS_CHECK(!(wordsFile.empty() || docsFile.empty()));
    LOG(INFO) << "Reading words from wordsfile \"" << wordsFile << "\""
              << " and from docsFile \"" << docsFile << "\"" << std::endl;
  }
  if (addWordsFromLiterals) {
    LOG(INFO) << (!addFromWordAndDocsFile ? "C" : "Additionally c")
              << "onsidering each literal as a text record" << std::endl;
  }
  // We have deleted the vocabulary during the index creation to save RAM, so
  // now we have to reload it. Also, when IndexBuilderMain is called with option
  // -A (add text index), this is the first thing we do .
  //
  // NOTE: In the previous version of the code (where the only option was to
  // read from a wordsfile), this as done in `processWordsForInvertedLists`.
  // That is, when we now call call `processWordsForVocabulary` (which builds
  // the text vocabulary), we already have the KB vocabular in RAM as well.
  LOG(DEBUG) << "Reloading the RDF vocabulary ..." << std::endl;
  vocab_ = RdfsVocabulary{};
  readConfiguration();
  {
    auto [b, k] = bAndKForBM25;
    storeTextScoringParamsInConfiguration(textScoringMetric, b, k);
  }
  vocab_.readFromFile(onDiskBase_ + VOCAB_SUFFIX);

  scoreData_ = {vocab_.getLocaleManager(), textScoringMetric_,
                bAndKParamForTextScoring_};

  // Build the text vocabulary (first scan over the text records).
  processWordsForVocabulary(wordsFile, addWordsFromLiterals);
  // Calculate the score data for the words
  scoreData_.calculateScoreData(docsFile, addWordsFromLiterals, textVocab_,
                                vocab_);
  // Build the half-inverted lists (second scan over the text records).
  LOG(INFO) << "Building the half-inverted index lists ..." << std::endl;
  calculateBlockBoundaries();
  TextVec vec{indexFilename + ".text-vec-sorter.tmp",
              memoryLimitIndexBuilding() / 3, allocator_};
  processWordsForInvertedLists(wordsFile, addWordsFromLiterals, vec);
  createTextIndex(indexFilename, vec);
  openTextFileHandle();
}

// _____________________________________________________________________________
size_t TextIndexBuilder::processWordsForVocabulary(string const& contextFile,
                                                   bool addWordsFromLiterals) {
  size_t numLines = 0;
  ad_utility::HashSet<string> distinctWords;
  for (const auto& line :
       wordsInTextRecords(contextFile, addWordsFromLiterals)) {
    ++numLines;
    if (!line.isEntity_) {
      distinctWords.insert(line.word_);
    }
  }
  textVocab_.createFromSet(distinctWords, onDiskBase_ + ".text.vocabulary");
  return numLines;
}

// _____________________________________________________________________________
void TextIndexBuilder::processWordsForInvertedLists(const string& contextFile,
                                                    bool addWordsFromLiterals,
                                                    TextVec& vec) {
  LOG(TRACE) << "BEGIN IndexImpl::passContextFileIntoVector" << std::endl;
  ad_utility::HashMap<WordIndex, Score> wordsInContext;
  ad_utility::HashMap<Id, Score> entitiesInContext;
  auto currentContext = TextRecordIndex::make(0);
  // The nofContexts can be misleading since it also counts empty contexts
  size_t nofContexts = 0;
  size_t nofWordPostings = 0;
  size_t nofEntityPostings = 0;
  size_t entityNotFoundErrorMsgCount = 0;
  size_t nofLiterals = 0;

  for (const auto& line :
       wordsInTextRecords(contextFile, addWordsFromLiterals)) {
    if (line.contextId_ != currentContext) {
      ++nofContexts;
      addContextToVector(vec, currentContext, wordsInContext,
                         entitiesInContext);
      currentContext = line.contextId_;
      wordsInContext.clear();
      entitiesInContext.clear();
    }
    if (line.isEntity_) {
      ++nofEntityPostings;
      processEntityCaseDuringInvertedListProcessing(
          line, entitiesInContext, nofLiterals, entityNotFoundErrorMsgCount);
    } else {
      ++nofWordPostings;
      processWordCaseDuringInvertedListProcessing(line, wordsInContext,
                                                  scoreData_);
    }
  }
  if (entityNotFoundErrorMsgCount > 0) {
    LOG(WARN) << "Number of mentions of entities not found in the vocabulary: "
              << entityNotFoundErrorMsgCount << std::endl;
  }
  LOG(DEBUG) << "Number of total entity mentions: " << nofEntityPostings
             << std::endl;
  ++nofContexts;
  addContextToVector(vec, currentContext, wordsInContext, entitiesInContext);
  textMeta_.setNofTextRecords(nofContexts);
  textMeta_.setNofWordPostings(nofWordPostings);
  textMeta_.setNofEntityPostings(nofEntityPostings);
  nofNonLiteralsInTextIndex_ = nofContexts - nofLiterals;
  configurationJson_["num-non-literals-text-index"] =
      nofNonLiteralsInTextIndex_;
  writeConfiguration();

  LOG(TRACE) << "END IndexImpl::passContextFileIntoVector" << std::endl;
}

// _____________________________________________________________________________
cppcoro::generator<WordsFileLine> TextIndexBuilder::wordsInTextRecords(
    std::string contextFile, bool addWordsFromLiterals) const {
  auto localeManager = textVocab_.getLocaleManager();
  // ROUND 1: If context file aka wordsfile is not empty, read words from there.
  // Remember the last context id for the (optional) second round.
  TextRecordIndex contextId = TextRecordIndex::make(0);
  if (!contextFile.empty()) {
    WordsFileParser p(contextFile, localeManager);
    ad_utility::HashSet<string> items;
    for (auto& line : p) {
      contextId = line.contextId_;
      co_yield line;
    }
    if (contextId > TextRecordIndex::make(0)) {
      contextId = contextId.incremented();
    }
  }
  // ROUND 2: Optionally, consider each literal from the internal vocabulary as
  // a text record.
  if (addWordsFromLiterals) {
    for (VocabIndex index = VocabIndex::make(0); index.get() < vocab_.size();
         index = index.incremented()) {
      auto text = vocab_[index];
      if (!isLiteral(text)) {
        continue;
      }

      // We need the explicit cast to `std::string` because the return type of
      // `indexToString` might be `string_view` if the vocabulary is stored
      // uncompressed in memory.
      WordsFileLine entityLine{std::string{text}, true, contextId, 1, true};
      co_yield entityLine;
      std::string_view textView = text;
      textView = textView.substr(0, textView.rfind('"'));
      textView.remove_prefix(1);
      for (auto word : tokenizeAndNormalizeText(textView, localeManager)) {
        WordsFileLine wordLine{std::move(word), false, contextId, 1};
        co_yield wordLine;
      }
      contextId = contextId.incremented();
    }
  }
}

// _____________________________________________________________________________
void TextIndexBuilder::processEntityCaseDuringInvertedListProcessing(
    const WordsFileLine& line,
    ad_utility::HashMap<Id, Score>& entitiesInContext, size_t& nofLiterals,
    size_t& entityNotFoundErrorMsgCount) const {
  VocabIndex eid;
  // TODO<joka921> Currently only IRIs and strings from the vocabulary can
  // be tagged entities in the text index (no doubles, ints, etc).
  if (getVocab().getId(line.word_, &eid)) {
    // Note that `entitiesInContext` is a HashMap, so the `Id`s don't have
    // to be contiguous.
    entitiesInContext[Id::makeFromVocabIndex(eid)] += line.score_;
    if (line.isLiteralEntity_) {
      ++nofLiterals;
    }
  } else {
    logEntityNotFound(line.word_, entityNotFoundErrorMsgCount);
  }
}

// _____________________________________________________________________________
void TextIndexBuilder::processWordCaseDuringInvertedListProcessing(
    const WordsFileLine& line,
    ad_utility::HashMap<WordIndex, Score>& wordsInContext,
    ScoreData& scoreData) const {
  // TODO<joka921> Let the `textVocab_` return a `WordIndex` directly.
  WordVocabIndex vid;
  bool ret = textVocab_.getId(line.word_, &vid);
  WordIndex wid = vid.get();
  if (!ret) {
    LOG(ERROR) << "ERROR: word \"" << line.word_ << "\" "
               << "not found in textVocab. Terminating\n";
    AD_FAIL();
  }
  if (scoreData.getScoringMetric() == TextScoringMetric::EXPLICIT) {
    wordsInContext[wid] += line.score_;
  } else {
    wordsInContext[wid] = scoreData.getScore(wid, line.contextId_);
  }
}

// _____________________________________________________________________________
void TextIndexBuilder::logEntityNotFound(const string& word,
                                         size_t& entityNotFoundErrorMsgCount) {
  if (entityNotFoundErrorMsgCount < 20) {
    LOG(WARN) << "Entity from text not in KB: " << word << '\n';
    if (++entityNotFoundErrorMsgCount == 20) {
      LOG(WARN) << "There are more entities not in the KB..."
                << " suppressing further warnings...\n";
    }
  } else {
    entityNotFoundErrorMsgCount++;
  }
}

// _____________________________________________________________________________
void TextIndexBuilder::addContextToVector(
    TextVec& vec, TextRecordIndex context,
    const ad_utility::HashMap<WordIndex, Score>& words,
    const ad_utility::HashMap<Id, Score>& entities) const {
  // Determine blocks for each word and each entity.
  // Add the posting to each block.
  ad_utility::HashSet<TextBlockIndex> touchedBlocks;
  ql::ranges::for_each(words, [&](const auto& word) {
    TextBlockIndex blockId = getWordBlockId(word.first);
    touchedBlocks.insert(blockId);
    vec.push(std::array{Id::makeFromInt(blockId), Id::makeFromBool(false),
                        Id::makeFromInt(context.get()),
                        Id::makeFromInt(word.first),
                        Id::makeFromDouble(word.second)});
  });

  // All entities have to be written in the entity list part for each block.
  // Ensure that they are added only once for each block.
  // For example, there could be both words computer and computing
  // in the same context. Still, co-occurring entities would only have to be
  // written to a comp* block once.
  for (TextBlockIndex blockId : touchedBlocks) {
    for (auto it = entities.begin(); it != entities.end(); ++it) {
      AD_CONTRACT_CHECK(it->first.getDatatype() == Datatype::VocabIndex);
      vec.push(std::array{Id::makeFromInt(blockId), Id::makeFromBool(true),
                          Id::makeFromInt(context.get()),
                          Id::makeFromInt(it->first.getVocabIndex().get()),
                          Id::makeFromDouble(it->second)});
    }
  }
}

// _____________________________________________________________________________
void TextIndexBuilder::createTextIndex(const string& filename, TextVec& vec) {
  ad_utility::File out(filename.c_str(), "w");
  currenttOffset_ = 0;
  // Detect block boundaries from the main key of the vec.
  // Write the data for each block.
  // First, there's the classic lists, then the additional entity ones.
  TextBlockIndex currentBlockIndex = 0;
  WordIndex currentMinWordIndex = std::numeric_limits<WordIndex>::max();
  WordIndex currentMaxWordIndex = std::numeric_limits<WordIndex>::min();
  vector<Posting> classicPostings;
  vector<Posting> entityPostings;
  for (const auto& value : vec.sortedView()) {
    TextBlockIndex textBlockIndex = value[0].getInt();
    bool flag = value[1].getBool();
    TextRecordIndex textRecordIndex = TextRecordIndex::make(value[2].getInt());
    WordOrEntityIndex wordOrEntityIndex = value[3].getInt();
    Score score = static_cast<Score>(value[4].getDouble());
    if (textBlockIndex != currentBlockIndex) {
      AD_CONTRACT_CHECK(!classicPostings.empty());
      bool scoreIsInt = textScoringMetric_ == TextScoringMetric::EXPLICIT;
      ContextListMetaData classic = textIndexReadWrite::writePostings(
          out, classicPostings, currenttOffset_, scoreIsInt);
      ContextListMetaData entity = textIndexReadWrite::writePostings(
          out, entityPostings, currenttOffset_, scoreIsInt);
      textMeta_.addBlock(TextBlockMetaData(
          currentMinWordIndex, currentMaxWordIndex, classic, entity));
      classicPostings.clear();
      entityPostings.clear();
      currentBlockIndex = textBlockIndex;
      currentMinWordIndex = wordOrEntityIndex;
      currentMaxWordIndex = wordOrEntityIndex;
    }
    if (!flag) {
      classicPostings.emplace_back(textRecordIndex, wordOrEntityIndex, score);
      if (wordOrEntityIndex < currentMinWordIndex) {
        currentMinWordIndex = wordOrEntityIndex;
      }
      if (wordOrEntityIndex > currentMaxWordIndex) {
        currentMaxWordIndex = wordOrEntityIndex;
      }

    } else {
      entityPostings.emplace_back(textRecordIndex, wordOrEntityIndex, score);
    }
  }
  // Write the last block
  AD_CONTRACT_CHECK(!classicPostings.empty());
  bool scoreIsInt = textScoringMetric_ == TextScoringMetric::EXPLICIT;
  ContextListMetaData classic = textIndexReadWrite::writePostings(
      out, classicPostings, currenttOffset_, scoreIsInt);
  ContextListMetaData entity = textIndexReadWrite::writePostings(
      out, entityPostings, currenttOffset_, scoreIsInt);
  textMeta_.addBlock(TextBlockMetaData(currentMinWordIndex, currentMaxWordIndex,
                                       classic, entity));
  classicPostings.clear();
  entityPostings.clear();
  LOG(DEBUG) << "Done creating text index." << std::endl;
  LOG(INFO) << "Statistics for text index: " << textMeta_.statistics()
            << std::endl;

  LOG(DEBUG) << "Writing Meta data to index file ..." << std::endl;
  ad_utility::serialization::FileWriteSerializer serializer{std::move(out)};
  serializer << textMeta_;
  out = std::move(serializer).file();
  off_t startOfMeta = textMeta_.getOffsetAfter();
  out.write(&startOfMeta, sizeof(startOfMeta));
  out.close();
  LOG(INFO) << "Text index build completed" << std::endl;
}

template <typename I, typename BlockBoundaryAction>
void TextIndexBuilder::calculateBlockBoundariesImpl(
    I&& index, size_t blockSize,
    const BlockBoundaryAction& blockBoundaryAction) {
  LOG(TRACE) << "BEGIN IndexImpl::calculateBlockBoundaries" << std::endl;
  // The blocks are set in a way that all blocks contain blockSize nof elements
  // except the last one which could contain less.

  size_t vocabSize = index.textVocab_.size();
  if (vocabSize == 0) {
    LOG(WARN) << "You are trying to call calculateBlockBoundaries on an empty "
                 "text vocabulary\n";
    return;
  }
  size_t numBlocks =
      std::ceil(static_cast<float>(vocabSize) / static_cast<float>(blockSize));
  for (size_t currentId = blockSize - 1; currentId < vocabSize - 1;
       currentId += blockSize) {
    blockBoundaryAction(currentId);
  }
  blockBoundaryAction(vocabSize - 1);
  LOG(DEBUG) << "Block boundaries computed: #blocks = " << numBlocks
             << ", #words = " << vocabSize << std::endl;
}

// _____________________________________________________________________________
void TextIndexBuilder::calculateBlockBoundaries() {
  blockBoundaries_.clear();
  auto addToBlockBoundaries = [this](size_t i) {
    blockBoundaries_.push_back(i);
  };
  return calculateBlockBoundariesImpl(*this, textBlockSize_,
                                      addToBlockBoundaries);
}

// _____________________________________________________________________________
void TextIndexBuilder::buildDocsDB(const string& docsFileName) const {
  LOG(INFO) << "Building DocsDB...\n";
  std::ifstream docsFile{docsFileName};
  std::ofstream ofs{onDiskBase_ + ".text.docsDB"};
  // To avoid excessive use of RAM,
  // we write the offsets to and `ad_utility::MmapVector` first;
  ad_utility::MmapVectorTmp<off_t> offsets{onDiskBase_ + ".text.docsDB.tmp"};
  off_t currentOffset = 0;
  uint64_t currentContextId = 0;
  string line;
  line.reserve(BUFFER_SIZE_DOCSFILE_LINE);
  while (std::getline(docsFile, line)) {
    std::string_view lineView = line;
    size_t tab = lineView.find('\t');
    uint64_t contextId = 0;
    // Get contextId from line
    std::from_chars(lineView.data(), lineView.data() + tab, contextId);
    // Set lineView to the docText
    lineView = lineView.substr(tab + 1);
    ofs << lineView;
    while (currentContextId < contextId) {
      offsets.push_back(currentOffset);
      currentContextId++;
    }
    offsets.push_back(currentOffset);
    currentContextId++;
    currentOffset += static_cast<off_t>(lineView.size());
  }
  offsets.push_back(currentOffset);
  ofs.write(reinterpret_cast<const char*>(offsets.data()),
            sizeof(off_t) * offsets.size());
  LOG(INFO) << "DocsDB done.\n";
}
