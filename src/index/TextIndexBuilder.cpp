// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "index/TextIndexBuilder.h"

#include "index/TextIndexReadWrite.h"

// _____________________________________________________________________________
void TextIndexBuilder::buildTextIndexFile(
    const std::optional<std::pair<std::string, std::string>>& wordsAndDocsFile,
    bool addWordsFromLiterals, TextScoringMetric textScoringMetric,
    std::pair<float, float> bAndKForBM25) {
  AD_CORRECTNESS_CHECK(wordsAndDocsFile.has_value() || addWordsFromLiterals);
  LOG(INFO) << std::endl;
  LOG(INFO) << "Adding text index ..." << std::endl;
  std::string indexFilename = onDiskBase_ + ".text.index";
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
  WordTextVec wordTextVec{indexFilename + ".word-text-vec-sorter.tmp",
                          memoryLimitIndexBuilding() / 4, allocator_};
  EntityTextVec entityTextVec{indexFilename + ".entity-text-vec-sorter.tmp",
                              memoryLimitIndexBuilding() / 4, allocator_};
  // This fills both vectors
  processWordsForInvertedLists(wordsFile, addWordsFromLiterals, wordTextVec,
                               entityTextVec);

  // Create the text index and write to file
  createTextIndex(indexFilename, wordTextVec, entityTextVec);
  openTextFileHandle();
}

// _____________________________________________________________________________
size_t TextIndexBuilder::processWordsForVocabulary(
    const std::string& contextFile, bool addWordsFromLiterals) {
  size_t numLines = 0;
  ad_utility::HashSet<std::string> distinctWords;
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
void TextIndexBuilder::processWordsForInvertedLists(
    const std::string& contextFile, bool addWordsFromLiterals,
    WordTextVec& wordTextVec, EntityTextVec& entityTextVec) {
  LOG(TRACE) << "BEGIN IndexImpl::passContextFileIntoVector" << std::endl;
  WordMap wordsInContext;
  EntityMap entitiesInContext;
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
      addContextToVectors(wordTextVec, entityTextVec, currentContext,
                          wordsInContext, entitiesInContext);
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
  addContextToVectors(wordTextVec, entityTextVec, currentContext,
                      wordsInContext, entitiesInContext);
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
    ad_utility::HashSet<std::string> items;
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
    const WordsFileLine& line, EntityMap& entitiesInContext,
    size_t& nofLiterals, size_t& entityNotFoundErrorMsgCount) const {
  VocabIndex eid;
  // TODO<joka921> Currently only IRIs and strings from the vocabulary can
  // be tagged entities in the text index (no doubles, ints, etc).
  if (getVocab().getId(line.word_, &eid)) {
    // Note that `entitiesInContext` is a HashMap, so the `Id`s don't have
    // to be contiguous.
    entitiesInContext[eid] += line.score_;
    if (line.isLiteralEntity_) {
      ++nofLiterals;
    }
  } else {
    logEntityNotFound(line.word_, entityNotFoundErrorMsgCount);
  }
}

// _____________________________________________________________________________
void TextIndexBuilder::processWordCaseDuringInvertedListProcessing(
    const WordsFileLine& line, WordMap& wordsInContext,
    ScoreData& scoreData) const {
  // TODO<joka921> Let the `textVocab_` return a `WordIndex` directly.
  WordVocabIndex vid;
  bool ret = textVocab_.getId(line.word_, &vid);
  if (!ret) {
    LOG(ERROR) << "ERROR: word \"" << line.word_ << "\" "
               << "not found in textVocab. Terminating\n";
    AD_FAIL();
  }
  if (scoreData.getScoringMetric() == TextScoringMetric::EXPLICIT) {
    wordsInContext[vid] += line.score_;
  } else {
    wordsInContext[vid] = scoreData.getScore(vid, line.contextId_);
  }
}

// _____________________________________________________________________________
void TextIndexBuilder::logEntityNotFound(const std::string& word,
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
void TextIndexBuilder::addContextToVectors(WordTextVec& wordTextVec,
                                           EntityTextVec& entityTextVec,
                                           TextRecordIndex context,
                                           const WordMap& words,
                                           const EntityMap& entities) {
  // Add all words to respective vector
  ql::ranges::for_each(words, [&](const auto& word) {
    wordTextVec.push(std::array{Id::makeFromWordVocabIndex(word.first),
                                Id::makeFromTextRecordIndex(context),
                                Id::makeFromDouble(word.second)});
    ql::ranges::for_each(entities, [&](const auto& entity) {
      entityTextVec.push(std::array{Id::makeFromWordVocabIndex(word.first),
                                    Id::makeFromTextRecordIndex(context),
                                    Id::makeFromVocabIndex(entity.first),
                                    Id::makeFromDouble(entity.second)});
    });
  });
}

// _____________________________________________________________________________
void TextIndexBuilder::createTextIndex(const std::string& filename,
                                       WordTextVec& wordTextVec,
                                       EntityTextVec& entityTextVec) {
  TextBlockWriter::writeTextIndexFile(filename, wordTextVec, entityTextVec,
                                      textScoringMetric_, textMeta_,
                                      nofWordPostingsInTextBlock_);
}

// _____________________________________________________________________________
void TextIndexBuilder::buildDocsDB(const std::string& docsFileName) const {
  LOG(INFO) << "Building DocsDB...\n";
  std::ifstream docsFile{docsFileName};
  std::ofstream ofs{onDiskBase_ + ".text.docsDB"};
  // To avoid excessive use of RAM,
  // we write the offsets to and `ad_utility::MmapVector` first;
  ad_utility::MmapVectorTmp<off_t> offsets{onDiskBase_ + ".text.docsDB.tmp"};
  off_t currentOffset = 0;
  uint64_t currentContextId = 0;
  std::string line;
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
