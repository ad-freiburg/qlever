// Copyright 2015 - 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Björn Buchhold <buchhold@cs.uni-freiburg.de>
//          Johannes Kalmbach <johannes.kalmbach@gmail.com>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "index/IndexImpl.h"

#include <absl/strings/str_split.h>

#include <charconv>
#include <ranges>
#include <stxxl/algorithm>
#include <tuple>
#include <utility>

#include "backports/algorithm.h"
#include "engine/CallFixedSize.h"
#include "index/FTSAlgorithms.h"
#include "index/TextIndexReadWrite.h"
#include "parser/WordsAndDocsFileParser.h"
#include "util/Conversions.h"

// _____________________________________________________________________________
cppcoro::generator<WordsFileLine> IndexImpl::wordsInTextRecords(
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
      WordsFileLine entityLine{text, true, contextId, 1, true};
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

void IndexImpl::processEntityCaseDuringInvertedListProcessing(
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
void IndexImpl::processWordCaseDuringInvertedListProcessing(
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
  if (scoreData.getScoringMetric() == TextScoringMetric::COUNT) {
    wordsInContext[wid] += line.score_;
  } else {
    wordsInContext[wid] = scoreData.getScore(wid, line.contextId_);
  }
}

// _____________________________________________________________________________
void IndexImpl::logEntityNotFound(const string& word,
                                  size_t& entityNotFoundErrorMsgCount) const {
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
void IndexImpl::buildTextIndexFile(
    const std::pair<string, string>& wordsAndDocsFile,
    bool addWordsFromLiterals) {
  LOG(INFO) << std::endl;
  LOG(INFO) << "Adding text index ..." << std::endl;
  string indexFilename = onDiskBase_ + ".text.index";
  // Either read words from given files or consider each literal as text record
  // or both (but at least one of them, otherwise this function is not called).
  if (!(wordsAndDocsFile.first.empty() && wordsAndDocsFile.second.empty())) {
    LOG(INFO) << "Reading words from wordsfile \"" << wordsAndDocsFile.first
              << "\""
              << " and from docsFile \"" << wordsAndDocsFile.second << "\""
              << std::endl;
  }
  if (addWordsFromLiterals) {
    LOG(INFO) << ((wordsAndDocsFile.first.empty() &&
                   wordsAndDocsFile.second.empty())
                      ? "C"
                      : "Additionally c")
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
  vocab_.readFromFile(onDiskBase_ + VOCAB_SUFFIX);

  scoreData_ = {vocab_.getLocaleManager(), textScoringMetric_,
                bAndKParamForTextScoring_};

  // Build the text vocabulary (first scan over the text records).
  size_t nofLines =
      processWordsForVocabulary(wordsAndDocsFile.first, addWordsFromLiterals);
  // Calculate the score data for the words
  scoreData_.calculateScoreData(wordsAndDocsFile.second, addWordsFromLiterals,
                                textVocab_, vocab_);
  // Build the half-inverted lists (second scan over the text records).
  LOG(INFO) << "Building the half-inverted index lists ..." << std::endl;
  calculateBlockBoundaries();
  TextVec v;
  v.reserve(nofLines);
  processWordsForInvertedLists(wordsAndDocsFile.first, addWordsFromLiterals, v);
  LOG(DEBUG) << "Sorting text index, #elements = " << v.size() << std::endl;
  stxxl::sort(begin(v), end(v), SortText(),
              memoryLimitIndexBuilding().getBytes() / 3);
  LOG(DEBUG) << "Sort done" << std::endl;
  createTextIndex(indexFilename, v);
  openTextFileHandle();
}

// _____________________________________________________________________________
void IndexImpl::buildDocsDB(const string& docsFileName) const {
  LOG(INFO) << "Building DocsDB...\n";
  std::ifstream docsFile{docsFileName};
  std::ofstream ofs(onDiskBase_ + ".text.docsDB", std::ios_base::out);
  // To avoid excessive use of RAM,
  // we write the offsets to and stxxl:vector first;
  stxxl::vector<off_t> offsets;
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

  ofs.close();
  // Now append the tmp file to the docsDB file.
  ad_utility::File out(onDiskBase_ + ".text.docsDB", "a");
  for (size_t i = 0; i < offsets.size(); ++i) {
    off_t cur = offsets[i];
    out.write(&cur, sizeof(cur));
  }
  out.close();
  LOG(INFO) << "DocsDB done.\n";
}

// _____________________________________________________________________________
void IndexImpl::addTextFromOnDiskIndex() {
  // Read the text vocabulary (into RAM).
  textVocab_.readFromFile(onDiskBase_ + ".text.vocabulary");

  // Initialize the text index.
  std::string textIndexFileName = onDiskBase_ + ".text.index";
  LOG(INFO) << "Reading metadata from file " << textIndexFileName << " ..."
            << std::endl;
  textIndexFile_.open(textIndexFileName.c_str(), "r");
  AD_CONTRACT_CHECK(textIndexFile_.isOpen());
  off_t metaFrom;
  [[maybe_unused]] off_t metaTo = textIndexFile_.getLastOffset(&metaFrom);
  ad_utility::serialization::FileReadSerializer serializer(
      std::move(textIndexFile_));
  serializer.setSerializationPosition(metaFrom);
  serializer >> textMeta_;
  textIndexFile_ = std::move(serializer).file();
  LOG(INFO) << "Registered text index: " << textMeta_.statistics() << std::endl;
  // Initialize the text records file aka docsDB. NOTE: The search also works
  // without this, but then there is no content to show when a text record
  // matches. This is perfectly fine when the text records come from IRIs or
  // literals from our RDF vocabulary.
  std::string docsDbFileName = onDiskBase_ + ".text.docsDB";
  std::ifstream f(docsDbFileName.c_str());
  if (f.good()) {
    f.close();
    docsDB_.init(string(onDiskBase_ + ".text.docsDB"));
    LOG(INFO) << "Registered text records: #records = " << docsDB_._size
              << std::endl;
  } else {
    LOG(DEBUG) << "No file \"" << docsDbFileName
               << "\" with additional text records" << std::endl;
    f.close();
  }
}

// _____________________________________________________________________________
size_t IndexImpl::processWordsForVocabulary(string const& contextFile,
                                            bool addWordsFromLiterals) {
  size_t numLines = 0;
  ad_utility::HashSet<string> distinctWords;
  for (auto line : wordsInTextRecords(contextFile, addWordsFromLiterals)) {
    ++numLines;
    // LOG(INFO) << "LINE: "
    //           << std::setw(50) << line.word_ << "   "
    //           << line.isEntity_ << "\t"
    //           << line.contextId_.get() << "\t"
    //           << line.score_ << std::endl;
    if (!line.isEntity_) {
      distinctWords.insert(line.word_);
    }
  }
  textVocab_.createFromSet(distinctWords, onDiskBase_ + ".text.vocabulary");
  return numLines;
}

// _____________________________________________________________________________
void IndexImpl::processWordsForInvertedLists(const string& contextFile,
                                             bool addWordsFromLiterals,
                                             IndexImpl::TextVec& vec) {
  LOG(TRACE) << "BEGIN IndexImpl::passContextFileIntoVector" << std::endl;
  TextVec::bufwriter_type writer(vec);
  ad_utility::HashMap<WordIndex, Score> wordsInContext;
  ad_utility::HashMap<Id, Score> entitiesInContext;
  auto currentContext = TextRecordIndex::make(0);
  // The nofContexts can be misleading since it also counts empty contexts
  size_t nofContexts = 0;
  size_t nofWordPostings = 0;
  size_t nofEntityPostings = 0;
  size_t entityNotFoundErrorMsgCount = 0;
  size_t nofLiterals = 0;

  for (auto line : wordsInTextRecords(contextFile, addWordsFromLiterals)) {
    if (line.contextId_ != currentContext) {
      ++nofContexts;
      addContextToVector(writer, currentContext, wordsInContext,
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
  addContextToVector(writer, currentContext, wordsInContext, entitiesInContext);
  textMeta_.setNofTextRecords(nofContexts);
  textMeta_.setNofWordPostings(nofWordPostings);
  textMeta_.setNofEntityPostings(nofEntityPostings);
  nofNonLiteralsInTextIndex_ = nofContexts - nofLiterals;
  configurationJson_["num-non-literals-text-index"] =
      nofNonLiteralsInTextIndex_;
  writeConfiguration();

  writer.finish();
  LOG(TRACE) << "END IndexImpl::passContextFileIntoVector" << std::endl;
}

// _____________________________________________________________________________
void IndexImpl::addContextToVector(
    IndexImpl::TextVec::bufwriter_type& writer, TextRecordIndex context,
    const ad_utility::HashMap<WordIndex, Score>& words,
    const ad_utility::HashMap<Id, Score>& entities) {
  // Determine blocks for each word and each entity.
  // Add the posting to each block.
  ad_utility::HashSet<TextBlockIndex> touchedBlocks;
  for (auto it = words.begin(); it != words.end(); ++it) {
    TextBlockIndex blockId = getWordBlockId(it->first);
    touchedBlocks.insert(blockId);
    writer << std::make_tuple(blockId, context, it->first, it->second, false);
  }

  // All entities have to be written in the entity list part for each block.
  // Ensure that they are added only once for each block.
  // For example, there could be both words computer and computing
  // in the same context. Still, co-occurring entities would only have to be
  // written to a comp* block once.
  for (TextBlockIndex blockId : touchedBlocks) {
    for (auto it = entities.begin(); it != entities.end(); ++it) {
      AD_CONTRACT_CHECK(it->first.getDatatype() == Datatype::VocabIndex);
      writer << std::make_tuple(
          blockId, context, it->first.getVocabIndex().get(), it->second, true);
    }
  }
}

// _____________________________________________________________________________
void IndexImpl::createTextIndex(const string& filename,
                                const IndexImpl::TextVec& vec) {
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
  for (TextVec::bufreader_type reader(vec); !reader.empty(); ++reader) {
    if (std::get<0>(*reader) != currentBlockIndex) {
      AD_CONTRACT_CHECK(!classicPostings.empty());

      ContextListMetaData classic = textIndexReadWrite::writePostings(
          out, classicPostings, true, currenttOffset_);
      ContextListMetaData entity = textIndexReadWrite::writePostings(
          out, entityPostings, false, currenttOffset_);
      textMeta_.addBlock(TextBlockMetaData(
          currentMinWordIndex, currentMaxWordIndex, classic, entity));
      classicPostings.clear();
      entityPostings.clear();
      currentBlockIndex = std::get<0>(*reader);
      currentMinWordIndex = std::get<2>(*reader);
      currentMaxWordIndex = std::get<2>(*reader);
    }
    if (!std::get<4>(*reader)) {
      classicPostings.emplace_back(std::get<1>(*reader), std::get<2>(*reader),
                                   std::get<3>(*reader));
      if (std::get<2>(*reader) < currentMinWordIndex) {
        currentMinWordIndex = std::get<2>(*reader);
      }
      if (std::get<2>(*reader) > currentMaxWordIndex) {
        currentMaxWordIndex = std::get<2>(*reader);
      }

    } else {
      entityPostings.emplace_back(std::get<1>(*reader), std::get<2>(*reader),
                                  std::get<3>(*reader));
    }
  }
  // Write the last block
  AD_CONTRACT_CHECK(!classicPostings.empty());
  ContextListMetaData classic = textIndexReadWrite::writePostings(
      out, classicPostings, true, currenttOffset_);
  ContextListMetaData entity = textIndexReadWrite::writePostings(
      out, entityPostings, false, currenttOffset_);
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

/// yields  aaaa, aaab, ..., zzzz
static cppcoro::generator<std::string> fourLetterPrefixes() {
  static_assert(
      MIN_WORD_PREFIX_SIZE == 4,
      "If you need this to be changed, please contact the developers");
  auto chars = []() -> cppcoro::generator<char> {
    for (char c = 'a'; c <= 'z'; ++c) {
      co_yield c;
    }
  };

  for (char a : chars()) {
    for (char b : chars()) {
      for (char c : chars()) {
        for (char d : chars()) {
          std::string s{a, b, c, d};
          co_yield s;
        }
      }
    }
  }
}

/// Check if the `fourLetterPrefixes` are sorted wrt to the `comparator`
static bool areFourLetterPrefixesSorted(auto comparator) {
  std::string first;
  for (auto second : fourLetterPrefixes()) {
    if (!comparator(first, second)) {
      return false;
    }
    first = std::move(second);
  }
  return true;
}

template <typename I, typename BlockBoundaryAction>
void IndexImpl::calculateBlockBoundariesImpl(
    I&& index, const BlockBoundaryAction& blockBoundaryAction) {
  LOG(TRACE) << "BEGIN IndexImpl::calculateBlockBoundaries" << std::endl;
  // Go through the vocabulary
  // Start a new block whenever a word is
  // 1) The last word in the corpus
  // 2) shorter than the minimum prefix length
  // 3) The next word is shorter than the minimum prefix length
  // 4) word.substring(0, MIN_PREFIX_LENGTH) is different from the next.
  // Note that the evaluation of 4) is difficult to perform in a meaningful way
  // for all corner cases of Unicode. E.g. vivae and vivæ  compare equal on the
  // PRIMARY level which is relevant, but have a different length (5 vs 4).
  // We currently use several workarounds to get as close as possible to the
  // desired behavior.
  // A block boundary is always the last WordId in the block.
  // this way std::lower_bound will point to the correct bracket.

  if (!areFourLetterPrefixesSorted(index.textVocab_.getCaseComparator())) {
    LOG(ERROR) << "You have chosen a locale where the prefixes aaaa, aaab, "
                  "..., zzzz are not alphabetically ordered. This is currently "
                  "unsupported when building a text index";
    AD_FAIL();
  }

  if (index.textVocab_.size() == 0) {
    LOG(WARN) << "You are trying to call calculateBlockBoundaries on an empty "
                 "text vocabulary\n";
    return;
  }
  size_t numBlocks = 0;
  const auto& locManager = index.textVocab_.getLocaleManager();

  // iterator over aaaa, ...,  zzzz
  auto forcedBlockStarts = fourLetterPrefixes();
  auto forcedBlockStartsIt = forcedBlockStarts.begin();

  // If there is a four letter prefix aaaa, ...., zzzz in `forcedBlockStarts`
  // the `SortKey` of which is a prefix of `prefixSortKey`, then set
  // `prefixSortKey` to that `SortKey` and `prefixLength` to
  // `MIN_WORD_PREFIX_SIZE` (4). This ensures that the blocks corresponding to
  // these prefixes are never split up because of Unicode ligatures.
  auto adjustPrefixSortKey = [&](auto& prefixSortKey, auto& prefixLength) {
    while (true) {
      if (forcedBlockStartsIt == forcedBlockStarts.end()) {
        break;
      }
      auto forcedBlockStartSortKey = locManager.getSortKey(
          *forcedBlockStartsIt, LocaleManager::Level::PRIMARY);
      if (forcedBlockStartSortKey >= prefixSortKey) {
        break;
      }
      if (prefixSortKey.starts_with(forcedBlockStartSortKey)) {
        prefixSortKey = std::move(forcedBlockStartSortKey);
        prefixLength = MIN_WORD_PREFIX_SIZE;
        return;
      }
      forcedBlockStartsIt++;
    }
  };

  auto getLengthAndPrefixSortKey = [&](WordVocabIndex i) {
    auto word = index.textVocab_[i];
    auto [len, prefixSortKey] =
        locManager.getPrefixSortKey(word, MIN_WORD_PREFIX_SIZE);
    if (len > MIN_WORD_PREFIX_SIZE) {
      LOG(DEBUG) << "The prefix sort key for word \"" << word
                 << "\" and prefix length " << MIN_WORD_PREFIX_SIZE
                 << " actually refers to a prefix of size " << len << '\n';
    }
    // If we are in a block where one of the fourLetterPrefixes are contained,
    // use those as the block start.
    adjustPrefixSortKey(prefixSortKey, len);
    return std::tuple{std::move(len), std::move(prefixSortKey)};
  };
  auto [currentLen, prefixSortKey] =
      getLengthAndPrefixSortKey(WordVocabIndex::make(0));
  for (size_t i = 0; i < index.textVocab_.size() - 1; ++i) {
    // we need foo.value().get() because the vocab returns
    // a std::optional<std::reference_wrapper<string>> and the "." currently
    // doesn't implicitly convert to a true reference (unlike function calls)
    const auto& [nextLen, nextPrefixSortKey] =
        getLengthAndPrefixSortKey(WordVocabIndex::make(i + 1));

    bool tooShortButNotEqual =
        (currentLen < MIN_WORD_PREFIX_SIZE || nextLen < MIN_WORD_PREFIX_SIZE) &&
        (prefixSortKey != nextPrefixSortKey);
    // The `startsWith` also correctly handles the case where
    // `nextPrefixSortKey` is "longer" than `MIN_WORD_PREFIX_SIZE`, e.g. because
    // of unicode ligatures.
    bool samePrefix = nextPrefixSortKey.starts_with(prefixSortKey);
    if (tooShortButNotEqual || !samePrefix) {
      blockBoundaryAction(i);
      numBlocks++;
      currentLen = nextLen;
      prefixSortKey = nextPrefixSortKey;
    }
  }
  blockBoundaryAction(index.textVocab_.size() - 1);
  numBlocks++;
  LOG(DEBUG) << "Block boundaries computed: #blocks = " << numBlocks
             << ", #words = " << index.textVocab_.size() << std::endl;
}
// _____________________________________________________________________________
void IndexImpl::calculateBlockBoundaries() {
  blockBoundaries_.clear();
  auto addToBlockBoundaries = [this](size_t i) {
    blockBoundaries_.push_back(i);
  };
  return calculateBlockBoundariesImpl(*this, addToBlockBoundaries);
}

// _____________________________________________________________________________
TextBlockIndex IndexImpl::getWordBlockId(WordIndex wordIndex) const {
  return std::lower_bound(blockBoundaries_.begin(), blockBoundaries_.end(),
                          wordIndex) -
         blockBoundaries_.begin();
}

// _____________________________________________________________________________
void IndexImpl::openTextFileHandle() {
  AD_CONTRACT_CHECK(!onDiskBase_.empty());
  textIndexFile_.open(string(onDiskBase_ + ".text.index").c_str(), "r");
}

// _____________________________________________________________________________
std::string_view IndexImpl::wordIdToString(WordIndex wordIndex) const {
  return textVocab_[WordVocabIndex::make(wordIndex)];
}

// _____________________________________________________________________________
IdTable IndexImpl::readWordCl(
    const TextBlockMetaData& tbmd,
    const ad_utility::AllocatorWithLimit<Id>& allocator) const {
  IdTable idTable{3, allocator};
  idTable.resize(tbmd._cl._nofElements);
  textIndexReadWrite::readGapComprList<Id, uint64_t>(
      idTable.getColumn(0).begin(), tbmd._cl._nofElements,
      tbmd._cl._startContextlist,
      static_cast<size_t>(tbmd._cl._startWordlist - tbmd._cl._startContextlist),
      textIndexFile_, [](uint64_t id) {
        return Id::makeFromTextRecordIndex(TextRecordIndex::make(id));
      });

  textIndexReadWrite::readFreqComprList<Id, WordIndex>(
      idTable.getColumn(1).begin(), tbmd._cl._nofElements,
      tbmd._cl._startWordlist,
      static_cast<size_t>(tbmd._cl._startScorelist - tbmd._cl._startWordlist),
      textIndexFile_, [](WordIndex id) {
        return Id::makeFromWordVocabIndex(WordVocabIndex::make(id));
      });

  textIndexReadWrite::readFreqComprList<Id, Score>(
      idTable.getColumn(2).begin(), tbmd._cl._nofElements,
      tbmd._cl._startScorelist,
      static_cast<size_t>(tbmd._cl._lastByte + 1 - tbmd._cl._startScorelist),
      textIndexFile_, &Id::makeFromInt);
  return idTable;
}

// _____________________________________________________________________________
IdTable IndexImpl::readWordEntityCl(
    const TextBlockMetaData& tbmd,
    const ad_utility::AllocatorWithLimit<Id>& allocator) const {
  IdTable idTable{3, allocator};
  idTable.resize(tbmd._entityCl._nofElements);
  textIndexReadWrite::readGapComprList<Id, uint64_t>(
      idTable.getColumn(0).begin(), tbmd._entityCl._nofElements,
      tbmd._entityCl._startContextlist,
      static_cast<size_t>(tbmd._entityCl._startWordlist -
                          tbmd._entityCl._startContextlist),
      textIndexFile_, [](uint64_t id) {
        return Id::makeFromTextRecordIndex(TextRecordIndex::make(id));
      });

  textIndexReadWrite::readFreqComprList<Id, WordIndex>(
      idTable.getColumn(1).begin(), tbmd._entityCl._nofElements,
      tbmd._entityCl._startWordlist,
      static_cast<size_t>(tbmd._entityCl._startScorelist -
                          tbmd._entityCl._startWordlist),
      textIndexFile_, [](uint64_t from) {
        return Id::makeFromVocabIndex(VocabIndex::make(from));
      });

  textIndexReadWrite::readFreqComprList<Id, Score>(
      idTable.getColumn(2).begin(), tbmd._entityCl._nofElements,
      tbmd._entityCl._startScorelist,
      static_cast<size_t>(tbmd._entityCl._lastByte + 1 -
                          tbmd._entityCl._startScorelist),
      textIndexFile_, &Id::makeFromInt);
  return idTable;
}

// _____________________________________________________________________________
IdTable IndexImpl::getWordPostingsForTerm(
    const string& term,
    const ad_utility::AllocatorWithLimit<Id>& allocator) const {
  LOG(DEBUG) << "Getting word postings for term: " << term << '\n';
  IdTable idTable{allocator};
  idTable.setNumColumns(term.ends_with('*') ? 3 : 2);
  auto optionalTbmd = getTextBlockMetadataForWordOrPrefix(term);
  if (!optionalTbmd.has_value()) {
    return idTable;
  }
  const auto& tbmd = optionalTbmd.value().tbmd_;
  idTable = readWordCl(tbmd, allocator);
  if (optionalTbmd.value().hasToBeFiltered_) {
    idTable =
        FTSAlgorithms::filterByRange(optionalTbmd.value().idRange_, idTable);
  }
  LOG(DEBUG) << "Word postings for term: " << term
             << ": cids: " << idTable.getColumn(0).size() << '\n';
  return idTable;
}

// _____________________________________________________________________________
IdTable IndexImpl::getEntityMentionsForWord(
    const string& term,
    const ad_utility::AllocatorWithLimit<Id>& allocator) const {
  auto optTbmd = getTextBlockMetadataForWordOrPrefix(term);
  if (!optTbmd.has_value()) {
    return IdTable{allocator};
  }
  const auto& tbmd = optTbmd.value().tbmd_;
  return readWordEntityCl(tbmd, allocator);
}

// _____________________________________________________________________________
size_t IndexImpl::getIndexOfBestSuitedElTerm(
    const vector<string>& terms) const {
  // It is beneficial to choose a term where no filtering by regular word id
  // is needed. Then the entity lists can be read directly from disk.
  // For others it is always necessary to reach wordlist and filter them
  // if such an entity list is taken, another intersection is necessary.

  // Apart from that, entity lists are usually larger by a factor.
  // Hence it makes sense to choose the smallest.

  // Heuristic: Always prefer no-filtering terms over others, then
  // pick the one with the smallest EL block to be read.
  std::vector<std::tuple<size_t, bool, size_t>> toBeSorted;
  for (size_t i = 0; i < terms.size(); ++i) {
    auto optTbmd = getTextBlockMetadataForWordOrPrefix(terms[i]);
    if (!optTbmd.has_value()) {
      return i;
    }
    const auto& tbmd = optTbmd.value().tbmd_;
    toBeSorted.emplace_back(i, tbmd._firstWordId == tbmd._lastWordId,
                            tbmd._entityCl._nofElements);
  }
  std::sort(toBeSorted.begin(), toBeSorted.end(),
            [](const std::tuple<size_t, bool, size_t>& a,
               const std::tuple<size_t, bool, size_t>& b) {
              if (std::get<1>(a) == std::get<1>(b)) {
                return std::get<2>(a) < std::get<2>(b);
              } else {
                return std::get<1>(a);
              }
            });
  return std::get<0>(toBeSorted[0]);
}

// _____________________________________________________________________________
size_t IndexImpl::getSizeOfTextBlockForEntities(const string& word) const {
  if (word.empty()) {
    return 0;
  }
  auto optTbmd = getTextBlockMetadataForWordOrPrefix(word);
  if (!optTbmd.has_value()) {
    return 0;
  }
  return optTbmd.value().tbmd_._entityCl._nofElements;
}

// _____________________________________________________________________________
size_t IndexImpl::getSizeOfTextBlockForWord(const string& word) const {
  if (word.empty()) {
    return 0;
  }
  auto optTbmd = getTextBlockMetadataForWordOrPrefix(word);
  if (!optTbmd.has_value()) {
    return 0;
  }
  return optTbmd.value().tbmd_._cl._nofElements;
}

// _____________________________________________________________________________
size_t IndexImpl::getSizeEstimate(const string& words) const {
  // TODO vector can be of type std::string_view if called functions
  //  are updated to accept std::string_view instead of const std::string&
  std::vector<std::string> terms = absl::StrSplit(words, ' ');
  if (terms.empty()) {
    return 0;
  }
  auto termToEstimate = [&](const std::string& term) -> size_t {
    auto optTbmd = getTextBlockMetadataForWordOrPrefix(term);
    // TODO<C++23> Use `std::optional::transform`.
    if (!optTbmd.has_value()) {
      return 0;
    }
    return 1 + optTbmd.value().tbmd_._entityCl._nofElements / 100;
  };
  return ql::ranges::min(terms | ql::views::transform(termToEstimate));
}

// _____________________________________________________________________________
void IndexImpl::setTextName(const string& name) { textMeta_.setName(name); }

// _____________________________________________________________________________
auto IndexImpl::getTextBlockMetadataForWordOrPrefix(const std::string& word)
    const -> std::optional<TextBlockMetadataAndWordInfo> {
  AD_CORRECTNESS_CHECK(!word.empty());
  IdRange<WordVocabIndex> idRange;
  if (word.ends_with(PREFIX_CHAR)) {
    auto idRangeOpt = textVocab_.getIdRangeForFullTextPrefix(word);
    if (!idRangeOpt.has_value()) {
      LOG(INFO) << "Prefix: " << word << " not in vocabulary\n";
      return std::nullopt;
    }
    idRange = idRangeOpt.value();
  } else {
    WordVocabIndex idx;
    if (!textVocab_.getId(word, &idx)) {
      LOG(INFO) << "Term: " << word << " not in vocabulary\n";
      return std::nullopt;
    }
    idRange = IdRange{idx, idx};
  }
  const auto& tbmd = textMeta_.getBlockInfoByWordRange(idRange.first().get(),
                                                       idRange.last().get());
  bool hasToBeFiltered = tbmd._cl.hasMultipleWords() &&
                         !(tbmd._firstWordId == idRange.first().get() &&
                           tbmd._lastWordId == idRange.last().get());
  return TextBlockMetadataAndWordInfo{tbmd, hasToBeFiltered, idRange};
}

// _____________________________________________________________________________
void IndexImpl::setScoringMetricsUsedInSettings(
    TextScoringMetric scoringMetric) {
  configurationJson_["text-scoring-metric"] = scoringMetric;
  writeConfiguration();
}

// _____________________________________________________________________________
void IndexImpl::setBM25ParametersInSettings(float b, float k) {
  if (0 <= b && b <= 1 && 0 <= k) {
    configurationJson_["b-and-k-parameter-for-text-scoring"] =
        std::make_pair(b, k);
  } else {
    configurationJson_["b-and-k-parameter-for-text-scoring"] =
        std::make_pair(0.75, 1.75);
  }
  writeConfiguration();
}
