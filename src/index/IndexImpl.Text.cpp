// Copyright 2015 - 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Björn Buchhold <buchhold@cs.uni-freiburg.de>
//          Johannes Kalmbach <johannes.kalmbach@gmail.com>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "./IndexImpl.h"

#include <absl/strings/str_split.h>

#include <algorithm>
#include <stxxl/algorithm>
#include <tuple>
#include <utility>

#include "../engine/CallFixedSize.h"
#include "../parser/ContextFileParser.h"
#include "../util/Conversions.h"
#include "../util/Simple8bCode.h"
#include "./FTSAlgorithms.h"

namespace {

// Custom delimiter class for tokenization of literals using `absl::StrSplit`.
// The `Find` function returns the next delimiter in `text` after the given
// `pos` or an empty subtring if there is no next delimiter.
struct LiteralsTokenizationDelimiter {
  absl::string_view Find(absl::string_view text, size_t pos) {
    auto isWordChar = [](char c) -> bool { return std::isalnum(c); };
    auto found = std::find_if_not(text.begin() + pos, text.end(), isWordChar);
    if (found == text.end()) return text.substr(text.size());
    return {found, found + 1};
  }
};

}  // namespace

// _____________________________________________________________________________
cppcoro::generator<ContextFileParser::Line> IndexImpl::wordsInTextRecords(
    const std::string& contextFile, bool addWordsFromLiterals) {
  auto localeManager = textVocab_.getLocaleManager();
  // ROUND 1: If context file aka wordsfile is not empty, read words from there.
  // Remember the last context id for the (optional) second round.
  TextRecordIndex contextId = TextRecordIndex::make(0);
  if (!contextFile.empty()) {
    ContextFileParser::Line line;
    ContextFileParser p(contextFile, localeManager);
    ad_utility::HashSet<string> items;
    while (p.getLine(line)) {
      contextId = line._contextId;
      co_yield line;
    }
    if (contextId > TextRecordIndex::make(0)) {
      contextId = contextId.incremented();
    }
  }
  // ROUND 2: Optionally, consider each literal from the interal vocabulary as a
  // text record.
  if (addWordsFromLiterals) {
    for (VocabIndex index = VocabIndex::make(0); index.get() < vocab_.size();
         index = index.incremented()) {
      auto text = vocab_.at(index);
      if (!isLiteral(text)) {
        continue;
      }
      ContextFileParser::Line entityLine{text, true, contextId, 1};
      co_yield entityLine;
      std::string_view textView = text;
      textView = textView.substr(0, textView.rfind('"'));
      textView.remove_prefix(1);
      for (auto word : absl::StrSplit(textView, LiteralsTokenizationDelimiter{},
                                      absl::SkipEmpty{})) {
        auto wordNormalized = localeManager.getLowercaseUtf8(word);
        ContextFileParser::Line wordLine{wordNormalized, false, contextId, 1};
        co_yield wordLine;
      }
      contextId = contextId.incremented();
    }
  }
}

// _____________________________________________________________________________
void IndexImpl::addTextFromContextFile(const string& contextFile,
                                       bool addWordsFromLiterals) {
  LOG(INFO) << std::endl;
  LOG(INFO) << "Adding text index ..." << std::endl;
  string indexFilename = onDiskBase_ + ".text.index";
  // Either read words from given file or consider each literal as text record
  // or both (but at least one of them, otherwise this function is not called).
  if (!contextFile.empty()) {
    LOG(INFO) << "Reading words from \"" << contextFile << "\"" << std::endl;
  }
  if (addWordsFromLiterals) {
    LOG(INFO) << (contextFile.empty() ? "C" : "Additionally c")
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
  vocab_.readFromFile(onDiskBase_ + INTERNAL_VOCAB_SUFFIX,
                      onDiskBase_ + EXTERNAL_VOCAB_SUFFIX);

  // Build the text vocabulary (first scan over the text records).
  LOG(INFO) << "Building text vocabulary ..." << std::endl;
  size_t nofLines =
      processWordsForVocabulary(contextFile, addWordsFromLiterals);
  textVocab_.writeToFile(onDiskBase_ + ".text.vocabulary");

  // Build the half-inverted lists (second scan over the text records).
  LOG(INFO) << "Building the half-inverted index lists ..." << std::endl;
  calculateBlockBoundaries();
  TextVec v;
  v.reserve(nofLines);
  processWordsForInvertedLists(contextFile, addWordsFromLiterals, v);
  LOG(DEBUG) << "Sorting text index, #elements = " << v.size() << std::endl;
  stxxl::sort(begin(v), end(v), SortText(), stxxlMemoryInBytes() / 3);
  LOG(DEBUG) << "Sort done" << std::endl;
  createTextIndex(indexFilename, v);
  openTextFileHandle();
}

// _____________________________________________________________________________
void IndexImpl::buildDocsDB(const string& docsFileName) {
  LOG(INFO) << "Building DocsDB...\n";
  ad_utility::File docsFile(docsFileName.c_str(), "r");
  std::ofstream ofs(onDiskBase_ + ".text.docsDB", std::ios_base::out);
  // To avoid excessive use of RAM,
  // we write the offsets to and stxxl:vector first;
  typedef stxxl::vector<off_t> OffVec;
  OffVec offsets;
  off_t currentOffset = 0;
  uint64_t currentContextId = 0;
  char* buf = new char[BUFFER_SIZE_DOCSFILE_LINE];
  string line;
  while (docsFile.readLine(&line, buf, BUFFER_SIZE_DOCSFILE_LINE)) {
    size_t tab = line.find('\t');
    uint64_t contextId = atol(line.substr(0, tab).c_str());
    line = line.substr(tab + 1);
    ofs << line;
    while (currentContextId < contextId) {
      offsets.push_back(currentOffset);
      currentContextId++;
    }
    offsets.push_back(currentOffset);
    currentContextId++;
    currentOffset += line.size();
  }
  offsets.push_back(currentOffset);

  delete[] buf;
  ofs.close();
  // Now append the tmp file to the docsDB file.
  ad_utility::File out(string(onDiskBase_ + ".text.docsDB").c_str(), "a");
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
    //           << std::setw(50) << line._word << "   "
    //           << line._isEntity << "\t"
    //           << line._contextId.get() << "\t"
    //           << line._score << std::endl;
    if (!line._isEntity) {
      distinctWords.insert(line._word);
    }
  }
  textVocab_.createFromSet(distinctWords);
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
  size_t nofContexts = 0;
  size_t nofWordPostings = 0;
  size_t nofEntityPostings = 0;
  size_t entityNotFoundErrorMsgCount = 0;

  for (auto line : wordsInTextRecords(contextFile, addWordsFromLiterals)) {
    if (line._contextId != currentContext) {
      ++nofContexts;
      addContextToVector(writer, currentContext, wordsInContext,
                         entitiesInContext);
      currentContext = line._contextId;
      wordsInContext.clear();
      entitiesInContext.clear();
    }
    if (line._isEntity) {
      ++nofEntityPostings;
      // TODO<joka921> Currently only IRIs and strings from the vocabulary can
      // be tagged entities in the text index (no doubles, ints, etc).
      VocabIndex eid;
      if (getVocab().getId(line._word, &eid)) {
        // Note that `entitiesInContext` is a HashMap, so the `Id`s don't have
        // to be contiguous.
        entitiesInContext[Id::makeFromVocabIndex(eid)] += line._score;
      } else {
        if (entityNotFoundErrorMsgCount < 20) {
          LOG(WARN) << "Entity from text not in KB: " << line._word << '\n';
          if (++entityNotFoundErrorMsgCount == 20) {
            LOG(WARN) << "There are more entities not in the KB..."
                      << " suppressing further warnings...\n";
          }
        } else {
          entityNotFoundErrorMsgCount++;
        }
      }
    } else {
      ++nofWordPostings;
      // TODO<joka921> Let the `textVocab_` return a `WordIndex` directly.
      VocabIndex vid;
      bool ret = textVocab_.getId(line._word, &vid);
      WordIndex wid = vid.get();
      if (!ret) {
        LOG(ERROR) << "ERROR: word \"" << line._word << "\" "
                   << "not found in textVocab. Terminating\n";
        AD_FAIL();
      }
      wordsInContext[wid] += line._score;
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

  for (auto it = entities.begin(); it != entities.end(); ++it) {
    TextBlockIndex blockId = getEntityBlockId(it->first);
    touchedBlocks.insert(blockId);
    AD_CONTRACT_CHECK(it->first.getDatatype() == Datatype::VocabIndex);
    writer << std::make_tuple(blockId, context, it->first.getVocabIndex().get(),
                              it->second, false);
  }

  // All entities have to be written in the entity list part for each block.
  // Ensure that they are added only once for each block.
  // For example, there could be both words computer and computing
  // in the same context. Still, co-occurring entities would only have to be
  // written to a comp* block once.
  for (TextBlockIndex blockId : touchedBlocks) {
    for (auto it = entities.begin(); it != entities.end(); ++it) {
      // Don't add an entity to its own block..
      // FIX JUN 07 2017: DO add it. It's needed so that it is returned
      // as a result itself.
      // if (blockId == getEntityBlockId(it->first)) { continue; }
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
  size_t nofEntities = 0;
  size_t nofEntityContexts = 0;
  for (TextVec::bufreader_type reader(vec); !reader.empty(); ++reader) {
    if (std::get<0>(*reader) != currentBlockIndex) {
      AD_CONTRACT_CHECK(!classicPostings.empty());

      bool isEntityBlock = isEntityBlockId(currentBlockIndex);
      if (isEntityBlock) {
        ++nofEntities;
        nofEntityContexts += classicPostings.size();
      }
      ContextListMetaData classic = writePostings(out, classicPostings, true);
      ContextListMetaData entity = writePostings(out, entityPostings, false);
      textMeta_.addBlock(
          TextBlockMetaData(currentMinWordIndex, currentMaxWordIndex, classic,
                            entity),
          isEntityBlock);
      classicPostings.clear();
      entityPostings.clear();
      currentBlockIndex = std::get<0>(*reader);
      currentMinWordIndex = std::get<2>(*reader);
      currentMaxWordIndex = std::get<2>(*reader);
    }
    if (!std::get<4>(*reader)) {
      classicPostings.emplace_back(std::make_tuple(
          std::get<1>(*reader), std::get<2>(*reader), std::get<3>(*reader)));
      if (std::get<2>(*reader) < currentMinWordIndex) {
        currentMinWordIndex = std::get<2>(*reader);
      }
      if (std::get<2>(*reader) > currentMaxWordIndex) {
        currentMaxWordIndex = std::get<2>(*reader);
      }

    } else {
      entityPostings.emplace_back(std::make_tuple(
          std::get<1>(*reader), std::get<2>(*reader), std::get<3>(*reader)));
    }
  }
  // Write the last block
  AD_CONTRACT_CHECK(!classicPostings.empty());
  if (isEntityBlockId(currentBlockIndex)) {
    ++nofEntities;
    nofEntityContexts += classicPostings.size();
  }
  ContextListMetaData classic = writePostings(out, classicPostings, true);
  ContextListMetaData entity = writePostings(out, entityPostings, false);
  textMeta_.addBlock(TextBlockMetaData(currentMinWordIndex, currentMaxWordIndex,
                                       classic, entity),
                     isEntityBlockId(currentMaxWordIndex));
  textMeta_.setNofEntities(nofEntities);
  textMeta_.setNofEntityContexts(nofEntityContexts);
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

// _____________________________________________________________________________
ContextListMetaData IndexImpl::writePostings(ad_utility::File& out,
                                             const vector<Posting>& postings,
                                             bool skipWordlistIfAllTheSame) {
  ContextListMetaData meta;
  meta._nofElements = postings.size();
  if (meta._nofElements == 0) {
    meta._startContextlist = currenttOffset_;
    meta._startWordlist = currenttOffset_;
    meta._startScorelist = currenttOffset_;
    meta._lastByte = currenttOffset_ - 1;
    return meta;
  }

  // Collect the individual lists
  // Context lists are gap encoded, word and score lists frequency encoded.
  // TODO<joka921> these are gap encoded contextIds, maybe also create a type
  // for this.
  auto contextList = new uint64_t[meta._nofElements];
  WordIndex* wordList = new WordIndex[meta._nofElements];
  Score* scoreList = new Score[meta._nofElements];

  size_t n = 0;

  WordToCodeMap wordCodeMap;
  WordCodebook wordCodebook;
  ScoreCodeMap scoreCodeMap;
  ScoreCodebook scoreCodebook;

  createCodebooks(postings, wordCodeMap, wordCodebook, scoreCodeMap,
                  scoreCodebook);

  TextRecordIndex lastContext = std::get<0>(postings[0]);
  contextList[n] = lastContext.get();
  wordList[n] = wordCodeMap[std::get<1>(postings[0])];
  scoreList[n] = scoreCodeMap[std::get<2>(postings[0])];
  ++n;

  for (auto it = postings.begin() + 1; it < postings.end(); ++it) {
    uint64_t gap = std::get<0>(*it).get() - lastContext.get();
    contextList[n] = gap;
    lastContext = std::get<0>(*it);
    wordList[n] = wordCodeMap[std::get<1>(*it)];
    scoreList[n] = scoreCodeMap[std::get<2>(*it)];
    ++n;
  }

  AD_CONTRACT_CHECK(meta._nofElements == n);

  // Do the actual writing:
  size_t bytes = 0;

  // Write context list:
  meta._startContextlist = currenttOffset_;
  bytes = writeList(contextList, meta._nofElements, out);
  currenttOffset_ += bytes;

  // Write word list:
  // This can be skipped if we're writing classic lists and there
  // is only one distinct wordId in the block, since this Id is already
  // stored in the meta data.
  meta._startWordlist = currenttOffset_;
  if (!skipWordlistIfAllTheSame || wordCodebook.size() > 1) {
    currenttOffset_ += writeCodebook(wordCodebook, out);
    bytes = writeList(wordList, meta._nofElements, out);
    currenttOffset_ += bytes;
  }

  // Write scores
  meta._startScorelist = currenttOffset_;
  currenttOffset_ += writeCodebook(scoreCodebook, out);
  bytes = writeList(scoreList, meta._nofElements, out);
  currenttOffset_ += bytes;

  meta._lastByte = currenttOffset_ - 1;

  delete[] contextList;
  delete[] wordList;
  delete[] scoreList;

  return meta;
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

  auto getLengthAndPrefixSortKey = [&](VocabIndex i) {
    auto word = index.textVocab_[i].value();
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
      getLengthAndPrefixSortKey(VocabIndex::make(0));
  for (size_t i = 0; i < index.textVocab_.size() - 1; ++i) {
    // we need foo.value().get() because the vocab returns
    // a std::optional<std::reference_wrapper<string>> and the "." currently
    // doesn't implicitly convert to a true reference (unlike function calls)
    const auto& [nextLen, nextPrefixSortKey] =
        getLengthAndPrefixSortKey(VocabIndex::make(i + 1));

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
void IndexImpl::printBlockBoundariesToFile(const string& filename) const {
  std::ofstream of{filename};
  of << "Printing block boundaries ot text vocabulary\n"
     << "Format: <Last word of Block> <First word of next Block>\n";
  auto printBlockToFile = [this, &of](size_t i) {
    of << textVocab_[VocabIndex::make(i)].value() << " ";
    if (i + 1 < textVocab_.size()) {
      of << textVocab_[VocabIndex::make(i + 1)].value() << '\n';
    }
  };
  return calculateBlockBoundariesImpl(*this, printBlockToFile);
}

// _____________________________________________________________________________
TextBlockIndex IndexImpl::getWordBlockId(WordIndex wordIndex) const {
  return std::lower_bound(blockBoundaries_.begin(), blockBoundaries_.end(),
                          wordIndex) -
         blockBoundaries_.begin();
}

// _____________________________________________________________________________
TextBlockIndex IndexImpl::getEntityBlockId(Id entityId) const {
  AD_CONTRACT_CHECK(entityId.getDatatype() == Datatype::VocabIndex);
  return entityId.getVocabIndex().get() + blockBoundaries_.size();
}

// _____________________________________________________________________________
bool IndexImpl::isEntityBlockId(TextBlockIndex blockIndex) const {
  return blockIndex >= blockBoundaries_.size();
}

// _____________________________________________________________________________
template <typename Numeric>
size_t IndexImpl::writeList(Numeric* data, size_t nofElements,
                            ad_utility::File& file) const {
  if (nofElements > 0) {
    uint64_t* encoded = new uint64_t[nofElements];
    size_t size = ad_utility::Simple8bCode::encode(data, nofElements, encoded);
    size_t ret = file.write(encoded, size);
    AD_CONTRACT_CHECK(size == ret);
    delete[] encoded;
    return size;
  } else {
    return 0;
  }
}

// _____________________________________________________________________________
void IndexImpl::createCodebooks(const vector<IndexImpl::Posting>& postings,
                                IndexImpl::WordToCodeMap& wordCodemap,
                                IndexImpl::WordCodebook& wordCodebook,
                                IndexImpl::ScoreCodeMap& scoreCodemap,
                                IndexImpl::ScoreCodebook& scoreCodebook) const {
  ad_utility::HashMap<WordIndex, size_t> wfMap;
  ad_utility::HashMap<Score, size_t> sfMap;
  for (const auto& p : postings) {
    wfMap[std::get<1>(p)] = 0;
    sfMap[std::get<2>(p)] = 0;
  }
  for (const auto& p : postings) {
    ++wfMap[std::get<1>(p)];
    ++sfMap[std::get<2>(p)];
  }
  vector<std::pair<WordIndex, size_t>> wfVec;
  wfVec.resize(wfMap.size());
  size_t i = 0;
  for (auto it = wfMap.begin(); it != wfMap.end(); ++it) {
    wfVec[i].first = it->first;
    wfVec[i].second = it->second;
    ++i;
  }
  vector<std::pair<Score, size_t>> sfVec;
  sfVec.resize(sfMap.size());
  i = 0;
  for (auto it = sfMap.begin(); it != sfMap.end(); ++it) {
    sfVec[i].first = it->first;
    sfVec[i].second = it->second;
    ++i;
  }
  std::sort(wfVec.begin(), wfVec.end(),
            [](const auto& a, const auto& b) { return a.second > b.second; });
  std::sort(
      sfVec.begin(), sfVec.end(),
      [](const std::pair<Score, size_t>& a, const std::pair<Score, size_t>& b) {
        return a.second > b.second;
      });
  for (size_t j = 0; j < wfVec.size(); ++j) {
    wordCodebook.push_back(wfVec[j].first);
    wordCodemap[wfVec[j].first] = j;
  }
  for (size_t j = 0; j < sfVec.size(); ++j) {
    scoreCodebook.push_back(sfVec[j].first);
    scoreCodemap[sfVec[j].first] = static_cast<Score>(j);
  }
}

// _____________________________________________________________________________
template <class T>
size_t IndexImpl::writeCodebook(const vector<T>& codebook,
                                ad_utility::File& file) const {
  size_t byteSizeOfCodebook = sizeof(T) * codebook.size();
  file.write(&byteSizeOfCodebook, sizeof(byteSizeOfCodebook));
  file.write(codebook.data(), byteSizeOfCodebook);
  return byteSizeOfCodebook + sizeof(byteSizeOfCodebook);
}

// _____________________________________________________________________________
void IndexImpl::openTextFileHandle() {
  AD_CONTRACT_CHECK(!onDiskBase_.empty());
  textIndexFile_.open(string(onDiskBase_ + ".text.index").c_str(), "r");
}

// _____________________________________________________________________________
std::string_view IndexImpl::wordIdToString(WordIndex wordIndex) const {
  return textVocab_[VocabIndex::make(wordIndex)].value();
}

// _____________________________________________________________________________
void IndexImpl::getContextListForWords(const string& words,
                                       IdTable* dynResult) const {
  LOG(DEBUG) << "In getContextListForWords...\n";
  // TODO vector can be of type std::string_view if called functions
  //  are updated to accept std::string_view instead of const std::string&
  std::vector<std::string> terms = absl::StrSplit(words, ' ');
  AD_CONTRACT_CHECK(!terms.empty());

  Index::WordEntityPostings wep;
  if (terms.size() > 1) {
    vector<Index::WordEntityPostings> wepVecs;
    for (auto& term : terms) {
      wepVecs.push_back(getWordPostingsForTerm(term));
    }
    if (wepVecs.size() == 2) {
      FTSAlgorithms::intersectTwoPostingLists(
          wepVecs[0].cids_, wepVecs[1].scores_, wepVecs[1].cids_,
          wepVecs[1].scores_, wep.cids_, wep.scores_);
    } else {
      wep = FTSAlgorithms::intersectKWay(wepVecs, nullptr);
    }
  } else {
    wep = getWordPostingsForTerm(terms[0]);
  }

  LOG(DEBUG) << "Packing lists into a ResultTable\n...";
  IdTableStatic<2> result = std::move(*dynResult).toStatic<2>();
  result.resize(wep.cids_.size());
  for (size_t i = 0; i < wep.cids_.size(); ++i) {
    result(i, 0) = Id::makeFromTextRecordIndex(wep.cids_[i]);
    result(i, 1) = Id::makeFromInt(wep.scores_[i]);
  }
  *dynResult = std::move(result).toDynamic();
  LOG(DEBUG) << "Done with getContextListForWords.\n";
}

// _____________________________________________________________________________
Index::WordEntityPostings IndexImpl::readWordCl(
    const TextBlockMetaData& tbmd) const {
  Index::WordEntityPostings wep;
  wep.cids_ = readGapComprList<TextRecordIndex>(
      tbmd._cl._nofElements, tbmd._cl._startContextlist,
      static_cast<size_t>(tbmd._cl._startWordlist - tbmd._cl._startContextlist),
      &TextRecordIndex::make);
  wep.wids_ = readFreqComprList<WordIndex>(
      tbmd._cl._nofElements, tbmd._cl._startWordlist,
      static_cast<size_t>(tbmd._cl._startScorelist - tbmd._cl._startWordlist));
  wep.scores_ = readFreqComprList<Score>(
      tbmd._cl._nofElements, tbmd._cl._startScorelist,
      static_cast<size_t>(tbmd._cl._lastByte + 1 - tbmd._cl._startScorelist));
  return wep;
}

// _____________________________________________________________________________
Index::WordEntityPostings IndexImpl::readWordEntityCl(
    const TextBlockMetaData& tbmd) const {
  Index::WordEntityPostings wep;
  wep.cids_ = readGapComprList<TextRecordIndex>(
      tbmd._entityCl._nofElements, tbmd._entityCl._startContextlist,
      static_cast<size_t>(tbmd._entityCl._startWordlist -
                          tbmd._entityCl._startContextlist),
      &TextRecordIndex::make);
  wep.eids_ = readFreqComprList<Id>(
      tbmd._entityCl._nofElements, tbmd._entityCl._startWordlist,
      static_cast<size_t>(tbmd._entityCl._startScorelist -
                          tbmd._entityCl._startWordlist),
      &Id::fromBits);
  wep.scores_ = readFreqComprList<Score>(
      tbmd._entityCl._nofElements, tbmd._entityCl._startScorelist,
      static_cast<size_t>(tbmd._entityCl._lastByte + 1 -
                          tbmd._entityCl._startScorelist));
  return wep;
}

// _____________________________________________________________________________
Index::WordEntityPostings IndexImpl::getWordPostingsForTerm(
    const string& term) const {
  assert(!term.empty());
  LOG(DEBUG) << "Getting word postings for term: " << term << '\n';
  IdRange idRange;
  Index::WordEntityPostings wep;
  bool entityTerm = (term[0] == '<' && term.back() == '>');
  if (term[term.size() - 1] == PREFIX_CHAR) {
    if (!textVocab_.getIdRangeForFullTextPrefix(term, &idRange)) {
      LOG(INFO) << "Prefix: " << term << " not in vocabulary\n";
      return wep;
    }
  } else {
    if (entityTerm) {
      if (!vocab_.getId(term, &idRange._first)) {
        LOG(INFO) << "Term: " << term << " not in entity vocabulary\n";
        return wep;
      }
    } else if (!textVocab_.getId(term, &idRange._first)) {
      LOG(INFO) << "Term: " << term << " not in vocabulary\n";
      return wep;
    }
    idRange._last = idRange._first;
  }
  if (entityTerm &&
      !textMeta_.existsTextBlockForEntityId(idRange._first.get())) {
    LOG(INFO) << "Entity " << term << " not contained in the text.\n";
    return wep;
  }
  const auto& tbmd =
      entityTerm ? textMeta_.getBlockInfoByEntityId(idRange._first.get())
                 : textMeta_.getBlockInfoByWordRange(idRange._first.get(),
                                                     idRange._last.get());
  wep = readWordCl(tbmd);
  if (tbmd._cl.hasMultipleWords() &&
      !(tbmd._firstWordId == idRange._first.get() &&
        tbmd._lastWordId == idRange._last.get())) {
    wep = FTSAlgorithms::filterByRange(idRange, wep);
  }
  LOG(DEBUG) << "Word postings for term: " << term
             << ": cids: " << wep.cids_.size() << " scores "
             << wep.scores_.size() << '\n';
  return wep;
}

// _____________________________________________________________________________
Index::WordEntityPostings IndexImpl::getContextEntityScoreListsForWords(
    const string& words) const {
  LOG(DEBUG) << "In getEntityContextScoreListsForWords...\n";
  // TODO vector can be of type std::string_view if called functions
  //  are updated to accept std::string_view instead of const std::string&
  std::vector<std::string> terms = absl::StrSplit(words, ' ');
  AD_CONTRACT_CHECK(!terms.empty());
  Index::WordEntityPostings resultWep;
  if (terms.size() > 1) {
    // Find the term with the smallest block and/or one where no filtering
    // via wordlists is necessary. Only take entity postings form this one.
    // This is valid because the set of co-occuring entities depends on
    // the context and not on the word/block used as entry point.
    // Take all other words and get word posting lists for them.
    // Intersect all and keep the entity word ids.
    size_t useElFromTerm = getIndexOfBestSuitedElTerm(
        terms);  // QUESTION: funktion umändern oder raus, jetzt wo wordid auch
                 // wichtig ist
    LOG(TRACE) << "Best term to take entity list from: " << terms[useElFromTerm]
               << std::endl;

    if (terms.size() == 2) {
      // Special case of two terms: no k-way intersect needed.
      size_t onlyWordsFrom = 1 - useElFromTerm;
      Index::WordEntityPostings wWep =
          getWordPostingsForTerm(terms[onlyWordsFrom]);
      Index::WordEntityPostings eWep =
          getEntityPostingsForTerm(terms[useElFromTerm]);
      resultWep = FTSAlgorithms::intersect(wWep, eWep);
    } else {
      // Generic case: Use a k-way intersect whereas the entity postings
      // play a special role.
      vector<Index::WordEntityPostings> wepVecs;
      for (size_t i = 0; i < terms.size(); ++i) {
        if (i != useElFromTerm) {
          wepVecs.push_back(getWordPostingsForTerm(terms[i]));
        }
      }
      wepVecs.push_back(getEntityPostingsForTerm(terms[useElFromTerm]));
      resultWep = FTSAlgorithms::intersectKWay(
          wepVecs,
          &wepVecs.back().eids_);  // TODO: rewrite into crossIntersectKWay so
                                   // that word id  is also considered
    }
  } else {
    // Special case: Just one word to deal with.
    resultWep = getEntityPostingsForTerm(terms[0]);
  }
  LOG(DEBUG) << "Done with getEntityContextScoreListsForWords. "
             << "Got " << resultWep.cids_.size() << " elements. \n";
  return resultWep;
}

// _____________________________________________________________________________
void IndexImpl::getECListForWordsOneVar(const string& words, size_t limit,
                                        IdTable* result) const {
  LOG(DEBUG) << "In getECListForWords...\n";
  Index::WordEntityPostings wep = getContextEntityScoreListsForWords(words);
  FTSAlgorithms::aggScoresAndTakeTopKContexts(wep, limit, result);
  LOG(DEBUG) << "Done with getECListForWords. Result size: " << result->size()
             << "\n";
}

// _____________________________________________________________________________
void IndexImpl::getECListForWords(const string& words, size_t nofVars,
                                  size_t limit, IdTable* result) const {
  LOG(DEBUG) << "In getECListForWords...\n";
  Index::WordEntityPostings wep;
  vector<TextRecordIndex> cids;
  vector<Id> eids;
  vector<Score> scores;
  wep = getContextEntityScoreListsForWords(words);
  int width = result->numColumns();
  CALL_FIXED_SIZE(width, FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts,
                  cids, eids, scores, nofVars, limit, result);
  LOG(DEBUG) << "Done with getECListForWords. Result size: " << result->size()
             << "\n";
}

// _____________________________________________________________________________
void IndexImpl::getFilteredECListForWords(const string& words,
                                          const IdTable& filter,
                                          size_t filterColumn, size_t nofVars,
                                          size_t limit, IdTable* result) const {
  LOG(DEBUG) << "In getFilteredECListForWords...\n";
  if (!filter.empty()) {
    // Build a map filterEid->set<Rows>
    using FilterMap = ad_utility::HashMap<Id, IdTable>;
    LOG(DEBUG) << "Constructing map...\n";
    FilterMap fMap;
    for (size_t i = 0; i < filter.size(); ++i) {
      Id eid = filter(i, filterColumn);
      auto it = fMap.find(eid);
      if (it == fMap.end()) {
        it = fMap.insert(std::make_pair(eid, IdTable(filter.numColumns(),
                                                     filter.getAllocator())))
                 .first;
      }
      it->second.push_back(filter[i]);
    }
    Index::WordEntityPostings wep = getContextEntityScoreListsForWords(words);
    int width = result->numColumns();
    if (nofVars == 1) {
      CALL_FIXED_SIZE(width,
                      FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts,
                      wep, fMap, limit, result);
    } else {
      ad_utility::callFixedSize(
          width,
          []<int I>(auto&&... args) {
            FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<I>(
                AD_FWD(args)...);
          },
          wep, fMap, nofVars, limit, result);
    }
  }
  LOG(DEBUG) << "Done with getFilteredECListForWords. Result size: "
             << result->size() << "\n";
}

// _____________________________________________________________________________
void IndexImpl::getFilteredECListForWordsWidthOne(const string& words,
                                                  const IdTable& filter,
                                                  size_t nofVars, size_t limit,
                                                  IdTable* result) const {
  LOG(DEBUG) << "In getFilteredECListForWords...\n";
  // Build a map filterEid->set<Rows>
  using FilterSet = ad_utility::HashSet<Id>;
  LOG(DEBUG) << "Constructing filter set...\n";
  FilterSet fSet;
  for (size_t i = 0; i < filter.size(); ++i) {
    fSet.insert(filter(i, 0));
  }
  Index::WordEntityPostings wep = getContextEntityScoreListsForWords(words);
  int width = result->numColumns();
  if (nofVars == 1) {
    FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts(wep, fSet, limit,
                                                            result);
  } else {
    ad_utility::callFixedSize(
        width,
        []<int I>(auto&&... args) {
          FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<I>(
              AD_FWD(args)...);
        },
        wep, fSet, nofVars, limit, result);
  }
  LOG(DEBUG) << "Done with getFilteredECListForWords. Result size: "
             << result->size() << "\n";
}

// _____________________________________________________________________________
Index::WordEntityPostings IndexImpl::getEntityPostingsForTerm(
    const string& term) const {
  LOG(DEBUG) << "Getting entity postings for term: " << term << '\n';
  IdRange idRange;
  Index::WordEntityPostings resultWep;
  bool entityTerm = (term[0] == '<' && term.back() == '>');
  if (term.back() == PREFIX_CHAR) {
    if (!textVocab_.getIdRangeForFullTextPrefix(term, &idRange)) {
      LOG(INFO) << "Prefix: " << term << " not in vocabulary\n";
      return resultWep;
    }
  } else {
    if (entityTerm) {
      if (!vocab_.getId(term, &idRange._first)) {
        LOG(DEBUG) << "Term: " << term << " not in entity vocabulary\n";
        return resultWep;
      }
    } else if (!textVocab_.getId(term, &idRange._first)) {
      LOG(DEBUG) << "Term: " << term << " not in vocabulary\n";
      return resultWep;
    }
    idRange._last = idRange._first;
  }

  // TODO<joka921> Find out which ID types the `getBlockInfo...` functions
  // should take.
  const auto& tbmd =
      entityTerm ? textMeta_.getBlockInfoByEntityId(idRange._first.get())
                 : textMeta_.getBlockInfoByWordRange(idRange._first.get(),
                                                     idRange._last.get());

  if (!tbmd._cl.hasMultipleWords() ||
      (tbmd._firstWordId == idRange._first.get() &&
       tbmd._lastWordId == idRange._last.get())) {
    // CASE: Only one word in the block or full block should be matched.
    // Hence we can just read the entity CL lists for co-occurring
    // entity postings.
    resultWep = readWordEntityCl(tbmd);
  } else {
    // CASE: more than one word in the block.
    // Need to obtain matching postings for regular words and intersect for
    // a list of matching contexts.
    Index::WordEntityPostings matchingContextsWep =
        getWordPostingsForTerm(term);

    // Read the full lists
    Index::WordEntityPostings eBlockWep = readWordEntityCl(tbmd);
    resultWep = FTSAlgorithms::intersect(matchingContextsWep, eBlockWep);
  }
  return resultWep;
}

// _____________________________________________________________________________
template <typename T, typename MakeFromUint64t>
vector<T> IndexImpl::readGapComprList(size_t nofElements, off_t from,
                                      size_t nofBytes,
                                      MakeFromUint64t makeFromUint64t) const {
  LOG(DEBUG) << "Reading gap-encoded list from disk...\n";
  LOG(TRACE) << "NofElements: " << nofElements << ", from: " << from
             << ", nofBytes: " << nofBytes << '\n';
  vector<T> result;
  result.resize(nofElements + 250);
  uint64_t* encoded = new uint64_t[nofBytes / 8];
  textIndexFile_.read(encoded, nofBytes, from);
  LOG(DEBUG) << "Decoding Simple8b code...\n";
  ad_utility::Simple8bCode::decode(encoded, nofElements, result.data(),
                                   makeFromUint64t);
  LOG(DEBUG) << "Reverting gaps to actual IDs...\n";

  // TODO<joka921> make this hack unnecessary, probably by a proper output
  // iterator.
  if constexpr (requires { T::make(0); }) {
    uint64_t id = 0;
    for (size_t i = 0; i < result.size(); ++i) {
      id += result[i].get();
      result[i] = T::make(id);
    }
  } else {
    T id = 0;
    for (size_t i = 0; i < result.size(); ++i) {
      id += result[i];
      result[i] = id;
    }
  }
  result.resize(nofElements);
  delete[] encoded;
  LOG(DEBUG) << "Done reading gap-encoded list. Size: " << result.size()
             << "\n";
  return result;
}

// _____________________________________________________________________________
template <typename T, typename MakeFromUint64t>
vector<T> IndexImpl::readFreqComprList(size_t nofElements, off_t from,
                                       size_t nofBytes,
                                       MakeFromUint64t makeFromUint) const {
  AD_CONTRACT_CHECK(nofBytes > 0);
  LOG(DEBUG) << "Reading frequency-encoded list from disk...\n";
  LOG(TRACE) << "NofElements: " << nofElements << ", from: " << from
             << ", nofBytes: " << nofBytes << '\n';
  size_t nofCodebookBytes;
  vector<T> result;
  uint64_t* encoded = new uint64_t[nofElements];
  result.resize(nofElements + 250);
  off_t current = from;
  size_t ret = textIndexFile_.read(&nofCodebookBytes, sizeof(off_t), current);
  LOG(TRACE) << "Nof Codebook Bytes: " << nofCodebookBytes << '\n';
  AD_CONTRACT_CHECK(sizeof(off_t) == ret);
  current += ret;
  T* codebook = new T[nofCodebookBytes / sizeof(T)];
  ret = textIndexFile_.read(codebook, nofCodebookBytes, current);
  current += ret;
  AD_CONTRACT_CHECK(ret == size_t(nofCodebookBytes));
  ret = textIndexFile_.read(
      encoded, static_cast<size_t>(nofBytes - (current - from)), current);
  current += ret;
  AD_CONTRACT_CHECK(size_t(current - from) == nofBytes);
  LOG(DEBUG) << "Decoding Simple8b code...\n";
  ad_utility::Simple8bCode::decode(encoded, nofElements, result.data(),
                                   makeFromUint);
  LOG(DEBUG) << "Reverting frequency encoded items to actual IDs...\n";
  result.resize(nofElements);
  for (size_t i = 0; i < result.size(); ++i) {
    // TODO<joka921> handle the strong ID types properly.
    if constexpr (requires(T t) { t.getBits(); }) {
      result[i] = Id::makeFromVocabIndex(
          VocabIndex::make(codebook[result[i].getBits()].getBits()));
    } else {
      result[i] = codebook[result[i]];
    }
  }
  delete[] encoded;
  delete[] codebook;
  LOG(DEBUG) << "Done reading frequency-encoded list. Size: " << result.size()
             << "\n";
  return result;
}

#if 0
// _____________________________________________________________________________
void IndexImpl::dumpAsciiLists(const vector<string>& lists,
                           bool decGapsFreq) const {
  if (lists.size() == 0) {
    size_t nofBlocks = textMeta_.getBlockCount();
    for (size_t i = 0; i < nofBlocks; ++i) {
      TextBlockMetaData tbmd = textMeta_.getBlockById(i);
      LOG(INFO) << "At block: " << i << std::endl;
      auto nofWordElems = tbmd._cl._nofElements;
      if (nofWordElems < 1000000) continue;
      if (tbmd._firstWordId > textVocab_.size()) return;
      if (decGapsFreq) {
        AD_THROW(ad_utility::Exception::NOT_YET_IMPLEMENTED, "not yet impl.");
      } else {
        dumpAsciiLists(tbmd);
      }
    }
  } else {
    for (size_t i = 0; i < lists.size(); ++i) {
      IdRange idRange;
      textVocab_.getIdRangeForFullTextPrefix(lists[i], &idRange);
      TextBlockMetaData tbmd =
          textMeta_.getBlockInfoByWordRange(idRange._first, idRange._last);
      if (decGapsFreq) {
        vector<Id> eids;
        vector<Id> cids;
        vector<Score> scores;
        getEntityPostingsForTerm(lists[i], cids, eids, scores);
        auto firstWord = wordIdToString(tbmd._firstWordId);
        auto lastWord = wordIdToString(tbmd._lastWordId);
        string basename = onDiskBase_ + ".list." + firstWord + "-" + lastWord;
        string docIdsFn = basename + ".recIds.ent.ascii";
        string wordIdsFn = basename + ".wordIds.ent.ascii";
        string scoresFn = basename + ".scores.ent.ascii";
        writeAsciiListFile(docIdsFn, cids);
        writeAsciiListFile(wordIdsFn, eids);
        writeAsciiListFile(scoresFn, scores);
      } else {
        dumpAsciiLists(tbmd);
      }
    }
  };
}
#endif

#if 0
//_ ____________________________________________________________________________
void IndexImpl::dumpAsciiLists(const TextBlockMetaData& tbmd) const {
  auto firstWord = wordIdToString(tbmd._firstWordId);
  auto lastWord = wordIdToString(tbmd._lastWordId);
  LOG(INFO) << "This block is from " << firstWord << " to " << lastWord
            << std::endl;
  string basename = onDiskBase_ + ".list." + firstWord + "-" + lastWord;
  size_t nofCodebookBytes;
  {
    string docIdsFn = basename + ".docids.noent.ascii";
    string wordIdsFn = basename + ".wordids.noent.ascii";
    string scoresFn = basename + ".scores.noent.ascii";
    vector<Id> ids;

    LOG(DEBUG) << "Reading non-entity docId list..." << std::endl;
    auto nofElements = tbmd._cl._nofElements;
    if (nofElements == 0) return;

    auto from = tbmd._cl._startContextlist;
    auto nofBytes = static_cast<size_t>(tbmd._cl._startWordlist -
                                        tbmd._cl._startContextlist);

    ids.resize(nofElements + 250);
    uint64_t* encodedD = new uint64_t[nofBytes / 8];
    textIndexFile_.read(encodedD, nofBytes, from);
    LOG(DEBUG) << "Decoding Simple8b code...\n";
    ad_utility::Simple8bCode::decode(encodedD, nofElements, ids.data());
    ids.resize(nofElements);
    delete[] encodedD;
    writeAsciiListFile(docIdsFn, ids);

    if (tbmd._cl.hasMultipleWords()) {
      LOG(DEBUG) << "Reading non-entity wordId list..." << std::endl;
      from = tbmd._cl._startWordlist;
      nofBytes = static_cast<size_t>(tbmd._cl._startScorelist -
                                     tbmd._cl._startWordlist);

      ids.clear();
      ids.resize(nofElements + 250);
      uint64_t* encodedW = new uint64_t[nofBytes / 8];
      off_t current = from;
      size_t ret =
          textIndexFile_.read(&nofCodebookBytes, sizeof(off_t), current);
      LOG(DEBUG) << "Nof Codebook Bytes: " << nofCodebookBytes << '\n';
      AD_CHECK_EQ(sizeof(off_t), ret);
      current += ret;
      Id* codebookW = new Id[nofCodebookBytes / sizeof(Id)];
      ret = textIndexFile_.read(codebookW, nofCodebookBytes, current);
      current += ret;
      AD_CHECK_EQ(ret, size_t(nofCodebookBytes));
      ret = textIndexFile_.read(
          encodedW, static_cast<size_t>(nofBytes - (current - from)), current);
      current += ret;
      AD_CHECK_EQ(size_t(current - from), nofBytes);
      LOG(DEBUG) << "Decoding Simple8b code...\n";
      ad_utility::Simple8bCode::decode(encodedW, nofElements, ids.data());
      ids.resize(nofElements);
      ;
      delete[] encodedW;
      delete[] codebookW;
      writeAsciiListFile(wordIdsFn, ids);
    }
    LOG(DEBUG) << "Reading non-entity score list..." << std::endl;
    from = tbmd._cl._startScorelist;
    nofBytes =
        static_cast<size_t>(tbmd._cl._lastByte + 1 - tbmd._cl._startScorelist);
    ids.clear();
    ids.resize(nofElements + 250);
    uint64_t* encodedS = new uint64_t[nofBytes / 8];
    off_t current = from;
    size_t ret = textIndexFile_.read(&nofCodebookBytes, sizeof(off_t), current);
    LOG(DEBUG) << "Nof Codebook Bytes: " << nofCodebookBytes << '\n';
    AD_CHECK_EQ(sizeof(off_t), ret);
    current += ret;
    Score* codebookS = new Score[nofCodebookBytes / sizeof(Score)];
    ret = textIndexFile_.read(codebookS, nofCodebookBytes, current);
    current += ret;
    AD_CHECK_EQ(ret, size_t(nofCodebookBytes));
    ret = textIndexFile_.read(
        encodedS, static_cast<size_t>(nofBytes - (current - from)), current);
    current += ret;
    AD_CHECK_EQ(size_t(current - from), nofBytes);
    LOG(DEBUG) << "Decoding Simple8b code...\n";
    ad_utility::Simple8bCode::decode(encodedS, nofElements, ids.data());
    ids.resize(nofElements);
    delete[] encodedS;
    delete[] codebookS;
    writeAsciiListFile(scoresFn, ids);
  }
  {
    string eDocIdsFn = basename + ".docids.ent.ascii";
    string eWordIdsFn = basename + ".wordids.ent.ascii";
    string eScoresFn = basename + ".scores.ent.ascii";
    vector<Id> ids;

    auto nofElements = tbmd._entityCl._nofElements;
    if (nofElements == 0) return;
    LOG(DEBUG) << "Reading entity docId list..." << std::endl;
    auto from = tbmd._entityCl._startContextlist;
    auto nofBytes = static_cast<size_t>(tbmd._entityCl._startWordlist -
                                        tbmd._entityCl._startContextlist);
    ids.clear();
    ids.resize(nofElements + 250);
    uint64_t* encodedD = new uint64_t[nofBytes / 8];
    textIndexFile_.read(encodedD, nofBytes, from);
    LOG(DEBUG) << "Decoding Simple8b code...\n";
    ad_utility::Simple8bCode::decode(encodedD, nofElements, ids.data());
    ids.resize(nofElements);
    delete[] encodedD;
    writeAsciiListFile(eDocIdsFn, ids);

    if (tbmd._cl.hasMultipleWords()) {
      LOG(DEBUG) << "Reading entity wordId list..." << std::endl;
      from = tbmd._entityCl._startWordlist;
      nofBytes = static_cast<size_t>(tbmd._entityCl._startScorelist -
                                     tbmd._entityCl._startWordlist);

      ids.clear();
      ids.resize(nofElements + 250);
      uint64_t* encodedW = new uint64_t[nofBytes / 8];
      off_t current = from;
      size_t ret =
          textIndexFile_.read(&nofCodebookBytes, sizeof(off_t), current);
      LOG(DEBUG) << "Nof Codebook Bytes: " << nofCodebookBytes << '\n';
      AD_CHECK_EQ(sizeof(off_t), ret);
      current += ret;
      Id* codebookW = new Id[nofCodebookBytes / sizeof(Id)];
      ret = textIndexFile_.read(codebookW, nofCodebookBytes, current);
      current += ret;
      AD_CHECK_EQ(ret, size_t(nofCodebookBytes));
      ret = textIndexFile_.read(
          encodedW, static_cast<size_t>(nofBytes - (current - from)), current);
      current += ret;
      AD_CHECK_EQ(size_t(current - from), nofBytes);
      LOG(DEBUG) << "Decoding Simple8b code...\n";
      ad_utility::Simple8bCode::decode(encodedW, nofElements, ids.data());
      ids.resize(nofElements);
      ;
      delete[] encodedW;
      delete[] codebookW;
      writeAsciiListFile(eWordIdsFn, ids);
    }
    LOG(DEBUG) << "Reading entity score list..." << std::endl;
    from = tbmd._entityCl._startScorelist;
    nofBytes = static_cast<size_t>(tbmd._entityCl._lastByte + 1 -
                                   tbmd._entityCl._startScorelist);
    ids.clear();
    ids.resize(nofElements + 250);
    uint64_t* encodedS = new uint64_t[nofBytes / 8];
    off_t current = from;
    size_t ret = textIndexFile_.read(&nofCodebookBytes, sizeof(off_t), current);

    LOG(DEBUG) << "Nof Codebook Bytes: " << nofCodebookBytes << '\n';
    AD_CHECK_EQ(sizeof(off_t), ret);
    current += ret;
    Score* codebookS = new Score[nofCodebookBytes / sizeof(Score)];
    ret = textIndexFile_.read(codebookS, nofCodebookBytes, current);
    current += ret;
    AD_CHECK_EQ(ret, size_t(nofCodebookBytes));
    ret = textIndexFile_.read(
        encodedS, static_cast<size_t>(nofBytes - (current - from)), current);
    current += ret;
    AD_CHECK_EQ(size_t(current - from), nofBytes);
    LOG(DEBUG) << "Decoding Simple8b code...\n";
    ad_utility::Simple8bCode::decode(encodedS, nofElements, ids.data());
    ids.resize(nofElements);
    ;
    delete[] encodedS;
    delete[] codebookS;
    writeAsciiListFile(eScoresFn, ids);
  }
}
#endif

// _____________________________________________________________________________
size_t IndexImpl::getIndexOfBestSuitedElTerm(
    const vector<string>& terms) const {
  // It is beneficial to choose a term where no filtering by regular word id
  // is needed. Then the entity lists can be read directly from disk.
  // For others it is always necessary to reach wordlist and filter them
  // if such an entity list is taken, another intersection is necessary.

  // Apart from that, entity lists are usually larger by a factor.
  // Hence it makes sense to choose the smallest.

  // Heuristic: Always prefer no-filtering terms over otheres, then
  // pick the one with the smallest EL block to be read.
  std::vector<std::tuple<size_t, bool, size_t>> toBeSorted;
  for (size_t i = 0; i < terms.size(); ++i) {
    bool entityTerm = (terms[i][0] == '<' && terms[i].back() == '>');
    IdRange range;
    if (terms[i].back() == PREFIX_CHAR) {
      textVocab_.getIdRangeForFullTextPrefix(terms[i], &range);
    } else {
      if (entityTerm) {
        if (!vocab_.getId(terms[i], &range._first)) {
          LOG(DEBUG) << "Term: " << terms[i] << " not in entity vocabulary\n";
          return i;
        } else {
        }
      } else if (!textVocab_.getId(terms[i], &range._first)) {
        LOG(DEBUG) << "Term: " << terms[i] << " not in vocabulary\n";
        return i;
      }
      range._last = range._first;
    }
    const auto& tbmd =
        entityTerm ? textMeta_.getBlockInfoByEntityId(range._first.get())
                   : textMeta_.getBlockInfoByWordRange(range._first.get(),
                                                       range._last.get());
    toBeSorted.emplace_back(std::make_tuple(
        i, tbmd._firstWordId == tbmd._lastWordId, tbmd._entityCl._nofElements));
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
template <size_t I>
void IndexImpl::getECListForWordsAndSingleSub(
    const string& words, const vector<array<Id, I>>& subres,
    size_t subResMainCol, size_t limit, vector<array<Id, 3 + I>>& res) const {
  // Get context entity postings matching the words
  Index::WordEntityPostings wep = getContextEntityScoreListsForWords(words);

  // TODO: more code for efficienty.
  // Examine the possibility to branch if subresult is much larger
  // than the number of matching postings.
  // Could binary search then instead of create the map first.

  LOG(DEBUG) << "Filtering matching contexts and building cross-product...\n";
  vector<array<Id, 3 + I>> nonAggRes;
  if (!wep.cids_.empty()) {
    // Transform the sub res into a map from key entity to tuples
    ad_utility::HashMap<Id, vector<array<Id, I>>> subEs;
    for (size_t i = 0; i < subres.size(); ++i) {
      auto& tuples = subEs[subres[i][subResMainCol]];
      tuples.push_back(subres[i]);
    }
    // Test if each context is fitting.
    size_t currentContextFrom = 0;
    TextRecordIndex currentContext = wep.cids_[0];
    bool matched = false;
    for (size_t i = 0; i < wep.cids_.size(); ++i) {
      if (wep.cids_[i] != currentContext) {
        if (matched) {
          FTSAlgorithms::appendCrossProduct(wep, currentContextFrom, i, subEs,
                                            nonAggRes);
        }
        matched = false;
        currentContext = wep.cids_[i];
        currentContextFrom = i;
      }
      if (!matched) {
        matched = (subEs.contains(wep.eids_[i]));
      }
    }
  }
  FTSAlgorithms::aggScoresAndTakeTopKContexts(nonAggRes, limit, res);
}

template void IndexImpl::getECListForWordsAndSingleSub(
    const string& words, const vector<array<Id, 1>>& subres,
    size_t subResMainCol, size_t limit, vector<array<Id, 4>>& res) const;

template void IndexImpl::getECListForWordsAndSingleSub(
    const string& words, const vector<array<Id, 2>>& subres,
    size_t subResMainCol, size_t limit, vector<array<Id, 5>>& res) const;

// _____________________________________________________________________________
void IndexImpl::getECListForWordsAndTwoW1Subs(
    const string& words, const vector<array<Id, 1>> subres1,
    const vector<array<Id, 1>> subres2, size_t limit,
    vector<array<Id, 5>>& res) const {
  // Get context entity postings matching the words
  Index::WordEntityPostings wep = getContextEntityScoreListsForWords(words);

  // TODO: more code for efficiency.
  // Examine the possibility to branch if subresults are
  // much larger than the number of matching postings.
  // Could binary search in them, then instead of create sets first.

  LOG(DEBUG) << "Filtering matching contexts and building cross-product...\n";
  vector<array<Id, 5>> nonAggRes;
  if (!wep.cids_.empty()) {
    // Transform the sub res' into sets of entity Ids
    ad_utility::HashSet<Id> subEs1;
    ad_utility::HashSet<Id> subEs2;
    for (size_t i = 0; i < subres1.size(); ++i) {
      subEs1.insert(subres1[i][0]);
    }
    for (size_t i = 0; i < subres2.size(); ++i) {
      subEs2.insert(subres2[i][0]);
    }
    // Test if each context is fitting.
    size_t currentContextFrom = 0;
    TextRecordIndex currentContext = wep.cids_[0];
    bool matched = false;
    bool matched1 = false;
    bool matched2 = false;
    for (size_t i = 0; i < wep.cids_.size(); ++i) {
      if (wep.cids_[i] != currentContext) {
        if (matched) {
          FTSAlgorithms::appendCrossProduct(wep, currentContextFrom, i, subEs1,
                                            subEs2, nonAggRes);
        }
        matched = false;
        matched1 = false;
        matched2 = false;
        currentContext = wep.cids_[i];
        currentContextFrom = i;
      }
      if (!matched) {
        if (!matched1) {
          matched1 = (subEs1.contains(wep.eids_[i]));
        }
        if (!matched2) {
          matched2 = (subEs2.contains(wep.eids_[i]));
        }
        matched = matched1 && matched2;
      }
    }
  }
  FTSAlgorithms::aggScoresAndTakeTopKContexts(nonAggRes, limit, res);
}

// _____________________________________________________________________________
void IndexImpl::getECListForWordsAndSubtrees(
    const string& words,
    const vector<ad_utility::HashMap<Id, vector<vector<Id>>>>& subResMaps,
    size_t limit, vector<vector<Id>>& res) const {
  // Get context entity postings matching the words
  Index::WordEntityPostings wep = getContextEntityScoreListsForWords(words);

  LOG(DEBUG) << "Filtering matching contexts and building cross-product...\n";
  vector<vector<Id>> nonAggRes;
  if (!wep.cids_.empty()) {
    // Test if each context is fitting.
    size_t currentContextFrom = 0;
    TextRecordIndex currentContext = wep.cids_[0];
    bool matched = false;
    vector<bool> matchedSubs;
    matchedSubs.resize(subResMaps.size(), false);
    for (size_t i = 0; i < wep.cids_.size(); ++i) {
      if (wep.cids_[i] != currentContext) {
        if (matched) {
          FTSAlgorithms::appendCrossProduct(wep, currentContextFrom, i,
                                            subResMaps, nonAggRes);
        }
        matched = false;
        std::fill(matchedSubs.begin(), matchedSubs.end(), false);
        currentContext = wep.cids_[i];
        currentContextFrom = i;
      }
      if (!matched) {
        matched = true;
        for (size_t j = 0; j < matchedSubs.size(); ++j) {
          if (!matchedSubs[j]) {
            if (subResMaps[j].contains(wep.eids_[i])) {
              matchedSubs[j] = true;
            } else {
              matched = false;
            }
          }
        }
      }
    }
  }

  FTSAlgorithms::aggScoresAndTakeTopKContexts(nonAggRes, limit, res);
}

// _____________________________________________________________________________
size_t IndexImpl::getSizeEstimate(const string& words) const {
  size_t minElLength = std::numeric_limits<size_t>::max();
  // TODO vector can be of type std::string_view if called functions
  //  are updated to accept std::string_view instead of const std::string&
  std::vector<std::string> terms = absl::StrSplit(words, ' ');
  for (size_t i = 0; i < terms.size(); ++i) {
    IdRange range;
    bool entityTerm = (terms[i][0] == '<' && terms[i].back() == '>');
    if (terms[i].back() == PREFIX_CHAR) {
      if (!textVocab_.getIdRangeForFullTextPrefix(terms[i], &range)) {
        return 0;
      }
    } else {
      if (entityTerm) {
        if (!vocab_.getId(terms[i], &range._first)) {
          LOG(DEBUG) << "Term: " << terms[i] << " not in entity vocabulary\n";
          return 0;
        }
      } else if (!textVocab_.getId(terms[i], &range._first)) {
        LOG(DEBUG) << "Term: " << terms[i] << " not in vocabulary\n";
        return 0;
      }
      range._last = range._first;
    }
    const auto& tbmd =
        entityTerm ? textMeta_.getBlockInfoByEntityId(range._first.get())
                   : textMeta_.getBlockInfoByWordRange(range._first.get(),
                                                       range._last.get());
    if (minElLength > tbmd._entityCl._nofElements) {
      minElLength = tbmd._entityCl._nofElements;
    }
  }
  return 1 + minElLength / 100;
}

// _____________________________________________________________________________
void IndexImpl::getRhsForSingleLhs(const IdTable& in, Id lhsId,
                                   IdTable* result) const {
  LOG(DEBUG) << "Getting only rhs from a relation with " << in.size()
             << " elements by an Id key.\n";
  AD_CONTRACT_CHECK(result);
  AD_CONTRACT_CHECK(result->empty());

  // The second entry is unused.
  Id compareElem[] = {lhsId, Id::makeUndefined()};
  auto it = std::lower_bound(
      in.begin(), in.end(), compareElem,
      [](const auto& a, const auto& b) { return a[0] < b[0]; });

  while (it != in.end() && (*it)[0] == lhsId) {
    result->push_back({(*it)[1]});
    ++it;
  }

  LOG(DEBUG) << "Done. Matching right-hand-side EntityList now has "
             << result->size() << " elements."
             << "\n";
}

// _____________________________________________________________________________
void IndexImpl::setTextName(const string& name) { textMeta_.setName(name); }
