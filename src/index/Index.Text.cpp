// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <tuple>
#include <utility>
#include <stxxl/algorithm>
#include "./Index.h"
#include "../parser/ContextFileParser.h"
#include "../util/Simple8bCode.h"

// _____________________________________________________________________________
void Index::addTextFromContextFile(const string& contextFile) {
  string indexFilename = _onDiskBase + ".text.index";
  size_t nofLines = passContextFileForVocabulary(contextFile);
  _textVocab.writeToFile(_onDiskBase + ".text.vocabulary");
  calculateBlockBoundaries();
  TextVec v(nofLines);
  passContextFileIntoVector(contextFile, v);
  LOG(INFO) << "Sorting text index..." << std::endl;
  stxxl::sort(begin(v), end(v), SortText(), STXXL_MEMORY_TO_USE);
  LOG(INFO) << "Sort done." << std::endl;
  createTextIndex(indexFilename, v);
  openTextFileHandle();
}

// _____________________________________________________________________________
void Index::buildDocsDB(const string& docsFileName) {
  LOG(INFO) << "Building DocsDB...\n";
  ad_utility::File docsFile(docsFileName.c_str(), "r");
  ad_utility::File out(string(_onDiskBase + ".text.docsDB").c_str(), "w");
  off_t currentOffset = 0;
  char* buf = new char[BUFFER_SIZE_DOCSFILE_LINE];
  string line;
  while (docsFile.readLine(&line, buf, BUFFER_SIZE_DOCSFILE_LINE)) {
    out.writeLine(line);
    size_t tab = line.find('\t');
    Id contextId = static_cast<Id>(atol(line.substr(0, tab).c_str()));
    _docsDB._offsets.emplace_back(pair<Id, off_t>(contextId, currentOffset));
    // One extra byte for the newline:
    currentOffset += line.size() + 1;
  }
  off_t startOfOffsets = currentOffset;
  for (auto& p : _docsDB._offsets) {
    out.write(&p, sizeof(p));
  }
  out.write(&startOfOffsets, sizeof(startOfOffsets));
  out.close();
  LOG(INFO) << "DocsDB done.\n";
}

// _____________________________________________________________________________
void Index::addTextFromOnDiskIndex() {
  _textVocab.readFromFile(_onDiskBase + ".text.vocabulary");
  _textIndexFile.open(string(_onDiskBase + ".text.index").c_str(), "r");
  AD_CHECK(_textIndexFile.isOpen());
  off_t metaFrom;
  off_t metaTo = _textIndexFile.getLastOffset(&metaFrom);
  unsigned char* buf = new unsigned char[metaTo - metaFrom];
  _textIndexFile.read(buf, static_cast<size_t>(metaTo - metaFrom), metaFrom);
  _textMeta.createFromByteBuffer(buf);
  delete[] buf;
  LOG(INFO) << "Reading excerpt offsets from file." << endl;
  std::ifstream f(string(_onDiskBase + ".text.docsDB").c_str());
  if (f.good()) {
    f.close();
    LOG(INFO) << "Docs DB exists. Reading excerpt offsets...\n";
    //_docsDB.init(string(_onDiskBase + ".text.docsDB"));
    LOG(INFO) << "Done reading excerpt offsets." << endl;
  } else {
    LOG(INFO) << "No Docs DB found.\n";
    f.close();
  }
  LOG(INFO) << "Registered text index: " << _textMeta.statistics()
            << std::endl;
}

// _____________________________________________________________________________
size_t Index::passContextFileForVocabulary(string const& contextFile) {
  LOG(INFO) << "Making pass over ContextFile " << contextFile <<
            " for vocabulary." << std::endl;
  ContextFileParser::Line line;
  ContextFileParser p(contextFile);
  std::unordered_set<string> items;
  size_t i = 0;
  while (p.getLine(line)) {
    ++i;
    if (!line._isEntity) {
      items.insert(line._word);
    }
    if (i % 10000000 == 0) {
      LOG(INFO) << "Lines processed: " << i << '\n';
    }
  }
  LOG(INFO) << "Pass done.\n";
  _textVocab.createFromSet(items);
  return i;
}

// _____________________________________________________________________________
void Index::passContextFileIntoVector(string const& contextFile,
                                      Index::TextVec& vec) {
  LOG(INFO) << "Making pass over ContextFile " << contextFile
            << " and creating stxxl vector.\n";
  ContextFileParser::Line line;
  ContextFileParser p(contextFile);
  std::unordered_map<string, Id> vocabMap = _vocab.asMap();
  size_t i = 0;
  // write using vector_bufwriter
  TextVec::bufwriter_type writer(vec);
  std::unordered_map<Id, Score> wordsInContext;
  std::unordered_map<Id, Score> entitiesInContext;
  Id currentContext = 0;
  size_t entityNotFoundErrorMsgCount = 0;
  while (p.getLine(line)) {
    if (line._contextId != currentContext) {
      addContextToVector(writer, currentContext, wordsInContext,
                         entitiesInContext);
      currentContext = line._contextId;
      wordsInContext.clear();
      entitiesInContext.clear();
    }
    if (line._isEntity) {
      Id eid;
      if (_vocab.getId(line._word, &eid)) {
        entitiesInContext[eid] += line._score;
      } else {
        if (entityNotFoundErrorMsgCount < 20) {
          LOG(WARN) << "Entity from text not in KB: " << line._word << '\n';
          if (++entityNotFoundErrorMsgCount == 20) {
            LOG(WARN) << "There are more entities not in the KB..."
                  " suppressing further warnings...\n";
          }
        }
      }
    } else {
      Id wid;
#ifndef NDEBUG
      bool ret = _textVocab.getId(line._word, &wid);
      assert(ret);
#else
      _textVocab.getId(line._word, &wid);
      #endif

      wordsInContext[wid] += line._score;
    }
    ++i;
    if (i % 10000000 == 0) {
      LOG(INFO) << "Lines processed: " << i << '\n';
    }
  }
  addContextToVector(writer, currentContext, wordsInContext, entitiesInContext);
  writer.finish();
  LOG(INFO) << "Pass done.\n";
}

// _____________________________________________________________________________
void Index::addContextToVector(
    Index::TextVec::bufwriter_type& writer,
    Id context, const unordered_map<Id, Score>& words,
    const unordered_map<Id, Score>& entities) {
  // Determine blocks for each word and each entity.
  // Add the posting to each block.
  std::unordered_set<Id> touchedBlocks;
  for (auto it = words.begin(); it != words.end(); ++it) {
    Id blockId = getWordBlockId(it->first);
    touchedBlocks.insert(blockId);
    writer << std::make_tuple(blockId, context, it->first, it->second, false);
  }

  for (auto it = entities.begin(); it != entities.end(); ++it) {
    Id blockId = getEntityBlockId(it->first);
    touchedBlocks.insert(blockId);
    writer << std::make_tuple(blockId, context, it->first, it->second, false);
  }

  // All entities have to be written in the entity list part for each block.
  // Ensure that they are added only once for each block.
  // For example, there could be both words computer and computing
  // in the same context. Still, co-occurring entities would only have to be
  // written to a comp* block once.
  for (Id blockId : touchedBlocks) {
    for (auto it = entities.begin(); it != entities.end(); ++it) {
      writer << std::make_tuple(blockId, context, it->first, it->second, true);
    }
  }

}

// _____________________________________________________________________________
void Index::createTextIndex(const string& filename, const Index::TextVec& vec) {
  ad_utility::File out(filename.c_str(), "w");
  _currentoff_t = 0;
  // Detect block boundaries from the main key of the vec.
  // Write the data for each block.
  // First, there's the classic lists, then the additional entity ones.
  Id currentBlockId = 0;
  Id currentMinWordId = std::numeric_limits<Id>::max();
  Id currentMaxWordId = std::numeric_limits<Id>::min();
  vector<Posting> classicPostings;
  vector<Posting> entityPostings;
  for (TextVec::bufreader_type reader(vec); !reader.empty(); ++reader) {
    TextBlockMetaData tbmd;
    if (std::get<0>(*reader) != currentBlockId) {
      AD_CHECK(classicPostings.size() > 0);
      ContextListMetaData classic = writePostings(out, classicPostings, true);
      ContextListMetaData entity = writePostings(out, entityPostings, false);
      _textMeta.addBlock(TextBlockMetaData(
          currentMinWordId,
          currentMaxWordId,
          classic,
          entity
      ));
      classicPostings.clear();
      entityPostings.clear();
      currentBlockId = std::get<0>(*reader);
      currentMinWordId = std::get<2>(*reader);
      currentMaxWordId = std::get<2>(*reader);
    }
    if (!std::get<4>(*reader)) {
      classicPostings.emplace_back(std::make_tuple(
          std::get<1>(*reader),
          std::get<2>(*reader),
          std::get<3>(*reader)
      ));
      if (std::get<2>(*reader) < currentMinWordId) {
        currentMinWordId = std::get<2>(*reader);
      }
      if (std::get<2>(*reader) > currentMaxWordId) {
        currentMaxWordId = std::get<2>(*reader);
      }

    } else {
      entityPostings.emplace_back(std::make_tuple(
          std::get<1>(*reader),
          std::get<2>(*reader),
          std::get<3>(*reader)
      ));
    }
  }
  LOG(INFO) << "Done creating text index." << std::endl;
  LOG(INFO) << "Writing statistics:\n"
            << _textMeta.statistics() << std::endl;

  LOG(INFO) << "Writing Meta data to index file...\n";
  out << _textMeta;
  off_t startOfMeta = _textMeta.getOffsetAfter();
  out.write(&startOfMeta, sizeof(startOfMeta));
  out.close();
  LOG(INFO) << "Text index done.\n";
}

// _____________________________________________________________________________
ContextListMetaData Index::writePostings(ad_utility::File& out,
                                         const vector<Posting>& postings,
                                         bool skipWordlistIfAllTheSame) {
  ContextListMetaData meta;
  meta._nofElements = postings.size();
  if (meta._nofElements == 0) {
    meta._startContextlist = _currentoff_t;
    meta._startWordlist = _currentoff_t;
    meta._startScorelist = _currentoff_t;
    meta._lastByte = _currentoff_t - 1;
    return meta;
  }

  // Collect the individual lists
  // Context lists are gap encoded, word and score lists frequency encoded.
  Id* contextList = new Id[meta._nofElements];
  Id* wordList = new Id[meta._nofElements];
  Score* scoreList = new Score[meta._nofElements];

  size_t n = 0;

  IdCodeMap wordCodeMap;
  IdCodebook wordCodebook;
  ScoreCodeMap scoreCodeMap;
  ScoreCodebook scoreCodebook;

  createCodebooks(postings, wordCodeMap, wordCodebook, scoreCodeMap,
                  scoreCodebook);

  Id lastContext = std::get<0>(postings[0]);
  contextList[n] = lastContext;
  wordList[n] = wordCodeMap[std::get<1>(postings[0])];
  scoreList[n] = scoreCodeMap[std::get<2>(postings[0])];
  ++n;

  for (auto it = postings.begin() + 1; it < postings.end(); ++it) {
    Id gap = std::get<0>(*it) - lastContext;
    contextList[n] = gap;
    lastContext = std::get<0>(*it);
    wordList[n] = wordCodeMap[std::get<1>(*it)];
    scoreList[n] = scoreCodeMap[std::get<2>(*it)];
    ++n;
  }

  AD_CHECK(meta._nofElements == n);

  // Do the actual writing:
  size_t bytes = 0;

  // Write context list:
  meta._startContextlist = _currentoff_t;
  bytes = writeList(contextList, meta._nofElements, out);
  _currentoff_t += bytes;

  // Write word list:
  // This can be skipped if we're writing classic lists and there
  // is only one distinct wordId in the block, since this Id is already
  // stored in the meta data.
  meta._startWordlist = _currentoff_t;
  if (!skipWordlistIfAllTheSame || wordCodebook.size() > 1) {
    _currentoff_t += writeCodebook(wordCodebook, out);
    bytes = writeList(wordList, meta._nofElements, out);
    _currentoff_t += bytes;
  }

  // Write scores
  meta._startScorelist = _currentoff_t;
  _currentoff_t += writeCodebook(scoreCodebook, out);
  bytes = writeList(scoreList, meta._nofElements, out);
  _currentoff_t += bytes;

  meta._lastByte = _currentoff_t - 1;

  delete[] contextList;
  delete[] wordList;
  delete[] scoreList;

  return meta;
}

// _____________________________________________________________________________
void Index::calculateBlockBoundaries() {
  LOG(INFO) << "Calculating block boundaries...\n";
  // Go through the vocabulary
  // Start a new block whenever a word is
  // 1) The last word in the corpus
  // 2) shorter than the minimum prefix length
  // 3) The next word is shorter than the minimum prefix length
  // 4) word.substring(0, MIN_PREFIX_LENGTH) is different from the next.
  // A block boundary is always the last WordId in the block.
  // this way std::lower_bound will point to the correct bracket.
  for (size_t i = 0; i < _textVocab.size() - 1; ++i) {
    if (_textVocab[i].size() < MIN_WORD_PREFIX_SIZE ||
        (_textVocab[i + 1].size() < MIN_WORD_PREFIX_SIZE) ||
        _textVocab[i].substr(0, MIN_WORD_PREFIX_SIZE) !=
        _textVocab[i + 1].substr(0, MIN_WORD_PREFIX_SIZE)) {
      _blockBoundaries.push_back(i);
    }
  }
  _blockBoundaries.push_back(_textVocab.size() - 1);
  LOG(INFO) << "Done. Got " << _blockBoundaries.size() <<
            " for a vocabulary with " << _textVocab.size() << " words.\n";
}

// _____________________________________________________________________________
Id Index::getWordBlockId(Id wordId) const {
  return static_cast<Id>(
      std::lower_bound(_blockBoundaries.begin(), _blockBoundaries.end(),
                       wordId) - _blockBoundaries.begin());
}

// _____________________________________________________________________________
Id Index::getEntityBlockId(Id entityId) const {
  return entityId + _blockBoundaries.size();
}

// _____________________________________________________________________________
template<typename Numeric>
size_t Index::writeList(Numeric* data, size_t nofElements,
                        ad_utility::File& file) const {
  if (nofElements > 0) {
    uint64_t* encoded = new uint64_t[nofElements];
    size_t size = ad_utility::Simple8bCode::encode(data, nofElements,
                                                   encoded);
    size_t ret = file.write(encoded, size);
    AD_CHECK_EQ(size, ret);
    delete[] encoded;
    return size;
  } else {
    return 0;
  }
}

// _____________________________________________________________________________
void Index::createCodebooks(const vector<Index::Posting>& postings,
                            Index::IdCodeMap& wordCodemap,
                            Index::IdCodebook& wordCodebook,
                            Index::ScoreCodeMap& scoreCodemap,
                            Index::ScoreCodebook& scoreCodebook) const {
  unordered_map<Id, size_t> wfMap;
  unordered_map<Score, size_t> sfMap;
  for (const auto& p: postings) {
    wfMap[std::get<1>(p)] = 0;
    sfMap[std::get<2>(p)] = 0;
  }
  for (const auto& p: postings) {
    ++wfMap[std::get<1>(p)];
    ++sfMap[std::get<2>(p)];
  }
  vector<std::pair<Id, size_t> > wfVec;
  wfVec.resize(wfMap.size());
  size_t i = 0;
  for (auto it = wfMap.begin(); it != wfMap.end(); ++it) {
    wfVec[i].first = it->first;
    wfVec[i].second = it->second;
    ++i;
  }
  vector<std::pair<Score, size_t> > sfVec;
  sfVec.resize(sfMap.size());
  i = 0;
  for (auto it = sfMap.begin(); it != sfMap.end(); ++it) {
    sfVec[i].first = it->first;
    sfVec[i].second = it->second;
    ++i;
  }
  std::sort(wfVec.begin(), wfVec.end(),
            [](const std::pair<Id, size_t>& a,
               const std::pair<Id, size_t>& b) {
              return a.second < b.second;
            });
  std::sort(sfVec.begin(), sfVec.end(),
            [](const std::pair<Score, size_t>& a,
               const std::pair<Score, size_t>& b) {
              return a.second < b.second;
            });
  for (size_t i = 0; i < wfVec.size(); ++i) {
    wordCodebook.push_back(wfVec[i].first);
    wordCodemap[wfVec[i].first] = i;
  }
  for (size_t i = 0; i < sfVec.size(); ++i) {
    scoreCodebook.push_back(sfVec[i].first);
    scoreCodemap[sfVec[i].first] = i;
  }
}

// _____________________________________________________________________________
template<class T>
size_t Index::writeCodebook(const vector<T>& codebook,
                            ad_utility::File& file) const {
  off_t byteSizeOfCodebook = sizeof(T) * codebook.size();
  file.write(&byteSizeOfCodebook, sizeof(byteSizeOfCodebook));
  file.write(codebook.data(), byteSizeOfCodebook);
  return byteSizeOfCodebook + sizeof(byteSizeOfCodebook);
}

// _____________________________________________________________________________
void Index::openTextFileHandle() {
  AD_CHECK(_onDiskBase.size() > 0);
  _textIndexFile.open(string(_onDiskBase + ".text.index").c_str(), "r");
}

// _____________________________________________________________________________
const string& Index::wordIdToString(Id id) const {
  return _textVocab[id];
}

// _____________________________________________________________________________
void Index::getContextListForWords(const string& words,
                                   Index::WidthTwoList* result) const {
  LOG(DEBUG) << "In getContextListForWords...\n";
  auto terms = ad_utility::split(words, ' ');
  AD_CHECK(terms.size() > 0);

  if (terms.size() > 1) {
    AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
             "No multi word queries, yet.")
  }

  string term = terms[0];
  vector<Id> cids;
  vector<Score> scores;
  getWordPostingsForTerm(term, cids, scores);

  LOG(DEBUG) << "Packing lists into a ResultTable\n...";
  result->reserve(cids.size() + 2);
  result->resize(cids.size());
  for (size_t i = 0; i < cids.size(); ++i) {
    (*result)[i] = {{cids[i], scores[i]}};
  }
  LOG(DEBUG) << "Done with getContextListForWords.\n";
}

// _____________________________________________________________________________
void Index::getWordPostingsForTerm(const string& term, vector<Id>& cids,
                                   vector<Score>& scores) const {
  LOG(DEBUG) << "Getting word postings for term: " << term << '\n';
  IdRange idRange;
  if (term[term.size() - 1] == PREFIX_CHAR) {
    LOG(INFO) << "Prefix: " << term << " not in vocabulary\n";
    if (!_textVocab.getIdRangeForFullTextPrefix(term, &idRange)) {
      return;
    }
  } else {
    if (!_textVocab.getId(term, &idRange._first)) {
      LOG(INFO) << "Term: " << term << " not in vocabulary\n";
      return;
    }
    idRange._last = idRange._first;
  }
  const auto& tbmd = _textMeta.getBlockInfoByWordRange(idRange._first,
                                                       idRange._last);
  if (tbmd._cl.hasMultipleWords()) {
    vector<Id> blockCids;
    vector<Id> blockWids;
    vector<Score> blockScores;
    readGapComprList(tbmd._cl._nofElements,
                     tbmd._cl._startContextlist,
                     static_cast<size_t>(tbmd._cl._startWordlist -
                                         tbmd._cl._startContextlist),
                     blockCids);
    readFreqComprList(tbmd._cl._nofElements,
                      tbmd._cl._startWordlist,
                      static_cast<size_t>(tbmd._cl._startScorelist -
                                          tbmd._cl._startWordlist),
                      blockWids);
    readFreqComprList(tbmd._cl._nofElements,
                      tbmd._cl._startScorelist,
                      static_cast<size_t>(tbmd._cl._lastByte + 1 -
                                          tbmd._cl._startScorelist),
                      blockScores);
    filterByRange(idRange, blockCids, blockWids, blockScores,
                  cids, scores);
  } else {
    readGapComprList(tbmd._cl._nofElements,
                     tbmd._cl._startContextlist,
                     static_cast<size_t>(tbmd._cl._startWordlist -
                                         tbmd._cl._startContextlist),
                     cids);
    readFreqComprList(tbmd._cl._nofElements,
                      tbmd._cl._startScorelist,
                      static_cast<size_t>(tbmd._cl._lastByte + 1 -
                                          tbmd._cl._startScorelist),
                      scores);
  }
  LOG(DEBUG) << "Word postings for term: " << term
             << ": cids: " << cids.size() << " scores " << scores.size() <<
             '\n';
}

// _____________________________________________________________________________
void Index::getECListForWords(const string& words,
                              Index::WidthThreeList* result) const {
  LOG(DEBUG) << "In getECListForWords...\n";
  auto terms = ad_utility::split(words, ' ');
  AD_CHECK(terms.size() > 0);

  if (terms.size() > 1) {
    AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
             "No multi word queries, yet.")
  }

  string term = terms[0];
  vector<Id> cids;
  vector<Id> eids;
  vector<Score> scores;
  getEntityPostingsForTerm(term, cids, eids, scores);

  // TODO: Make n variable.
  aggScoresAndTakeTopKContexts(cids, eids, scores, 1, result);
  LOG(DEBUG) << "Done with getECListForWords.\n";
}

// _____________________________________________________________________________
void Index::getEntityPostingsForTerm(const string& term, vector<Id>& cids,
                                     vector<Id>& eids,
                                     vector<Score>& scores) const {
  IdRange idRange;
  if (term[term.size() - 1] == PREFIX_CHAR) {
    LOG(INFO) << "Prefix: " << term << " not in vocabulary\n";
    if (!_textVocab.getIdRangeForFullTextPrefix(term, &idRange)) {
      return;
    }
  } else {
    if (!_textVocab.getId(term, &idRange._first)) {
      LOG(INFO) << "Term: " << term << " not in vocabulary\n";
      return;
    }
    idRange._last = idRange._first;
  }
  const auto& tbmd = _textMeta.getBlockInfoByWordRange(idRange._first,
                                                       idRange._last);

  if (!tbmd._cl.hasMultipleWords() || (tbmd._firstWordId == idRange._first &&
                                       tbmd._lastWordId == idRange._last)) {
    // CASE: Only one word in the block or full block should be matched.
    // Hence we can just read the entity CL lists for co-occurring
    // entity postings.
    readGapComprList(tbmd._entityCl._nofElements,
                     tbmd._entityCl._startContextlist,
                     static_cast<size_t>(tbmd._entityCl._startWordlist -
                                         tbmd._entityCl._startContextlist),
                     cids);
    readFreqComprList(tbmd._entityCl._nofElements,
                      tbmd._entityCl._startWordlist,
                      static_cast<size_t>(tbmd._entityCl._startScorelist -
                                          tbmd._entityCl._startWordlist),
                      eids);
    readFreqComprList(tbmd._entityCl._nofElements,
                      tbmd._entityCl._startScorelist,
                      static_cast<size_t>(tbmd._entityCl._lastByte + 1 -
                                          tbmd._entityCl._startScorelist),
                      scores);
  } else {
    // CASE: more than one word in the block.
    // Need to obtain matching postings for regular words and intersect for
    // a list of matching contexts.
    vector<Id> matchingContexts;
    vector<Score> matchingContextScores;
    getWordPostingsForTerm(term, matchingContexts, matchingContextScores);

    // Read the full lists
    vector<Id> eBlockCids;
    vector<Id> eBlockWids;
    vector<Score> eBlockScores;
    readGapComprList(tbmd._entityCl._nofElements,
                     tbmd._entityCl._startContextlist,
                     static_cast<size_t>(tbmd._entityCl._startWordlist -
                                         tbmd._entityCl._startContextlist),
                     eBlockCids);
    readFreqComprList(tbmd._entityCl._nofElements,
                      tbmd._entityCl._startWordlist,
                      static_cast<size_t>(tbmd._entityCl._startScorelist -
                                          tbmd._entityCl._startWordlist),
                      eBlockWids);
    readFreqComprList(tbmd._entityCl._nofElements,
                      tbmd._entityCl._startScorelist,
                      static_cast<size_t>(tbmd._entityCl._lastByte + 1 -
                                          tbmd._entityCl._startScorelist),
                      eBlockScores);
    intersect(matchingContexts, matchingContextScores, eBlockCids, eBlockWids,
              eBlockScores, cids, eids, scores);
  }
}


// _____________________________________________________________________________
template<typename T>
void Index::readGapComprList(size_t nofElements, off_t from, size_t nofBytes,
                             vector<T>& result) const {
  LOG(DEBUG) << "Reading gap-encoded list from disk...\n";
  LOG(TRACE) << "NofElements: " << nofElements << ", from: " << from <<
             ", nofBytes: " << nofBytes << '\n';
  result.resize(nofElements + 250);
  uint64_t* encoded = new uint64_t[nofBytes / 8];
  _textIndexFile.read(encoded, nofBytes, from);
  LOG(DEBUG) << "Decoding Simple8b code...\n";
  ad_utility::Simple8bCode::decode(encoded, nofElements, result.data());
  LOG(DEBUG) << "Reverting gaps to actual IDs...\n";
  T id = 0;
  for (size_t i = 0; i < result.size(); ++i) {
    id += result[i];
    result[i] = id;
  }
  result.resize(nofElements);
  delete[] encoded;
  LOG(DEBUG) << "Done reading gap-encoded list. Size: " << result.size() <<
             "\n";
}

// _____________________________________________________________________________
template<typename T>
void Index::readFreqComprList(size_t nofElements, off_t from, size_t nofBytes,
                              vector<T>& result) const {
  LOG(DEBUG) << "Reading frequency-encoded list from disk...\n";
  LOG(TRACE) << "NofElements: " << nofElements << ", from: " << from <<
             ", nofBytes: " << nofBytes << '\n';
  size_t nofCodebookBytes;
  uint64_t* encoded = new uint64_t[nofElements];
  result.resize(nofElements + 250);
  off_t current = from;
  size_t ret = _textIndexFile.read(&nofCodebookBytes, sizeof(off_t), current);
  LOG(TRACE) << "Nof Codebook Bytes: " << nofCodebookBytes << '\n';
  AD_CHECK_EQ(sizeof(off_t), ret);
  current += ret;
  T* codebook = new T[nofCodebookBytes / sizeof(T)];
  ret = _textIndexFile.read(codebook, nofCodebookBytes);
  current += ret;
  AD_CHECK_EQ(ret, size_t(nofCodebookBytes));
  ret = _textIndexFile.read(encoded,
                            static_cast<size_t>(nofBytes - (current - from)));
  current += ret;
  AD_CHECK_EQ(current - from, nofBytes);
  LOG(DEBUG) << "Decoding Simple8b code...\n";
  ad_utility::Simple8bCode::decode(encoded, nofElements, result.data());
  LOG(DEBUG) << "Reverting frequency encoded items to actual IDs...\n";
  result.resize(nofElements);
  for (size_t i = 0; i < result.size(); ++i) {
    result[i] = codebook[result[i]];
  }
  delete[] encoded;
  delete[] codebook;
  LOG(DEBUG) << "Done reading frequency-encoded list. Size: " <<
             result.size() << "\n";
}

// _____________________________________________________________________________
void Index::filterByRange(const IdRange& idRange, const vector<Id>& blockCids,
                          const vector<Id>& blockWids,
                          const vector<Score>& blockScores,
                          vector<Id>& resultCids,
                          vector<Score>& resultScores) const {
  AD_CHECK(blockCids.size() == blockWids.size());
  AD_CHECK(blockCids.size() == blockScores.size());
  LOG(DEBUG) << "Filtering " << blockCids.size() <<
             " elements by ID range...\n";

  resultCids.resize(blockCids.size() + 2);
  resultScores.resize(blockCids.size() + 2);
  size_t nofResultElements = 0;

  for (size_t i = 0; i < blockWids.size(); ++i) {
    if (blockWids[i] >= idRange._first && blockWids[i] <= idRange._last) {
      resultCids[nofResultElements] = blockCids[i];
      resultScores[nofResultElements++] = blockScores[i];
    }
  }

  resultCids.resize(nofResultElements);
  resultScores.resize(nofResultElements);

  AD_CHECK(resultCids.size() == resultScores.size());
  LOG(DEBUG) << "Filtering by ID range done. Result has " <<
             resultCids.size() << " elements.\n";
}

// _____________________________________________________________________________
void Index::getTopKByScores(const vector<Id>& cids, const vector<Score>& scores,
                            size_t k, Index::WidthOneList* result) const {
  AD_CHECK_EQ(cids.size(), scores.size());
  k = std::min(k, cids.size());
  LOG(DEBUG) << "Call getTopKByScores (partial sort of " << cids.size()
             << " contexts by score)...\n";
  vector<size_t> indices;
  indices.resize(scores.size());
  for (size_t i = 0; i < indices.size(); ++i) {
    indices[i] = i;
  }
  LOG(DEBUG) << "Doing the partial sort...\n";
  std::partial_sort(indices.begin(), indices.begin() + k, indices.end(),
                    [&scores](size_t a, size_t b) {
                      return scores[a] > scores[b];
                    });
  LOG(DEBUG) << "Packing the final WidthOneList of cIds...\n";
  result->reserve(k + 2);
  result->resize(k);
  for (size_t i = 0; i < k; ++i) {
    (*result)[i] = {{cids[indices[i]]}};
  }
  LOG(DEBUG) << "Done with getTopKByScores.\n";
}

// _____________________________________________________________________________
void Index::aggScoresAndTakeTopKContexts(const vector<Id>& cids,
                                         const vector<Id>& eids,
                                         const vector<Score>& scores,
                                         size_t k,
                                         Index::WidthThreeList* result) const {
  AD_CHECK_EQ(cids.size(), eids.size());
  AD_CHECK_EQ(cids.size(), scores.size());
  LOG(DEBUG) << "Going from an entity, context and score list of size: "
             << cids.size() << " elements to a table with distinct entities "
             << "and at most " << k << " contexts per entity.\n";

  // The default case where k == 1 can use a map for a O(n) solution
  if (k == 1) {
    aggScoresAndTakeTopContext(cids, eids, scores, result);
    return;
  }

  // Otherwise use a max heap of size k for the context scores  to achieve
  // O(k log n)
  AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
           "> 1 contexts per entity not yet implemented.")


  // The result is NOT sorted due to the usage of maps.
  // Resorting the result is a seperate operation now.
  // Benefit 1) it's not always necessary to sort.
  // Benefit 2) The result size can be MUCH smaller than n.
  LOG(DEBUG) << "Done. There are " << result->size() <<
             " entity-score-context tuples now.\n";
}

// _____________________________________________________________________________
void Index::aggScoresAndTakeTopContext(const vector<Id>& cids,
                                       const vector<Id>& eids,
                                       const vector<Score>& scores,
                                       Index::WidthThreeList* result) const {
  LOG(DEBUG) << "Special case with 1 contexts per entity...\n";
  typedef unordered_map<Id, pair<Score, pair<Id, Score>>> AggMap;
  AggMap map;
  for (size_t i = 0; i < eids.size(); ++i) {
    if (map.count(eids[i]) == 0) {
      map[eids[i]] = pair<Score, pair<Id, Score>>(
          1, pair<Id, Score>(cids[i], scores[i]));
    } else {
      auto& val = map[eids[i]];
      val.first += 1;
      if (val.second.second < scores[i]) {
        val.second = pair<Id, Score>(cids[i], scores[i]);
      }
    }
  }
  result->reserve(map.size() + 2);
  result->resize(map.size());
  size_t n = 0;
  for (auto it = map.begin(); it != map.end(); ++it) {
    (*result)[n++] =
        array<Id, 3>{{
                         it->first,  // entity
                         static_cast<Id>(it->second.first),  // entity score
                         it->second.second.first  // top context Id
                     }};
  }
  AD_CHECK_EQ(n, result->size());
  LOG(DEBUG) << "Done. There are " << result->size() <<
             " entity-score-context tuples now.\n";
}

// _____________________________________________________________________________
void Index::intersect(const vector<Id>& matchingContexts,
                      const vector<Score>& matchingContextScores,
                      const vector<Id>& eBlockCids,
                      const vector<Id>& eBlockWids,
                      const vector<Score>& eBlockScores,
                      vector<Id>& resultCids,
                      vector<Id>& resultEids,
                      vector<Score>& resultScores) const {
  LOG(DEBUG) << "Intersection to filter the entity postings from a block "
             << "so that only matching ones remain\n";
  LOG(DEBUG) << "matchingContexts size: " << matchingContexts.size() << '\n';
  LOG(DEBUG) << "eBlockCids size: " << eBlockCids.size() << '\n';
  resultCids.resize(eBlockCids.size());
  resultEids.resize(eBlockCids.size());
  resultScores.resize(eBlockCids.size());
  // Cast away constness so we can add sentinels that will be removed
  // in the end and create and add those sentinels.
  // Note: this is only efficient if capacity + 2 >= size for the input
  // context lists. For now, we assume that all lists are read from disk
  // where they had more than enough size allocated.
  auto& l1 = const_cast<vector<Id>&>(matchingContexts);
  auto& l2 = const_cast<vector<Id>&>(eBlockCids);
  // The two below are needed for the final sentinel match
  auto& wids = const_cast<vector<Id>&>(eBlockWids);
  auto& scores = const_cast<vector<Score>&>(eBlockScores);

  Id sent1 = std::numeric_limits<Id>::max();
  Id sent2 = std::numeric_limits<Id>::max() - 1;
  Id sentMatch = std::numeric_limits<Id>::max() - 2;

  l1.push_back(sentMatch);
  l2.push_back(sentMatch);
  l1.push_back(sent1);
  l2.push_back(sent2);
  wids.push_back(0);
  scores.push_back(0);

  size_t i = 0;
  size_t j = 0;
  size_t n = 0;

  while (l1[i] < sent1) {
    while (l1[i] < l2[j]) { ++i; }
    while (l2[j] < l1[i]) { ++j; }
    while (l1[i] == l2[j]) {
      // Make sure we get all matching elements from the entity list (l2)
      // that match the current context.
      // If there are multiple elements for that context in l1,
      // we can safely skip them unless we want to incorporate the scores
      // later on.
      resultCids[n] = l2[j];
      resultEids[n] = wids[j];
      resultScores[n++] = scores[j++];
    }
    ++i;
  }

  // Remove sentinels
  l1.resize(l1.size() - 2);
  l2.resize(l2.size() - 2);
  wids.resize(wids.size() - 1);
  scores.resize(scores.size() - 1);
  resultCids.resize(n - 1);
  resultEids.resize(n - 1);
  resultScores.resize(n - 1);
  LOG(DEBUG) << "Intersection done. Size: " << resultCids.size() << "\n";
}
