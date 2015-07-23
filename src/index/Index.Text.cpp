// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <tuple>
#include <utility>
#include <stxxl/algorithm>
#include "./Index.h"
#include "../parser/ContextFileParser.h"
#include "../util/Simple8bCode.h"
#include "./FTSAlgorithms.h"

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
  char *buf = new char[BUFFER_SIZE_DOCSFILE_LINE];
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
  unsigned char *buf = new unsigned char[metaTo - metaFrom];
  _textIndexFile.read(buf, static_cast<size_t>(metaTo - metaFrom), metaFrom);
  _textMeta.createFromByteBuffer(buf);
  delete[] buf;
  LOG(INFO) << "Reading excerpt offsets from file." << endl;
  std::ifstream f(string(_onDiskBase + ".text.docsDB").c_str());
  if (f.good()) {
    f.close();
    LOG(INFO) << "Docs DB exists. Reading excerpt offsets...\n";
    _docsDB.init(string(_onDiskBase + ".text.docsDB"));
    LOG(INFO) << "Done reading excerpt offsets." << endl;
  } else {
    LOG(INFO) << "No Docs DB found.\n";
    f.close();
  }
  LOG(INFO) << "Registered text index: " << _textMeta.statistics()
            << std::endl;
}

// _____________________________________________________________________________
size_t Index::passContextFileForVocabulary(string const
                                           & contextFile) {
  LOG(INFO)

    << "Making pass over ContextFile " << contextFile <<
    " for vocabulary." <<
    std::endl;
  ContextFileParser::Line line;
  ContextFileParser p(contextFile);
  std::unordered_set<string> items;
  size_t i = 0;
  while (p.
      getLine(line)
      ) {
    ++
        i;
    if (!line._isEntity) {
      items.
          insert(line
                     ._word);
    }
    if (i % 10000000 == 0) {
      LOG(INFO)

        << "Lines processed: " << i << '\n';
    }
  }
  LOG(INFO)

    << "Pass done.\n";
  _textVocab.
      createFromSet(items);
  return
      i;
}

// _____________________________________________________________________________
void Index::passContextFileIntoVector(string const
                                      & contextFile,
                                      Index::TextVec& vec
) {
  LOG(INFO)

    << "Making pass over ContextFile " << contextFile
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
  while (p.
      getLine(line)
      ) {
    if (line._contextId != currentContext) {
      addContextToVector(writer, currentContext, wordsInContext,
                         entitiesInContext
      );
      currentContext = line._contextId;
      wordsInContext.

          clear();

      entitiesInContext.

          clear();

    }
    if (line._isEntity) {
      Id eid;
      if (_vocab.
          getId(line
                    ._word, &eid)) {
        entitiesInContext[eid] += line.
            _score;
      } else {
        if (entityNotFoundErrorMsgCount < 20) {
          LOG(WARN)

            << "Entity from text not in KB: " << line._word << '\n';
          if (++entityNotFoundErrorMsgCount == 20) {
            LOG(WARN)

              << "There are more entities not in the KB..."
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

      wordsInContext[wid] += line.
          _score;
    }
    ++
        i;
    if (i % 10000000 == 0) {
      LOG(INFO)

        << "Lines processed: " << i << '\n';
    }
  }
  addContextToVector(writer, currentContext, wordsInContext, entitiesInContext
  );
  writer.

      finish();
  LOG(INFO)

    << "Pass done.\n";
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
  Id *contextList = new Id[meta._nofElements];
  Id *wordList = new Id[meta._nofElements];
  Score *scoreList = new Score[meta._nofElements];

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
size_t Index::writeList(Numeric *data, size_t nofElements,
                        ad_utility::File& file) const {
  if (nofElements > 0) {
    uint64_t *encoded = new uint64_t[nofElements];
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
                                   Index::WidthTwoList *result) const {
  LOG(DEBUG) << "In getContextListForWords...\n";
  auto terms = ad_utility::split(words, ' ');
  AD_CHECK(terms.size() > 0);

  vector<Id> cids;
  vector<Score> scores;
  if (terms.size() > 1) {
    vector<vector<Id>> cidVecs;
    vector<vector<Score>> scoreVecs;
    for (auto& term: terms) {
      cidVecs.push_back(vector<Id>());
      scoreVecs.push_back(vector<Score>());
      getWordPostingsForTerm(term, cidVecs.back(), scoreVecs.back());
    }
    if (cidVecs.size() == 2) {
      FTSAlgorithms::intersectTwoPostingLists(cidVecs[0], scoreVecs[1],
                                              cidVecs[1],
                                              scoreVecs[1], cids, scores);
    } else {
      vector<Id> dummy;
      FTSAlgorithms::intersectKWay(cidVecs, scoreVecs, nullptr, cids, dummy,
                                   scores);
    }
  } else {
    getWordPostingsForTerm(terms[0], cids, scores);
  }


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
    if (!_textVocab.getIdRangeForFullTextPrefix(term, &idRange)) {
      LOG(INFO) << "Prefix: " << term << " not in vocabulary\n";
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
  if (tbmd._cl.hasMultipleWords() || (tbmd._firstWordId == idRange._first &&
                                      tbmd._lastWordId == idRange._last)) {
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
    FTSAlgorithms::filterByRange(idRange, blockCids, blockWids, blockScores,
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
void Index::getContextEntityScoreListsForWords(const string& words,
                                               vector<Id>& cids,
                                               vector<Id>& eids,
                                               vector<Score>& scores) const {
  LOG(DEBUG) << "In getEntityContextScoreListsForWords...\n";
  auto terms = ad_utility::split(words, ' ');
  AD_CHECK(terms.size() > 0);
  if (terms.size() > 1) {
    // Find the term with the smallest block and/or one where no filtering
    // via wordlists is necessary. Onyl take entity postings form this one.
    // This is valid because the set of co-occuring entities depends on
    // the context and not on the word/block used as entry point.
    // Take all other words and get word posting lists for them.
    // Intersect all and keep the entity word ids.
    size_t useElFromTerm = getIndexOfBestSuitedElTerm(terms);

    if (terms.size() == 2) {
      // Special case of two terms: no k-way intersect needed.
      vector<Id> wCids;
      vector<Score> wScores;
      vector<Id> eCids;
      vector<Id> eWids;
      vector<Score> eScores;
      size_t onlyWordsFrom = 1 - useElFromTerm;
      getWordPostingsForTerm(terms[onlyWordsFrom], wCids, wScores);
      getEntityPostingsForTerm(terms[useElFromTerm], eCids, eWids, eScores);
      FTSAlgorithms::intersect(wCids, wScores, eCids, eWids, eScores, cids,
                               eids, scores);
    } else {
      // Generic case: Use a k-way intersect whereas the entity postings
      // play a special role.
      vector<vector<Id>> cidVecs;
      vector<vector<Score>> scoreVecs;
      for (size_t i = 0; i < terms.size(); ++i) {
        if (i != useElFromTerm) {
          cidVecs.push_back(vector<Id>());
          scoreVecs.push_back(vector<Score>());
          getWordPostingsForTerm(terms[i], cidVecs.back(), scoreVecs.back());
        }
      }
      cidVecs.push_back(vector<Id>());
      scoreVecs.push_back(vector<Score>());
      vector<Id> eWids;
      getEntityPostingsForTerm(terms[useElFromTerm], cidVecs.back(), eWids,
                               scoreVecs.back());
      FTSAlgorithms::intersectKWay(cidVecs, scoreVecs, &eWids, cids, eids,
                                   scores);
    }
  } else {
    // Special case: Just one word to deal with.
    getEntityPostingsForTerm(terms[0], cids, eids, scores);
  }
  LOG(DEBUG) << "Done with getEntityContextScoreListsForWords. "
             << "Got " << cids.size() << " elements. \n";
}

// _____________________________________________________________________________
void Index::getECListForWords(const string& words,
                              size_t limit,
                              Index::WidthThreeList *result) const {
  LOG(DEBUG) << "In getECListForWords...\n";
  vector<Id> cids;
  vector<Id> eids;
  vector<Score> scores;
  getContextEntityScoreListsForWords(words, cids, eids, scores);
  FTSAlgorithms::aggScoresAndTakeTopKContexts(
      cids, eids, scores, limit, result);
  LOG(DEBUG) << "Done with getECListForWords. Result size: " << result->size()
             << "\n";
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
    FTSAlgorithms::intersect(matchingContexts, matchingContextScores,
                             eBlockCids, eBlockWids,
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
  uint64_t *encoded = new uint64_t[nofBytes / 8];
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
  uint64_t *encoded = new uint64_t[nofElements];
  result.resize(nofElements + 250);
  off_t current = from;
  size_t ret = _textIndexFile.read(&nofCodebookBytes, sizeof(off_t), current);
  LOG(TRACE) << "Nof Codebook Bytes: " << nofCodebookBytes << '\n';
  AD_CHECK_EQ(sizeof(off_t), ret);
  current += ret;
  T *codebook = new T[nofCodebookBytes / sizeof(T)];
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
size_t Index::getIndexOfBestSuitedElTerm(const vector<string>& terms) const {
  // It is beneficial to choose a term where no filtering by regular word id
  // is needed. Then the entity lists can be read directly from disk.
  // For others it is always necessary to reach wordlist and filter them
  // if such an entity list is taken, another interesection is necessary.

  // Apart from that, entity lists are usually larger by a factor.
  // Hence it makes sense to choose the smallest.

  // Heuristic: Always prefer no-filtering terms over otheres, then
  // pick the one with the smallest EL block to be read.
  std::vector<std::tuple<size_t, bool, size_t>> toBeSorted;
  for (size_t i = 0; i < terms.size(); ++i) {
    IdRange range;
    if (terms[i].back() == PREFIX_CHAR) {
      _textVocab.getIdRangeForFullTextPrefix(terms[i], &range);
    } else {
      _textVocab.getId(terms[i], &range._first);
      range._last = range._first;
    }
    auto tbmd = _textMeta.getBlockInfoByWordRange(range._first, range._last);
    toBeSorted.emplace_back(
        std::make_tuple(i, tbmd._firstWordId == tbmd._lastWordId,
                        tbmd._entityCl._nofElements));
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
template<size_t I>
void Index::getECListForWordsAndSingleSub(const string& words,
                                          const vector<array<Id, I>> subres,
                                          size_t subResMainCol,
                                          size_t limit,
                                          vector<array<Id, 3 + I>>& res) const {
  // Get context entity postings matching the words
  vector<Id> cids;
  vector<Id> eids;
  vector<Score> scores;
  getContextEntityScoreListsForWords(words, cids, eids, scores);

  // TODO: more code for efficienty.
  // Examine the possibility to branch if subresult is much larger
  // than the number of matching postings.
  // Could binary search then instead of create the map first.

  LOG(DEBUG) << "Filtering matching contexts and building cross-product...\n";
  vector<array<Id, 3 + I>> nonAggRes;
  if (cids.size() > 0) {
    // Transform the sub res into a map from key entity to tuples
    std::unordered_map<Id, vector<array<Id, I>>> subEs;
    for (size_t i = 0; i < subres.size(); ++i) {
      auto& tuples = subEs[subres[i][subResMainCol]];
      tuples.push_back(subres[i]);
    }
    // Test if each context is fitting.
    size_t currentContextFrom = 0;
    Id currentContext = cids[0];
    bool matched = false;
    for (size_t i = 0; i < cids.size(); ++i) {
      if (cids[i] != currentContext) {
        if (matched) {
          FTSAlgorithms::appendCrossProduct(
              cids, eids, scores, currentContextFrom, i, subEs, nonAggRes);
        }
        matched = false;
        currentContext = cids[i];
        currentContextFrom = i;
      }
      if (!matched) {
        matched = (subEs.count(eids[i]) > 0);
      }
    }
  }
  FTSAlgorithms::aggScoresAndTakeTopKContexts(nonAggRes, limit, res);
}

template
void Index::getECListForWordsAndSingleSub(const string& words,
                                          const vector<array<Id, 1>> subres,
                                          size_t subResMainCol,
                                          size_t limit,
                                          vector<array<Id, 4>>& res) const;

template
void Index::getECListForWordsAndSingleSub(const string& words,
                                          const vector<array<Id, 2>> subres,
                                          size_t subResMainCol,
                                          size_t limit,
                                          vector<array<Id, 5>>& res) const;

// _____________________________________________________________________________
void Index::getECListForWordsAndTwoW1Subs(const string& words,
                                          const vector<array<Id, 1>> subres1,
                                          const vector<array<Id, 1>> subres2,
                                          size_t limit,
                                          vector<array<Id, 5>>& res) const {
  // Get context entity postings matching the words
  vector<Id> cids;
  vector<Id> eids;
  vector<Score> scores;
  getContextEntityScoreListsForWords(words, cids, eids, scores);

  // TODO: more code for efficienty.
  // Examine the possiblity to branch if subresults are
  // much larger than the number of matching postings.
  // Could binary search in them, then instead of create sets first.

  LOG(DEBUG) << "Filtering matching contexts and building cross-product...\n";
  vector<array<Id, 5>> nonAggRes;
  if (cids.size() > 0) {
    // Transform the sub res' into sets of entity Ids
    std::unordered_set<Id> subEs1;
    std::unordered_set<Id> subEs2;
    for (size_t i = 0; i < subres1.size(); ++i) {
      subEs1.insert(subres1[i][0]);
    }
    for (size_t i = 0; i < subres2.size(); ++i) {
      subEs2.insert(subres2[i][0]);
    }
    // Test if each context is fitting.
    size_t currentContextFrom = 0;
    Id currentContext = cids[0];
    bool matched = false;
    bool matched1 = false;
    bool matched2 = false;
    for (size_t i = 0; i < cids.size(); ++i) {
      if (cids[i] != currentContext) {
        if (matched) {
          FTSAlgorithms::appendCrossProduct(
              cids, eids, scores, currentContextFrom, i, subEs1, subEs2,
              nonAggRes);
        }
        matched = false;
        matched1 = false;
        matched2 = false;
        currentContext = cids[i];
        currentContextFrom = i;
      }
      if (!matched) {
        if (!matched1) {
          matched1 = (subEs1.count(eids[i]) > 0);
        }
        if (!matched2) {
          matched2 = (subEs2.count(eids[i]) > 0);
        }
        matched = matched1 && matched2;
      }
    }
  }
  FTSAlgorithms::aggScoresAndTakeTopKContexts(nonAggRes, limit, res);
}

// _____________________________________________________________________________
void Index::getECListForWordsAndSubtrees(
    const string& words,
    const vector<unordered_map<Id, vector<vector<Id>>>>& subResMaps,
    size_t limit,
    vector<vector<Id>>& res) const {

  // Get context entity postings matching the words
  vector<Id> cids;
  vector<Id> eids;
  vector<Score> scores;
  getContextEntityScoreListsForWords(words, cids, eids, scores);

  LOG(DEBUG) << "Filtering matching contexts and building cross-product...\n";
  vector<vector<Id>> nonAggRes;
  if (cids.size() > 0) {
    // Test if each context is fitting.
    size_t currentContextFrom = 0;
    Id currentContext = cids[0];
    bool matched = false;
    vector<bool> matchedSubs;
    matchedSubs.resize(subResMaps.size(), false);
    for (size_t i = 0; i < cids.size(); ++i) {
      if (cids[i] != currentContext) {
        if (matched) {
          FTSAlgorithms::appendCrossProduct(
              cids, eids, scores, currentContextFrom, i, subResMaps, nonAggRes);
        }
        matched = false;
        std::fill(matchedSubs.begin(), matchedSubs.end(), false);
        currentContext = cids[i];
        currentContextFrom = i;
      }
      if (!matched) {
        matched = true;
        for (size_t j = 0; j < matchedSubs.size(); ++j) {
          if (!matchedSubs[j]) {
            if (subResMaps[j].count(eids[i]) > 0) {
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
