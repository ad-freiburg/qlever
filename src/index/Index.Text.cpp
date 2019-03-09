// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <stxxl/algorithm>
#include <tuple>
#include <utility>
#include "../engine/CallFixedSize.h"
#include "../parser/ContextFileParser.h"
#include "../util/Simple8bCode.h"
#include "./FTSAlgorithms.h"
#include "./Index.h"

// _____________________________________________________________________________
void Index::addTextFromContextFile(const string& contextFile) {
  string indexFilename = _onDiskBase + ".text.index";
  size_t nofLines = passContextFileForVocabulary(contextFile);
  _textVocab.writeToFile(_onDiskBase + ".text.vocabulary");
  calculateBlockBoundaries();
  TextVec v(nofLines);
  passContextFileIntoVector(contextFile, v);
  LOG(INFO) << "Sorting text index with " << v.size() << " items ..."
            << std::endl;
  stxxl::sort(begin(v), end(v), SortText(), STXXL_MEMORY_TO_USE);
  LOG(INFO) << "Sort done." << std::endl;
  createTextIndex(indexFilename, v);
  openTextFileHandle();
}

// _____________________________________________________________________________
void Index::buildDocsDB(const string& docsFileName) {
  LOG(INFO) << "Building DocsDB...\n";
  ad_utility::File docsFile(docsFileName.c_str(), "r");
  std::ofstream ofs(_onDiskBase + ".text.docsDB", std::ios_base::out);
  // To avoid excessive use of RAM,
  // we write the offsets to and stxxl:vector first;
  typedef stxxl::vector<off_t> OffVec;
  OffVec offsets;
  off_t currentOffset = 0;
  Id currentContextId = 0;
  char* buf = new char[BUFFER_SIZE_DOCSFILE_LINE];
  string line;
  while (docsFile.readLine(&line, buf, BUFFER_SIZE_DOCSFILE_LINE)) {
    size_t tab = line.find('\t');
    Id contextId = static_cast<Id>(atol(line.substr(0, tab).c_str()));
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
  ad_utility::File out(string(_onDiskBase + ".text.docsDB").c_str(), "a");
  for (size_t i = 0; i < offsets.size(); ++i) {
    off_t cur = offsets[i];
    out.write(&cur, sizeof(cur));
  }
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
    _docsDB.init(string(_onDiskBase + ".text.docsDB"));
    LOG(INFO) << "Done reading excerpt offsets." << endl;
  } else {
    LOG(INFO) << "No Docs DB found.\n";
    f.close();
  }
  LOG(INFO) << "Registered text index: " << _textMeta.statistics() << std::endl;
}

// _____________________________________________________________________________
size_t Index::passContextFileForVocabulary(string const& contextFile) {
  LOG(INFO) << "Making pass over ContextFile " << contextFile
            << " for vocabulary." << std::endl;
  ContextFileParser::Line line;
  ContextFileParser p(contextFile);
  ad_utility::HashSet<string> items;
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
void Index::passContextFileIntoVector(const string& contextFile,
                                      Index::TextVec& vec) {
  LOG(INFO) << "Making pass over ContextFile " << contextFile
            << " and creating stxxl vector.\n";
  ContextFileParser::Line line;
  ContextFileParser p(contextFile);
  size_t i = 0;
  // write using vector_bufwriter

  // we have deleted the vocabulary during the index creation to save ram, so
  // now we have to reload it,
  LOG(INFO) << "Loading vocabulary from disk (needed for correct Ids in text "
               "index)\n";
  // this has to be repeated completely here because we have the possibility to
  // only add a text index. In that case the Vocabulary has never been
  // initialized before
  _vocab = Vocabulary<CompressedString>();
  readConfiguration();
  _vocab.readFromFile(_onDiskBase + ".vocabulary",
                      _onDiskLiterals ? _onDiskBase + ".literals-index" : "");

  TextVec::bufwriter_type writer(vec);
  ad_utility::HashMap<Id, Score> wordsInContext;
  ad_utility::HashMap<Id, Score> entitiesInContext;
  Id currentContext = 0;
  size_t nofContexts = 0;
  size_t nofWordPostings = 0;
  size_t nofEntityPostings = 0;

  size_t entityNotFoundErrorMsgCount = 0;
  while (p.getLine(line)) {
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
      Id eid;
      if (_vocab.getId(line._word, &eid)) {
        entitiesInContext[eid] += line._score;
      } else {
        if (entityNotFoundErrorMsgCount < 20) {
          LOG(WARN) << "Entity from text not in KB: " << line._word << '\n';
          if (++entityNotFoundErrorMsgCount == 20) {
            LOG(WARN) << "There are more entities not in the KB..."
                      << " suppressing further warnings...\n";
          }
        }
      }
    } else {
      ++nofWordPostings;
      Id wid;
#ifndef NDEBUG
      bool ret = _textVocab.getId(line._word, &wid);
      if (!ret) {
        LOG(INFO) << "ERROR: word " << line._word
                  << "not found in textVocab. Terminating\n";
      }
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
  ++nofContexts;
  addContextToVector(writer, currentContext, wordsInContext, entitiesInContext);
  _textMeta.setNofTextRecords(nofContexts);
  _textMeta.setNofWordPostings(nofWordPostings);
  _textMeta.setNofEntityPostings(nofEntityPostings);

  writer.finish();
  LOG(INFO) << "Pass done.\n";
}

// _____________________________________________________________________________
void Index::addContextToVector(Index::TextVec::bufwriter_type& writer,
                               Id context,
                               const ad_utility::HashMap<Id, Score>& words,
                               const ad_utility::HashMap<Id, Score>& entities) {
  // Determine blocks for each word and each entity.
  // Add the posting to each block.
  ad_utility::HashSet<Id> touchedBlocks;
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
      // Don't add an entity to its own block..
      // FIX JUN 07 2017: DO add it. It's needed so that it is returned
      // as a result itself.
      // if (blockId == getEntityBlockId(it->first)) { continue; }
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
  size_t nofEntities = 0;
  size_t nofEntityContexts = 0;
  for (TextVec::bufreader_type reader(vec); !reader.empty(); ++reader) {
    if (std::get<0>(*reader) != currentBlockId) {
      AD_CHECK(classicPostings.size() > 0);
      if (isEntityBlockId(currentBlockId)) {
        ++nofEntities;
        nofEntityContexts += classicPostings.size();
      }
      ContextListMetaData classic = writePostings(out, classicPostings, true);
      ContextListMetaData entity = writePostings(out, entityPostings, false);
      _textMeta.addBlock(TextBlockMetaData(currentMinWordId, currentMaxWordId,
                                           classic, entity));
      classicPostings.clear();
      entityPostings.clear();
      currentBlockId = std::get<0>(*reader);
      currentMinWordId = std::get<2>(*reader);
      currentMaxWordId = std::get<2>(*reader);
    }
    if (!std::get<4>(*reader)) {
      classicPostings.emplace_back(std::make_tuple(
          std::get<1>(*reader), std::get<2>(*reader), std::get<3>(*reader)));
      if (std::get<2>(*reader) < currentMinWordId) {
        currentMinWordId = std::get<2>(*reader);
      }
      if (std::get<2>(*reader) > currentMaxWordId) {
        currentMaxWordId = std::get<2>(*reader);
      }

    } else {
      entityPostings.emplace_back(std::make_tuple(
          std::get<1>(*reader), std::get<2>(*reader), std::get<3>(*reader)));
    }
  }
  // Write the last block
  AD_CHECK(classicPostings.size() > 0);
  if (isEntityBlockId(currentBlockId)) {
    ++nofEntities;
    nofEntityContexts += classicPostings.size();
  }
  ContextListMetaData classic = writePostings(out, classicPostings, true);
  ContextListMetaData entity = writePostings(out, entityPostings, false);
  _textMeta.addBlock(
      TextBlockMetaData(currentMinWordId, currentMaxWordId, classic, entity));
  _textMeta.setNofEntities(nofEntities);
  _textMeta.setNofEntityContexts(nofEntityContexts);
  classicPostings.clear();
  entityPostings.clear();
  LOG(INFO) << "Done creating text index." << std::endl;
  LOG(INFO) << "Writing statistics:\n" << _textMeta.statistics() << std::endl;

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
    // we need foo.value().get() because the vocab returns
    // a std::optional<std::reference_wrapper<string>> and the "." currently
    // doesn't implicitly convert to a true reference (unlike function calls)
    if (_textVocab[i].value().get().size() < MIN_WORD_PREFIX_SIZE ||
        (_textVocab[i + 1].value().get().size() < MIN_WORD_PREFIX_SIZE) ||
        _textVocab[i].value().get().substr(0, MIN_WORD_PREFIX_SIZE) !=
            _textVocab[i + 1].value().get().substr(0, MIN_WORD_PREFIX_SIZE)) {
      _blockBoundaries.push_back(i);
    }
  }
  _blockBoundaries.push_back(_textVocab.size() - 1);
  LOG(INFO) << "Done. Got " << _blockBoundaries.size()
            << " for a vocabulary with " << _textVocab.size() << " words.\n";
}

// _____________________________________________________________________________
Id Index::getWordBlockId(Id wordId) const {
  return static_cast<Id>(std::lower_bound(_blockBoundaries.begin(),
                                          _blockBoundaries.end(), wordId) -
                         _blockBoundaries.begin());
}

// _____________________________________________________________________________
Id Index::getEntityBlockId(Id entityId) const {
  return entityId + _blockBoundaries.size();
}

// _____________________________________________________________________________
bool Index::isEntityBlockId(Id blockId) const {
  return blockId >= _blockBoundaries.size();
}

// _____________________________________________________________________________
template <typename Numeric>
size_t Index::writeList(Numeric* data, size_t nofElements,
                        ad_utility::File& file) const {
  if (nofElements > 0) {
    uint64_t* encoded = new uint64_t[nofElements];
    size_t size = ad_utility::Simple8bCode::encode(data, nofElements, encoded);
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
  ad_utility::HashMap<Id, size_t> wfMap;
  ad_utility::HashMap<Score, size_t> sfMap;
  for (const auto& p : postings) {
    wfMap[std::get<1>(p)] = 0;
    sfMap[std::get<2>(p)] = 0;
  }
  for (const auto& p : postings) {
    ++wfMap[std::get<1>(p)];
    ++sfMap[std::get<2>(p)];
  }
  vector<std::pair<Id, size_t>> wfVec;
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
            [](const std::pair<Id, size_t>& a, const std::pair<Id, size_t>& b) {
              return a.second > b.second;
            });
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
size_t Index::writeCodebook(const vector<T>& codebook,
                            ad_utility::File& file) const {
  size_t byteSizeOfCodebook = sizeof(T) * codebook.size();
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
  return _textVocab[id].value();
}

// _____________________________________________________________________________
void Index::getContextListForWords(const string& words,
                                   IdTable* dynResult) const {
  LOG(DEBUG) << "In getContextListForWords...\n";
  auto terms = ad_utility::split(words, ' ');
  AD_CHECK(terms.size() > 0);

  vector<Id> cids;
  vector<Score> scores;
  if (terms.size() > 1) {
    vector<vector<Id>> cidVecs;
    vector<vector<Score>> scoreVecs;
    for (auto& term : terms) {
      cidVecs.push_back(vector<Id>());
      scoreVecs.push_back(vector<Score>());
      getWordPostingsForTerm(term, cidVecs.back(), scoreVecs.back());
    }
    if (cidVecs.size() == 2) {
      FTSAlgorithms::intersectTwoPostingLists(
          cidVecs[0], scoreVecs[1], cidVecs[1], scoreVecs[1], cids, scores);
    } else {
      vector<Id> dummy;
      FTSAlgorithms::intersectKWay(cidVecs, scoreVecs, nullptr, cids, dummy,
                                   scores);
    }
  } else {
    getWordPostingsForTerm(terms[0], cids, scores);
  }

  LOG(DEBUG) << "Packing lists into a ResultTable\n...";
  IdTableStatic<2> result = dynResult->moveToStatic<2>();
  result.reserve(cids.size() + 2);
  result.resize(cids.size());
  for (size_t i = 0; i < cids.size(); ++i) {
    result(i, 0) = cids[i];
    result(i, 1) = scores[i];
  }
  *dynResult = result.moveToDynamic();
  LOG(DEBUG) << "Done with getContextListForWords.\n";
}

// _____________________________________________________________________________
void Index::getWordPostingsForTerm(const string& term, vector<Id>& cids,
                                   vector<Score>& scores) const {
  assert(term.size() > 0);
  LOG(DEBUG) << "Getting word postings for term: " << term << '\n';
  IdRange idRange;
  bool entityTerm = (term[0] == '<' && term.back() == '>');
  if (term[term.size() - 1] == PREFIX_CHAR) {
    if (!_textVocab.getIdRangeForFullTextPrefix(term, &idRange)) {
      LOG(INFO) << "Prefix: " << term << " not in vocabulary\n";
      return;
    }
  } else {
    if (entityTerm) {
      if (!_vocab.getId(term, &idRange._first)) {
        LOG(INFO) << "Term: " << term << " not in entity vocabulary\n";
        return;
      }
    } else if (!_textVocab.getId(term, &idRange._first)) {
      LOG(INFO) << "Term: " << term << " not in vocabulary\n";
      return;
    }
    idRange._last = idRange._first;
  }
  if (entityTerm && !_textMeta.existsTextBlockForEntityId(idRange._first)) {
    LOG(INFO) << "Entity " << term << " not contained in the text.\n";
    return;
  }
  const auto& tbmd =
      entityTerm
          ? _textMeta.getBlockInfoByEntityId(idRange._first)
          : _textMeta.getBlockInfoByWordRange(idRange._first, idRange._last);
  if (tbmd._cl.hasMultipleWords() && !(tbmd._firstWordId == idRange._first &&
                                       tbmd._lastWordId == idRange._last)) {
    vector<Id> blockCids;
    vector<Id> blockWids;
    vector<Score> blockScores;
    readGapComprList(tbmd._cl._nofElements, tbmd._cl._startContextlist,
                     static_cast<size_t>(tbmd._cl._startWordlist -
                                         tbmd._cl._startContextlist),
                     blockCids);
    readFreqComprList(
        tbmd._cl._nofElements, tbmd._cl._startWordlist,
        static_cast<size_t>(tbmd._cl._startScorelist - tbmd._cl._startWordlist),
        blockWids);
    readFreqComprList(
        tbmd._cl._nofElements, tbmd._cl._startScorelist,
        static_cast<size_t>(tbmd._cl._lastByte + 1 - tbmd._cl._startScorelist),
        blockScores);
    FTSAlgorithms::filterByRange(idRange, blockCids, blockWids, blockScores,
                                 cids, scores);
  } else {
    readGapComprList(tbmd._cl._nofElements, tbmd._cl._startContextlist,
                     static_cast<size_t>(tbmd._cl._startWordlist -
                                         tbmd._cl._startContextlist),
                     cids);
    readFreqComprList(
        tbmd._cl._nofElements, tbmd._cl._startScorelist,
        static_cast<size_t>(tbmd._cl._lastByte + 1 - tbmd._cl._startScorelist),
        scores);
  }
  LOG(DEBUG) << "Word postings for term: " << term << ": cids: " << cids.size()
             << " scores " << scores.size() << '\n';
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
    // via wordlists is necessary. Only take entity postings form this one.
    // This is valid because the set of co-occuring entities depends on
    // the context and not on the word/block used as entry point.
    // Take all other words and get word posting lists for them.
    // Intersect all and keep the entity word ids.
    size_t useElFromTerm = getIndexOfBestSuitedElTerm(terms);
    LOG(TRACE) << "Best term to take entity list from: " << terms[useElFromTerm]
               << std::endl;

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
      FTSAlgorithms::intersect(wCids, eCids, eWids, eScores, cids, eids,
                               scores);
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
void Index::getECListForWordsOneVar(const string& words, size_t limit,
                                    IdTable* result) const {
  LOG(DEBUG) << "In getECListForWords...\n";
  vector<Id> cids;
  vector<Id> eids;
  vector<Score> scores;
  getContextEntityScoreListsForWords(words, cids, eids, scores);
  FTSAlgorithms::aggScoresAndTakeTopKContexts(cids, eids, scores, limit,
                                              result);
  LOG(DEBUG) << "Done with getECListForWords. Result size: " << result->size()
             << "\n";
}

// _____________________________________________________________________________
void Index::getECListForWords(const string& words, size_t nofVars, size_t limit,
                              IdTable* result) const {
  LOG(DEBUG) << "In getECListForWords...\n";
  vector<Id> cids;
  vector<Id> eids;
  vector<Score> scores;
  getContextEntityScoreListsForWords(words, cids, eids, scores);
  int width = result->cols();
  CALL_FIXED_SIZE_1(width, FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts,
                    cids, eids, scores, nofVars, limit, result);
  LOG(DEBUG) << "Done with getECListForWords. Result size: " << result->size()
             << "\n";
}

// _____________________________________________________________________________
void Index::getFilteredECListForWords(const string& words,
                                      const IdTable& filter,
                                      size_t filterColumn, size_t nofVars,
                                      size_t limit, IdTable* result) const {
  LOG(DEBUG) << "In getFilteredECListForWords...\n";
  if (filter.size() > 0) {
    // Build a map filterEid->set<Rows>
    using FilterMap = ad_utility::HashMap<Id, IdTable>;
    LOG(DEBUG) << "Constructing map...\n";
    FilterMap fMap;
    for (size_t i = 0; i < filter.size(); ++i) {
      Id eid = filter(i, filterColumn);
      auto it = fMap.find(eid);
      if (it == fMap.end()) {
        fMap[eid] = IdTable(filter.cols());
      } else {
        it->second.push_back(filter, i);
      }
    }
    vector<Id> cids;
    vector<Id> eids;
    vector<Score> scores;
    getContextEntityScoreListsForWords(words, cids, eids, scores);
    int width = result->cols();
    if (nofVars == 1) {
      CALL_FIXED_SIZE_1(width,
                        FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts,
                        cids, eids, scores, fMap, limit, result);
    } else {
      CALL_FIXED_SIZE_1(
          width, FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts,
          cids, eids, scores, fMap, nofVars, limit, result);
    }
  }
  LOG(DEBUG) << "Done with getFilteredECListForWords. Result size: "
             << result->size() << "\n";
}

// _____________________________________________________________________________
void Index::getFilteredECListForWordsWidthOne(const string& words,
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
  vector<Id> cids;
  vector<Id> eids;
  vector<Score> scores;
  getContextEntityScoreListsForWords(words, cids, eids, scores);
  int width = result->cols();
  if (nofVars == 1) {
    FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts(
        cids, eids, scores, fSet, limit, result);
  } else {
    CALL_FIXED_SIZE_1(width,
                      FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts,
                      cids, eids, scores, fSet, nofVars, limit, result);
  }
  LOG(DEBUG) << "Done with getFilteredECListForWords. Result size: "
             << result->size() << "\n";
}

// _____________________________________________________________________________
void Index::getEntityPostingsForTerm(const string& term, vector<Id>& cids,
                                     vector<Id>& eids,
                                     vector<Score>& scores) const {
  LOG(DEBUG) << "Getting entity postings for term: " << term << '\n';
  IdRange idRange;
  bool entityTerm = (term[0] == '<' && term.back() == '>');
  if (term.back() == PREFIX_CHAR) {
    if (!_textVocab.getIdRangeForFullTextPrefix(term, &idRange)) {
      LOG(INFO) << "Prefix: " << term << " not in vocabulary\n";
      return;
    }
  } else {
    if (entityTerm) {
      if (!_vocab.getId(term, &idRange._first)) {
        LOG(DEBUG) << "Term: " << term << " not in entity vocabulary\n";
        return;
      }
    } else if (!_textVocab.getId(term, &idRange._first)) {
      LOG(DEBUG) << "Term: " << term << " not in vocabulary\n";
      return;
    }
    idRange._last = idRange._first;
  }
  const auto& tbmd =
      entityTerm
          ? _textMeta.getBlockInfoByEntityId(idRange._first)
          : _textMeta.getBlockInfoByWordRange(idRange._first, idRange._last);

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
    FTSAlgorithms::intersect(matchingContexts, eBlockCids, eBlockWids,
                             eBlockScores, cids, eids, scores);
  }
}

// _____________________________________________________________________________
template <typename T>
void Index::readGapComprList(size_t nofElements, off_t from, size_t nofBytes,
                             vector<T>& result) const {
  LOG(DEBUG) << "Reading gap-encoded list from disk...\n";
  LOG(TRACE) << "NofElements: " << nofElements << ", from: " << from
             << ", nofBytes: " << nofBytes << '\n';
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
  LOG(DEBUG) << "Done reading gap-encoded list. Size: " << result.size()
             << "\n";
}

// _____________________________________________________________________________
template <typename T>
void Index::readFreqComprList(size_t nofElements, off_t from, size_t nofBytes,
                              vector<T>& result) const {
  AD_CHECK_GT(nofBytes, 0);
  LOG(DEBUG) << "Reading frequency-encoded list from disk...\n";
  LOG(TRACE) << "NofElements: " << nofElements << ", from: " << from
             << ", nofBytes: " << nofBytes << '\n';
  size_t nofCodebookBytes;
  uint64_t* encoded = new uint64_t[nofElements];
  result.resize(nofElements + 250);
  off_t current = from;
  size_t ret = _textIndexFile.read(&nofCodebookBytes, sizeof(off_t), current);
  LOG(TRACE) << "Nof Codebook Bytes: " << nofCodebookBytes << '\n';
  AD_CHECK_EQ(sizeof(off_t), ret);
  current += ret;
  T* codebook = new T[nofCodebookBytes / sizeof(T)];
  ret = _textIndexFile.read(codebook, nofCodebookBytes, current);
  current += ret;
  AD_CHECK_EQ(ret, size_t(nofCodebookBytes));
  ret = _textIndexFile.read(
      encoded, static_cast<size_t>(nofBytes - (current - from)), current);
  current += ret;
  AD_CHECK_EQ(size_t(current - from), nofBytes);
  LOG(DEBUG) << "Decoding Simple8b code...\n";
  ad_utility::Simple8bCode::decode(encoded, nofElements, result.data());
  LOG(DEBUG) << "Reverting frequency encoded items to actual IDs...\n";
  result.resize(nofElements);
  for (size_t i = 0; i < result.size(); ++i) {
    result[i] = codebook[result[i]];
  }
  delete[] encoded;
  delete[] codebook;
  LOG(DEBUG) << "Done reading frequency-encoded list. Size: " << result.size()
             << "\n";
}

// _____________________________________________________________________________
void Index::dumpAsciiLists(const vector<string>& lists,
                           bool decGapsFreq) const {
  if (lists.size() == 0) {
    size_t nofBlocks = _textMeta.getBlockCount();
    for (size_t i = 0; i < nofBlocks; ++i) {
      TextBlockMetaData tbmd = _textMeta.getBlockById(i);
      LOG(INFO) << "At block: " << i << std::endl;
      auto nofWordElems = tbmd._cl._nofElements;
      if (nofWordElems < 1000000) continue;
      if (tbmd._firstWordId > _textVocab.size()) return;
      if (decGapsFreq) {
        AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "not yet impl.");
      } else {
        dumpAsciiLists(tbmd);
      }
    }
  } else {
    for (size_t i = 0; i < lists.size(); ++i) {
      IdRange idRange;
      _textVocab.getIdRangeForFullTextPrefix(lists[i], &idRange);
      TextBlockMetaData tbmd =
          _textMeta.getBlockInfoByWordRange(idRange._first, idRange._last);
      if (decGapsFreq) {
        vector<Id> eids;
        vector<Id> cids;
        vector<Score> scores;
        getEntityPostingsForTerm(lists[i], cids, eids, scores);
        auto firstWord = wordIdToString(tbmd._firstWordId);
        auto lastWord = wordIdToString(tbmd._lastWordId);
        string basename = _onDiskBase + ".list." + firstWord + "-" + lastWord;
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

//_ ____________________________________________________________________________
void Index::dumpAsciiLists(const TextBlockMetaData& tbmd) const {
  auto firstWord = wordIdToString(tbmd._firstWordId);
  auto lastWord = wordIdToString(tbmd._lastWordId);
  LOG(INFO) << "This block is from " << firstWord << " to " << lastWord
            << std::endl;
  string basename = _onDiskBase + ".list." + firstWord + "-" + lastWord;
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
    _textIndexFile.read(encodedD, nofBytes, from);
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
          _textIndexFile.read(&nofCodebookBytes, sizeof(off_t), current);
      LOG(DEBUG) << "Nof Codebook Bytes: " << nofCodebookBytes << '\n';
      AD_CHECK_EQ(sizeof(off_t), ret);
      current += ret;
      Id* codebookW = new Id[nofCodebookBytes / sizeof(Id)];
      ret = _textIndexFile.read(codebookW, nofCodebookBytes, current);
      current += ret;
      AD_CHECK_EQ(ret, size_t(nofCodebookBytes));
      ret = _textIndexFile.read(
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
    size_t ret = _textIndexFile.read(&nofCodebookBytes, sizeof(off_t), current);
    LOG(DEBUG) << "Nof Codebook Bytes: " << nofCodebookBytes << '\n';
    AD_CHECK_EQ(sizeof(off_t), ret);
    current += ret;
    Score* codebookS = new Score[nofCodebookBytes / sizeof(Score)];
    ret = _textIndexFile.read(codebookS, nofCodebookBytes, current);
    current += ret;
    AD_CHECK_EQ(ret, size_t(nofCodebookBytes));
    ret = _textIndexFile.read(
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
    _textIndexFile.read(encodedD, nofBytes, from);
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
          _textIndexFile.read(&nofCodebookBytes, sizeof(off_t), current);
      LOG(DEBUG) << "Nof Codebook Bytes: " << nofCodebookBytes << '\n';
      AD_CHECK_EQ(sizeof(off_t), ret);
      current += ret;
      Id* codebookW = new Id[nofCodebookBytes / sizeof(Id)];
      ret = _textIndexFile.read(codebookW, nofCodebookBytes, current);
      current += ret;
      AD_CHECK_EQ(ret, size_t(nofCodebookBytes));
      ret = _textIndexFile.read(
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
    size_t ret = _textIndexFile.read(&nofCodebookBytes, sizeof(off_t), current);

    LOG(DEBUG) << "Nof Codebook Bytes: " << nofCodebookBytes << '\n';
    AD_CHECK_EQ(sizeof(off_t), ret);
    current += ret;
    Score* codebookS = new Score[nofCodebookBytes / sizeof(Score)];
    ret = _textIndexFile.read(codebookS, nofCodebookBytes, current);
    current += ret;
    AD_CHECK_EQ(ret, size_t(nofCodebookBytes));
    ret = _textIndexFile.read(
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

// _____________________________________________________________________________
size_t Index::getIndexOfBestSuitedElTerm(const vector<string>& terms) const {
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
      _textVocab.getIdRangeForFullTextPrefix(terms[i], &range);
    } else {
      if (entityTerm) {
        if (!_vocab.getId(terms[i], &range._first)) {
          LOG(DEBUG) << "Term: " << terms[i] << " not in entity vocabulary\n";
          return i;
        } else {
        }
      } else if (!_textVocab.getId(terms[i], &range._first)) {
        LOG(DEBUG) << "Term: " << terms[i] << " not in vocabulary\n";
        return i;
      }
      range._last = range._first;
    }
    const auto& tbmd =
        entityTerm
            ? _textMeta.getBlockInfoByEntityId(range._first)
            : _textMeta.getBlockInfoByWordRange(range._first, range._last);
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
void Index::getECListForWordsAndSingleSub(const string& words,
                                          const vector<array<Id, I>>& subres,
                                          size_t subResMainCol, size_t limit,
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
    ad_utility::HashMap<Id, vector<array<Id, I>>> subEs;
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

template void Index::getECListForWordsAndSingleSub(
    const string& words, const vector<array<Id, 1>>& subres,
    size_t subResMainCol, size_t limit, vector<array<Id, 4>>& res) const;

template void Index::getECListForWordsAndSingleSub(
    const string& words, const vector<array<Id, 2>>& subres,
    size_t subResMainCol, size_t limit, vector<array<Id, 5>>& res) const;

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

  // TODO: more code for efficiency.
  // Examine the possibility to branch if subresults are
  // much larger than the number of matching postings.
  // Could binary search in them, then instead of create sets first.

  LOG(DEBUG) << "Filtering matching contexts and building cross-product...\n";
  vector<array<Id, 5>> nonAggRes;
  if (cids.size() > 0) {
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
    Id currentContext = cids[0];
    bool matched = false;
    bool matched1 = false;
    bool matched2 = false;
    for (size_t i = 0; i < cids.size(); ++i) {
      if (cids[i] != currentContext) {
        if (matched) {
          FTSAlgorithms::appendCrossProduct(cids, eids, scores,
                                            currentContextFrom, i, subEs1,
                                            subEs2, nonAggRes);
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
    const vector<ad_utility::HashMap<Id, vector<vector<Id>>>>& subResMaps,
    size_t limit, vector<vector<Id>>& res) const {
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

// _____________________________________________________________________________
size_t Index::getSizeEstimate(const string& words) const {
  size_t minElLength = std::numeric_limits<size_t>::max();
  auto terms = ad_utility::split(words, ' ');
  for (size_t i = 0; i < terms.size(); ++i) {
    IdRange range;
    bool entityTerm = (terms[i][0] == '<' && terms[i].back() == '>');
    if (terms[i].back() == PREFIX_CHAR) {
      if (!_textVocab.getIdRangeForFullTextPrefix(terms[i], &range)) {
        return 0;
      }
    } else {
      if (entityTerm) {
        if (!_vocab.getId(terms[i], &range._first)) {
          LOG(DEBUG) << "Term: " << terms[i] << " not in entity vocabulary\n";
          return 0;
        }
      } else if (!_textVocab.getId(terms[i], &range._first)) {
        LOG(DEBUG) << "Term: " << terms[i] << " not in vocabulary\n";
        return 0;
      }
      range._last = range._first;
    }
    const auto& tbmd =
        entityTerm
            ? _textMeta.getBlockInfoByEntityId(range._first)
            : _textMeta.getBlockInfoByWordRange(range._first, range._last);
    if (minElLength > tbmd._entityCl._nofElements) {
      minElLength = tbmd._entityCl._nofElements;
    }
  }
  return 1 + minElLength / 100;
}

// _____________________________________________________________________________
void Index::getRhsForSingleLhs(const IdTable& in, Id lhsId,
                               IdTable* result) const {
  LOG(DEBUG) << "Getting only rhs from a relation with " << in.size()
             << " elements by an Id key.\n";
  AD_CHECK(result);
  AD_CHECK_EQ(0, result->size());

  Id compareElem[] = {lhsId, 0};
  auto it = std::lower_bound(
      in.begin(), in.end(), compareElem,
      [](const auto& a, const auto& b) { return a[0] < b[0]; });

  while (it != in.end() && it[0] == lhsId) {
    result->push_back({it[1]});
    ++it;
  }

  LOG(DEBUG) << "Done. Matching right-hand-side EntityList now has "
             << result->size() << " elements."
             << "\n";
}

// _____________________________________________________________________________
void Index::setTextName(const string& name) { _textMeta.setName(name); }
