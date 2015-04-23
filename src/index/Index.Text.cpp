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
  _vocab.writeToFile(_onDiskBase + ".text.vocabulary");
  calculateBlockBoundaries();
  TextVec v(nofLines);
  passContextFileIntoVector(contextFile, v);
  LOG(INFO) << "Sorting text index..." << std::endl;
  stxxl::sort(begin(v), end(v), SortText(), STXXL_MEMORY_TO_USE);
  LOG(INFO) << "Sort done." << std::endl;

  createTextIndex(indexFilename, v);
  openFileHandles();
}

// _____________________________________________________________________________
void Index::addTextFromOnDiskIndex() {
  _vocab.readFromFile(_onDiskBase + "text.vocabulary");
  _textIndexFile.open(string(_onDiskBase + ".text.index").c_str(), "r");
  AD_CHECK(_textIndexFile.isOpen());
  off_t metaFrom;
  off_t metaTo = _textIndexFile.getLastOffset(&metaFrom);
  unsigned char* buf = new unsigned char[metaTo - metaFrom];
  _textIndexFile.read(buf, static_cast<size_t>(metaTo - metaFrom), metaFrom);
  _textMeta.createFromByteBuffer(buf);
  delete[] buf;
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
  vector<Posting> classicPostings;
  vector<Posting> entityPostings;
  for (TextVec::bufreader_type reader(vec); !reader.empty(); ++reader) {
    TextBlockMetaData tbmd;
    if (std::get<0>(*reader) != currentBlockId) {
      AD_CHECK(classicPostings.size() > 0);
      ContextListMetaData classic = writePostings(out, classicPostings, true);
      ContextListMetaData entity = writePostings(out, entityPostings, false);
      _textMeta.addBlock(TextBlockMetaData(
          std::get<1>(*classicPostings.begin()),
          std::get<1>(classicPostings.back()),
          classic,
          entity
      ));
      classicPostings.clear();
      entityPostings.clear();
      currentBlockId = std::get<0>(*reader);
    }
    if (!std::get<4>(*reader)) {
      classicPostings.emplace_back(std::make_tuple(
          std::get<1>(*reader),
          std::get<2>(*reader),
          std::get<3>(*reader)
      ));
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