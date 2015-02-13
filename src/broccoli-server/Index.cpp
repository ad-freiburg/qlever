// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include <stdlib.h>

#include <cmath>
#include <string>
#include <vector>
#include <algorithm>
#include <limits>

#include "./Globals.h"
#include "../util/Exception.h"
#include "../util/Log.h"
#include "../util/File.h"
#include "../util/Simple8bCode.h"
#include "./Index.h"

using std::string;
using std::vector;
using std::endl;

using ad_utility::File;

namespace ad_semsearch {
// _____________________________________________________________________________
void Index::readUncompressedBlock(File* file,
    const BlockMetaData& blockMetaData, PostingList* postingList) const {
  _readIndexListsTimer.cont();
  LOG(DEBUG) << "Reading block from disk." << endl;
  size_t nofElements = blockMetaData._nofPostings;

  Id* words = new Id[nofElements];
  Id* contexts = new Id[nofElements];
  Score* scores = new Score[nofElements];
  Position* positions = new Position[nofElements];

  size_t ret = file->read(words, nofElements * sizeof(Id),
      blockMetaData._startOfWordList);
  AD_CHECK_EQ(ret, nofElements * sizeof(Id));
  ret = file->read(contexts, nofElements * sizeof(Id),
      blockMetaData._startOfContextList);
  AD_CHECK_EQ(ret, nofElements * sizeof(Id));
  ret = file->read(scores, nofElements * sizeof(Score),
      blockMetaData._startOfScoreList);
  AD_CHECK_EQ(ret, nofElements * sizeof(Score));
  ret = file->read(positions, nofElements * sizeof(Position),
      blockMetaData._startOfPositionList);
  AD_CHECK_EQ(ret, nofElements * sizeof(Position));

  // TODO(Björn): Resize + set should not be faster than reserve
  // and emplace back. Strangely is was in experiments.
  // Check this later with other compiler versions etc.
  postingList->resize(nofElements);
  for (size_t i = 0; i < nofElements; ++i) {
    (*postingList)[i]._id = words[i];
    (*postingList)[i]._contextId = contexts[i];
    (*postingList)[i]._score = scores[i];
    (*postingList)[i]._position = positions[i];
  }

  delete[] words;
  delete[] contexts;
  delete[] scores;
  delete[] positions;

  _readIndexListsTimer.stop();

  LOG(DEBUG) << "Done reading block from disk. Size: " << postingList->size()
      << endl;
}
// _____________________________________________________________________________
void Index::readCompressedBlock(File* file, const BlockMetaData& blockMetaData,
    PostingList* postingList) const {
  _readIndexListsTimer.cont();
  LOG(DEBUG) << "Reading block from disk." << endl;
  size_t nofElements = blockMetaData._nofPostings;

  _diskTimer.cont();
  Id* words = new Id[nofElements + 239];
  Id* contexts;  // Size is known only later
  Score* scores = new Score[nofElements + 239];
  Position* positions = new Position[nofElements + 239];
  uint64_t* encoded = new uint64_t[nofElements];

  off_t nofCodebookBytes;

  // Words
  off_t currentOfft = blockMetaData._startOfWordList;
  size_t ret = file->read(&nofCodebookBytes, sizeof(off_t), currentOfft);
  AD_CHECK_EQ(sizeof(off_t), ret);
  currentOfft += ret;
  Id* wordCodebook = new Id[nofCodebookBytes / sizeof(Id)];
  ret = file->read(wordCodebook, nofCodebookBytes);
  currentOfft += ret;
  AD_CHECK_EQ(ret, size_t(nofCodebookBytes));
  ret = file->read(encoded, blockMetaData._startOfContextList - currentOfft);
  currentOfft += ret;
  AD_CHECK_EQ(blockMetaData._startOfContextList, currentOfft);
  _diskTimer.stop();
  _wDecompressionTimer.cont();
  ad_utility::Simple8bCode::decode(encoded, nofElements, words);
  _wDecompressionTimer.stop();

  // Contexts
  Id nofNonZeroGaps;
  _diskTimer.cont();
  ret = file->read(&nofNonZeroGaps, sizeof(nofNonZeroGaps));
  currentOfft += ret;
  contexts = new Id[2 * nofNonZeroGaps + 239];
  ret = file->read(encoded, blockMetaData._startOfScoreList - currentOfft);
  currentOfft += ret;
  AD_CHECK_EQ(blockMetaData._startOfScoreList, currentOfft);
  _diskTimer.stop();
  _cDecompressionTimer.cont();
  ad_utility::Simple8bCode::decode(encoded, 2 * nofNonZeroGaps, contexts);
  _cDecompressionTimer.stop();

  // Scores
  _diskTimer.cont();
  ret = file->read(&nofCodebookBytes, sizeof(off_t));
  AD_CHECK_EQ(sizeof(off_t), ret);
  currentOfft += ret;
  Score* scoreCodebook = new Score[nofCodebookBytes / sizeof(Score)];
  ret = file->read(scoreCodebook, nofCodebookBytes);
  currentOfft += ret;
  AD_CHECK_EQ(ret, size_t(nofCodebookBytes));
  ret = file->read(encoded, blockMetaData._startOfPositionList - currentOfft);
  currentOfft += ret;
  AD_CHECK_EQ(blockMetaData._startOfPositionList, currentOfft);
  _diskTimer.stop();
  _sDecompressionTimer.cont();
  ad_utility::Simple8bCode::decode(encoded, nofElements, scores);
  _sDecompressionTimer.stop();

  // Positions
  _diskTimer.cont();
  ret = file->read(encoded, (blockMetaData._posOfLastByte + 1) - currentOfft);
  currentOfft += ret;
  AD_CHECK_EQ(blockMetaData._posOfLastByte + 1, currentOfft);
  _diskTimer.stop();
  _pDecompressionTimer.cont();
  ad_utility::Simple8bCode::decode(encoded, nofElements, positions);
  _pDecompressionTimer.stop();

  postingList->clear();

  // Do it separately for measurements
//  {
//    _readIndexListsTimer.stop();
//    Id* wordsR = new Id[nofElements + 239];
//    Id* contextsR = new Id[nofElements + 239];
//    Score* scoresR = new Score[nofElements + 239];
//
//    _remapTimer.cont();
//    for (size_t i = 0; i < nofElements; ++i) {
//      wordsR[i] = wordCodebook[words[i]];
//      scoresR[i] = scoreCodebook[scores[i]];
//    }
//
//    size_t posInContextsR = 0;
//    Id lastContextId = 0;
//    for (size_t i = 0; i < nofNonZeroGaps; ++i) {
//      lastContextId += contexts[i];
//      contextsR[posInContextsR++] = lastContextId;
//      size_t nofZeros = contexts[nofNonZeroGaps + i];
//      for (size_t j = 0; j < nofZeros; ++j) {
//        contextsR[posInContextsR++] = lastContextId;
//      }
//    }
//    AD_CHECK_EQ(nofElements, posInContextsR);
//    _remapTimer.stop();
//
//    _restoreTimer.cont();
//
//    postingList->resize(nofElements);
//    for (size_t i = 0; i < nofElements; ++i) {
//      (*postingList)[i]._id = wordsR[i];
//      (*postingList)[i]._contextId = contextsR[i];
//      (*postingList)[i]._score = scoresR[i];
//      (*postingList)[i]._position = positions[i];
//    }
//
//    _restoreTimer.stop();
//
//    delete[] wordsR;
//    delete[] contextsR;
//    delete[] scoresR;
//    _readIndexListsTimer.cont();
//    postingList->clear();
//  }

  // Do remap and restore together, because it is faster that way.
  Id contextId = 0;
  // TODO(Björn): Resize + set should not be faster than reserve
  // and emplace back. Strangely is was in experiments.
  // Check this later with other compiler versions etc.
  _remapRestoreTimer.cont();
  postingList->resize(nofElements);
  for (size_t i = 0; i < nofElements; ++i) {
    (*postingList)[i]._id = wordCodebook[words[i]];
    (*postingList)[i]._score = scoreCodebook[scores[i]];
    (*postingList)[i]._position = positions[i];
  }
  size_t posInResult = 0;
  for (size_t i = 0; i < nofNonZeroGaps; ++i) {
    contextId += contexts[i];
    (*postingList)[posInResult++]._contextId = contextId;
    for (size_t j = 0; j < contexts[nofNonZeroGaps + i]; ++j) {
      (*postingList)[posInResult++]._contextId = contextId;
    }
  }
  AD_CHECK_EQ(nofElements, posInResult);
  _remapRestoreTimer.stop();

  delete[] words;
  delete[] contexts;
  delete[] scores;
  delete[] positions;
  delete[] encoded;

  delete[] wordCodebook;
  delete[] scoreCodebook;

  _readIndexListsTimer.stop();

  LOG(DEBUG) << "Done reading block from disk. Size: " << postingList->size()
      << endl;
}
// _____________________________________________________________________________
void Index::registerFulltextIndex(const string& baseName,
    bool alsoRegisterExcerptsFile) {
  LOG(INFO) << "Registering Fulltext-Index with basename: " << baseName
      << endl;
  string indexFileName = baseName + INDEX_FILE_EXTENSION;
  string vocabFileName = baseName + VOCABULARY_FILE_EXTENSION;
  string excerptsFileName = baseName + EXCERPTS_FILE_EXTENSION;
  string excerptsOffsetsFileName = baseName + ".docs-offsets";
  _fullTextIndexes.push_back(File(indexFileName.c_str(), "r"));
  _fulltextVocabularies.push_back(Vocabulary());
  _fulltextVocabularies.back().readFromFile(vocabFileName);
  FulltextMetaData meta;
  meta.initFromFile(&_fullTextIndexes.back());
  _fulltextMetaData.push_back(meta);
  AD_CHECK_EQ(_fullTextIndexes.size(), _fulltextMetaData.size());
  AD_CHECK_EQ(_fullTextIndexes.size(), _fulltextVocabularies.size());


  LOG(INFO)
      << "Registration of Fulltext-Index complete. There are "
      << meta.getBlockCount() << " blocks." << endl;
  LOG(DEBUG)
      << "The registered Fulltext-Index has "
      << meta.calculateTotalNumberOfPostings() << " postings in total."
      << endl;
}
// _____________________________________________________________________________
void Index::registerOntologyIndex(const string& baseName,
    bool alsoRegisterPPandES) {
  LOG(INFO) << "Registering Ontology-Index with basename: " << baseName
      << endl;
  string indexFileName = baseName + INDEX_FILE_EXTENSION;
  string vocabFileName = baseName + VOCABULARY_FILE_EXTENSION;
  string pseudoPrefixesFileName = baseName + PSEUDO_PREFIXES_FILE_EXTENSION;
  string entityScoresFileName = baseName + ENTITY_SCORES_FILE_EXTENSION;
  string reverseRelationMap = baseName + REVERSE_RELATIONS_FILE_EXTENSION;

  _ontologyIndex.open(indexFileName.c_str(), "r");
  _ontologyVocabulary.readFromFile(vocabFileName);
  _ontologyMetaData.initFromFile(&_ontologyIndex);
  _reversedRelationProvider.initFromFile(reverseRelationMap);
  if (alsoRegisterPPandES) {
    readEntityScores(entityScoresFileName);
    readPseudoPrefixes(pseudoPrefixesFileName);
  }
  LOG(INFO)
      << "Registration of Ontology-Index complete. There are "
      << _ontologyMetaData.getRelationCount() << " relations." << endl;
}

// _____________________________________________________________________________
void Index::readFullRelation(File* file, const RelationMetaData& relMetaData,
    Relation* relationList) const {
  LOG(DEBUG) << "Reading full relation from disk." << endl;
  AD_CHECK(relationList);
  AD_CHECK_EQ(relationList->size(), 0);
  for (size_t i = 0; i < relMetaData._blockInfo.size(); ++i) {
    readRelationBlock(file, relMetaData._blockInfo[i], relationList);
  }
  LOG(DEBUG) << "Done reading relation. Size: " << relationList->size() << endl;
}
// _____________________________________________________________________________
void Index::readRelationBlock(File* file,
    const RelationBlockMetaData& blockMetaData, Relation* relationList) const {
  _readIndexListsTimer.cont();
  LOG(DEBUG) << "Reading relation-block from disk." << endl;
  AD_CHECK(relationList);
  size_t nofElements = blockMetaData._nofElements;

  Id* content = new Id[2* nofElements];
  Score* scores = new Score[nofElements];

  size_t ret = file->read(content, nofElements * sizeof(Id) * 2,
      blockMetaData._startOfLhsData);
  AD_CHECK_EQ(ret, nofElements * sizeof(Id) * 2);

  ret = file->read(scores, nofElements * sizeof(Score),
        blockMetaData._startOfScores);
    AD_CHECK_EQ(ret, nofElements * sizeof(Score));

  relationList->resize(nofElements);
  for (size_t i = 0; i < nofElements; ++i) {
    (*relationList)[i]._lhs = content[i];
    (*relationList)[i]._rhs = content[i + nofElements];
    (*relationList)[i]._score = scores[i];
  }

  delete[] content;
  delete[] scores;

  LOG(DEBUG) << "Done reading block. " << "Current size of target list "
      << "(not necessarily everything from this block): "
      << relationList->size() << endl;
  _readIndexListsTimer.stop();
}
// _____________________________________________________________________________
void Index::readRelationBlockRhsIntoEL(File* file,
    const RelationBlockMetaData& blockMetaData,
    EntityList* el) const {
  _readIndexListsTimer.cont();
  LOG(DEBUG) << "Reading relation-block rhs from disk." << endl;
  AD_CHECK(el);
  size_t nofElements = blockMetaData._nofElements;

  Id* rhs = new Id[nofElements];

  size_t ret = file->read(rhs, nofElements * sizeof(Id),
      blockMetaData._startOfRhsData);
  AD_CHECK_EQ(ret, nofElements * sizeof(Id));


  el->resize(nofElements);
  for (size_t i = 0; i < nofElements; ++i) {
    (*el)[i]._id = rhs[i];
    (*el)[i]._score = 1;
  }

  delete[] rhs;

  LOG(DEBUG) << "Done reading block. " << "Current size of target list "
      << "(not necessarily everything from this block): "
      << el->size() << endl;
  _readIndexListsTimer.stop();
}
// _____________________________________________________________________________
void Index::initInMemoryRelations() {
  AD_CHECK(!_initialized);
  AD_CHECK_GT(_ontologyVocabulary.size(), 0);

  // The has-relations relation.
  {
    _hasRelationsRelation.clear();
    Id relId;
    getIdForOntologyWord(string(RELATION_PREFIX) + HAS_RELATIONS_RELATION,
        &relId);
    const RelationMetaData& rmd = getRelationMetaData(relId);
    readFullRelation(rmd, &_hasRelationsRelation);
  }

  // The is-a relation.
  {
    _isARelation.clear();
    Id relId;
    getIdForOntologyWord(string(RELATION_PREFIX) + IS_A_RELATION,
        &relId);
    const RelationMetaData& rmd = getRelationMetaData(relId);
    readFullRelation(rmd, &_isARelation);
  }

  // The available classes list
  {
    _availableClasses.clear();
    Id relId;
    getIdForOntologyWord(string(RELATION_PREFIX) + HAS_INSTANCES_RELATION,
        &relId);
    const RelationMetaData& rmd = getRelationMetaData(relId);
    AD_CHECK(_ontologyIndex.isOpen());
    readAvailableClasses(&_ontologyIndex, rmd, &_availableClasses);
  }

  // The relation patterns to id list vector
  {
    LOG(INFO) << "getting the first relation Id\n";

    IdRange idRange;
    bool success = getOntologyVocabulary().getIdRangeForOntologyPrefix(
        string(RELATION_PREFIX) + PREFIX_CHAR, &idRange);
    AD_CHECK(success);
    _firstRelId = idRange._first;
    _lastRelId = idRange._last;
    LOG(DEBUG) << "Reading relation patterns to id list vector...\n";
    _relationPatternToIdList.clear();
    _relationPatternToIdList.resize(
        std::numeric_limits<RelationPattern>::max());
    Id relId;
    getIdForOntologyWord(string(RELATION_PREFIX) + RELATION_PATTERNS, &relId);
    const RelationMetaData& rmd = getRelationMetaData(relId);
    Relation relation;
    readFullRelation(rmd, &relation);
    RelationPattern pattern = 0;
    vector<Id> idsForThisPattern;
    for (size_t i = 0; i < relation.size(); ++i) {
      if (pattern != relation[i]._lhs) {
        LOG(TRACE) << "Pattern #" << pattern << " has a list of "
            << idsForThisPattern.size() << "relations.\n";
        _relationPatternToIdList[pattern] = idsForThisPattern;
        idsForThisPattern.clear();
        ++pattern;
      }
      while (pattern < relation[i]._lhs) {
        ++pattern;
      }
      AD_CHECK_LE(_firstRelId, relation[i]._rhs);
      if (pattern == relation[i]._lhs) {
        idsForThisPattern.push_back(relation[i]._rhs - _firstRelId);
      }
    }
    _relationPatternToIdList[pattern] = idsForThisPattern;
  }

  // The entity id to relation pattern vector
  {
    LOG(DEBUG) << "Reading entity Id to relation pattern vector...\n";
    _entityIdToRelationPattern.clear();
    _entityIdToRelationPattern.resize(getSizeOfEntityUniverse());
    Id relId;
    getIdForOntologyWord(string(RELATION_PREFIX) + HAS_RELATION_PATTERN,
        &relId);
    const RelationMetaData& rmd = getRelationMetaData(relId);
    Relation relation;
    readFullRelation(rmd, &relation);
    for (size_t i = 0; i < relation.size(); ++i) {
      _entityIdToRelationPattern[getPureValue(relation[i]._lhs)] =
          static_cast<RelationPattern>(relation[i]._rhs);
    }
  }

  std::unordered_map<Id, Id> eIdToClassId;
  // The class id to entity id vector
  {
    LOG(DEBUG) << "Reading entity Id to class pattern vector...\n";
    _classIdToEntityId.clear();
    Id relId;
    getIdForOntologyWord(string(RELATION_PREFIX) + EID_TO_CID, &relId);
    const RelationMetaData& rmd = getRelationMetaData(relId);
    Relation relation;
    readFullRelation(rmd, &relation);
    _classIdToEntityId.resize(relation.size());
    for (size_t i = 0; i < relation.size(); ++i) {
      eIdToClassId[relation[i]._lhs] = relation[i]._rhs;
      _classIdToEntityId[relation[i]._rhs] = relation[i]._lhs;
    }
  }

  // The class patterns to id list vector
  {
    LOG(DEBUG) << "Reading class patterns to id list vector...\n";
    _classPatternToIdList.clear();
    _classPatternToIdList.resize(
            std::numeric_limits<ClassPattern>::max());
    Id relId;
    getIdForOntologyWord(string(RELATION_PREFIX) + CLASS_PATTERNS, &relId);
    const RelationMetaData& rmd = getRelationMetaData(relId);
    Relation relation;
    readFullRelation(rmd, &relation);
    ClassPattern pattern = 0;
    vector<Id> idsForThisPattern;
    for (size_t i = 0; i < relation.size(); ++i) {
      if (pattern != relation[i]._lhs) {
        _classPatternToIdList[pattern] = idsForThisPattern;
        idsForThisPattern.clear();
        ++pattern;
      }
      if (pattern == relation[i]._lhs) {
        idsForThisPattern.push_back(relation[i]._rhs);
      }
    }
    _classPatternToIdList[pattern] = idsForThisPattern;
  }

  // The entity id to class pattern vector
  {
    LOG(DEBUG) << "Reading entity Id to class pattern vector...\n";
    _entityIdToClassPattern.clear();
    _entityIdToClassPattern.resize(getSizeOfEntityUniverse());
    Id relId;
    getIdForOntologyWord(string(RELATION_PREFIX) + HAS_CLASS_PATTERN,
        &relId);
    const RelationMetaData& rmd = getRelationMetaData(relId);
    Relation relation;
    readFullRelation(rmd, &relation);
    for (size_t i = 0; i < relation.size(); ++i) {
      _entityIdToClassPattern[getPureValue(relation[i]._lhs)] =
          static_cast<ClassPattern>(relation[i]._rhs);
    }
  }

  _initialized = true;
}
// _____________________________________________________________________________
void Index::readAvailableClasses(File* ontolgyIndex,
    const RelationMetaData& relMetaData,
    EntityList* availableClassesList) const {
  LOG(DEBUG) << "Reading available classes from disk." << endl;
  AD_CHECK(availableClassesList);
  AD_CHECK_EQ(availableClassesList->size(), 0);

  AD_CHECK_EQ(relMetaData._blockInfo.size(), 1);
  const RelationBlockMetaData& blockMetaData = relMetaData._blockInfo[0];
  size_t nofElements = blockMetaData._nofElements;

  Id* content = new Id[2 * nofElements];

  size_t ret = ontolgyIndex->read(content, nofElements * sizeof(Id) * 2,
      blockMetaData._startOfLhsData);
  AD_CHECK_EQ(ret, nofElements * sizeof(Id) * 2);

  for (size_t i = 0; i < nofElements; ++i) {
    EntityWithScore entry(content[i],
        static_cast<AggregatedScore>(content[i + nofElements]));
    availableClassesList->push_back(entry);
  }

  delete[] content;

  LOG(DEBUG)
  << "Done reading available classes. Read " << availableClassesList->size()
      << " classes," << endl;
}
// _____________________________________________________________________________
void Index::readEntityScores(const string& fileName) {
  LOG(INFO) << "Reading entity scores from file: " << fileName << "...\n";
  _entityScores.clear();
  _entityScores.resize(_ontologyVocabulary.size(), 0);
  ad_utility::File file(fileName.c_str(), "r");
  char buf[BUFFER_SIZE_ONTOLOGY_WORD];
  string line;
  size_t warningCounter = 0;
  while (file.readLine(&line, buf, BUFFER_SIZE_ONTOLOGY_WORD)) {
    Id entityId;
    size_t indexOfTab = line.find('\t');
    assert(indexOfTab != line.npos);
    size_t indexOfTab2 = line.find('\t', indexOfTab + 1);
    assert(indexOfTab2 != line.npos);
    string entity(line.substr(0, indexOfTab));

    AggregatedScore score(atoi(
        line.substr(indexOfTab + 1, indexOfTab2 - (indexOfTab + 1)).c_str()));
    int abstractnessCount(
        atoi(line.substr(indexOfTab2 + 1).c_str()));

    bool success = _ontologyVocabulary.getIdForOntologyWord(entity, &entityId);
    if (!success) {
      if (warningCounter < 15) {
        LOG(DEBUG) << "Unable to retrieve ID for entity: "  << entity << "\n";
      }
      if (warningCounter == 15) {
        LOG(DEBUG) << "Suppressing more warnings.\n";
      }
      warningCounter++;
    } else if (abstractnessCount < ABSTRACT_ENTITY_THRESHOLD) {
      _entityScores[getPureValue(entityId)] = score;
    }
  }
  LOG(INFO) << "Done reading entity scores.\n";
  LOG(INFO) << "Creating the list of all Entities from the scores...\n";

  _allEntities.clear();
  for (size_t i = 0; i < _entityScores.size(); ++i) {
    if (_entityScores[i] != 0) {
      _allEntities.push_back(
          EntityWithScore(getFirstId(IdType::ONTOLOGY_ELEMENT_ID) + i,
              _entityScores[i]));
    }
  }
  LOG(INFO) << "Done creating list of all Entities.\n";
}

// _____________________________________________________________________________
void Index::readContextDocumentMappingFromFile(const string& fileName) {
  LOG(INFO) << "Reading contextId to document mapping from file: \""
      << fileName << "\"..." << endl;
  AD_CHECK(_ontologyVocabulary.size() > 0);
  File in(fileName.c_str(), "r");
  off_t nofBytes = in.sizeOfFile();
  size_t nofDocuments = nofBytes / sizeof(Id);
  Id* data = new Id[nofDocuments];
  size_t bytesRead = in.readFromBeginning(data, nofBytes);
  AD_CHECK_EQ(static_cast<size_t>(nofBytes), bytesRead);
  for (size_t i = 0; i < nofDocuments; ++i) {
    _contextDocumentMapping.push_back(data[i]);
  }
  // Add one bogus context / permanent sentinel
  _contextDocumentMapping.push_back(~Id(0));
  delete[] data;
  LOG(INFO) << "Done, read " << _contextDocumentMapping.size()
      << " contextId upper bounds." << endl;
}


// _____________________________________________________________________________
void Index::getIdListForPseudoPrefix(const string& prefix,
    vector<Id>* result) const {
  _pseudoPrefixTimer.cont();
  IdRange idRange;
  if (_pseudoPrefixKeys.getIdRangeForPrefixNoPrefixSizeCheck(
      prefix + PREFIX_CHAR, &idRange)) {
    AD_CHECK_LT(idRange._last, _pseudoPrefixValues.size());
    size_t index = 0;
    result->resize(idRange._last + 1 - idRange._first);
    for (size_t i = idRange._first; i <= idRange._last; ++i) {
      (*result)[index++] = _pseudoPrefixValues[i];
    }
    std::sort(result->begin(), result->end());
  }
  _pseudoPrefixTimer.stop();
}

// _____________________________________________________________________________
bool Index::getIdRangeForValueRange(const string& lower,
    const string& upper, IdRange* idRange) const {
  return _ontologyVocabulary.getIdRangeBetweenTwoValuesInclusive(lower, upper,
      idRange);
}
// _____________________________________________________________________________
void Index::readPseudoPrefixes(const string& fileName) {
  LOG(INFO) << "Reading pseudo-prefixes from file " << fileName << '\n';

  ad_utility::File in(fileName.c_str(), "r");
  char buf[BUFFER_SIZE_ONTOLOGY_LINE];
  string line;

  while (in.readLine(&line, buf, BUFFER_SIZE_ONTOLOGY_LINE)) {
    size_t indexOfTab = line.find('\t');
    _pseudoPrefixKeys.push_back(line.substr(0, indexOfTab));
    Id id = getFirstId(IdType::ONTOLOGY_ELEMENT_ID)
        + atoi(line.substr(indexOfTab + 1).c_str());
    _pseudoPrefixValues.push_back(id);
  }

  LOG(INFO) << "Done reading pseudo prefixes. Successfully read "
      << _pseudoPrefixKeys.size() << " items.\n";
}

// _____________________________________________________________________________
void Index::readStopWordsFromFile(const string& fileName) {
}
// _____________________________________________________________________________
string Index::getUrlForEntity(Id entityId) const {
  if (_entityUrlMap.size() < getPureValue(entityId)) {
    return _entityUrlPrefix
        + ad_utility::getLastPartOfString(getOntologyWordById(entityId), ':')
        + _entityUrlSuffix;
  } else {
    return _entityUrlMap[getPureValue(entityId)];
  }
}

// ____________________________________________________________________________
string Index::reverseRelation(const string& orig) const {
  return _reversedRelationProvider.reverseRelation(orig);
}

// _____________________________________________________________________________
Index::Index(bool usesCompression)
: _initialized(false), _usesCompression(usesCompression),
  _firstRelId(0),
  _lastRelId(0),
  _reversedRelationProvider(),
  _entityUrlPrefix(WIKIPEDIA_URL) {
  _readIndexListsTimer.reset();
  _readExcerptsTimer.reset();
  _pseudoPrefixTimer.reset();
  _docsFileBuffer = new char[BUFFER_SIZE_DOCSFILE_LINE];
}

// _____________________________________________________________________________
Index::~Index() {
  delete[] _docsFileBuffer;
}
}
