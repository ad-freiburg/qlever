// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#ifndef SERVER_INDEX_H_
#define SERVER_INDEX_H_

#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "../util/File.h"
#include "../util/Timer.h"
#include "../util/Log.h"

#include "../engine/Id.h"

#include "./ReversedRelationNameProvider.h"
#include "./Globals.h"
#include "./PostingList.h"
#include "./Relation.h"
#include "./EntityList.h"
#include "./Identifiers.h"
#include "./Vocabulary.h"
#include "./IndexMetaData.h"

using ad_utility::File;
using std::string;
using std::vector;

namespace ad_semsearch {
//! The search-index behind everything.
//! Provides access to different indexed lists and operations
//! on those lists.
class Index {
  public:
    explicit Index(bool usesCompression = true);
    virtual ~Index();

    //! Reads a block from disc and into a posting list.
    //! Assumes a file layout that matches the info in the metaData.
    void readUncompressedBlock(File* file, const BlockMetaData& blockMetaData,
        PostingList* postingList) const;

    //! Reads a block from disc and into a posting list.
    //! Assumes a file layout that matches the info in the metaData.
    void readCompressedBlock(File* file, const BlockMetaData& blockMetaData,
        PostingList* postingList) const;

    //! Reads a block from disc and into a posting list.
    //! Assumes a file layout that matches the info in the metaData.
    void readBlock(const BlockMetaData& blockMetaData,
        PostingList* postingList, size_t numberOfFulltextIndex = 0) const {
      AD_CHECK_GT(_fullTextIndexes.size(), numberOfFulltextIndex);
      AD_CHECK(_fullTextIndexes[numberOfFulltextIndex].isOpen());
      if (_usesCompression) {
        readCompressedBlock(&_fullTextIndexes[numberOfFulltextIndex],
            blockMetaData, postingList);
      } else {
        readUncompressedBlock(&_fullTextIndexes[numberOfFulltextIndex],
            blockMetaData, postingList);
      }
    }

    //! Reads a relation from disc.
    void readFullRelation(File* file, const RelationMetaData& relMetaData,
        Relation* relationList) const;

    //! Reads a Relation Block from disc and appends to relationRead.
    void readRelationBlock(File* file,
        const RelationBlockMetaData& blockMetaData,
        Relation* relationList) const;

    // Directly read the rhs of a relation block into an EntityList.
    // Useful for is-a.
    void readRelationBlockRhsIntoEL(File* file,
        const RelationBlockMetaData& blockMetaData,
        EntityList* el) const;

    void readRelationBlockRhsIntoEL(const RelationBlockMetaData& blockMetaData,
        EntityList* el) const {
      AD_CHECK(_ontologyIndex.isOpen());
      readRelationBlockRhsIntoEL(&_ontologyIndex, blockMetaData, el);
    }

    //! Get the special has-relations relation.
    const Relation& getHasRelationsRelation() const {
      AD_CHECK(_initialized);
      return _hasRelationsRelation;
    }

    //! Get the is-a relation.
    const Relation& getIsARelation() const {
      AD_CHECK(_initialized);
      return _isARelation;
    }

    //! Get the special available classes EntityList.
    const EntityList& getAvailableClasses() const {
      AD_CHECK(_initialized);
      return _availableClasses;
    }

    //! Get the list of all entities.
    const EntityList& getAllEntities() const {
      AD_CHECK(_initialized);
      return _allEntities;
    }

    //! Reads a relation from disc. Use the registered ontologyIndex
    //! as file.
    void readFullRelation(const RelationMetaData& relMetaData,
        Relation* relationList) const {
      AD_CHECK(_ontologyIndex.isOpen());
      readFullRelation(&_ontologyIndex, relMetaData, relationList);
    }

    //! Reads a Relation Block from disc and appends to relationRead.
    //! Use the registered ontologyIndex as file.
    void readRelationBlock(const RelationBlockMetaData& blockMetaData,
        Relation* relationList) const {
      AD_CHECK(_ontologyIndex.isOpen());
      readRelationBlock(&_ontologyIndex, blockMetaData, relationList);
    }

    const FulltextMetaData& getFullTextMetaData(
        size_t numberOfFulltextIndex = 0) const {
      AD_CHECK_GT(_fulltextMetaData.size(), numberOfFulltextIndex);
      return _fulltextMetaData[numberOfFulltextIndex];
    }

    //! Registers a full-text index file. Reads meta data
    //! and the corresponding vocabulary.
    //! Note that this will register the index as additional
    //! index and not replace any existing index.
    //! ContextId are assumed to be disjunct.
    void registerFulltextIndex(const string& baseName,
        bool alsoRegisterExcerptsFile);

    //! Clears the list of registered full-text indexes
    void clearRegisteredFulltextIndexes() {
      _fullTextIndexes.clear();
      _fulltextMetaData.clear();
      _fulltextVocabularies.clear();
    }

    //! Tries to get the word represented by an arbitrary ID.
    //! Take care: If id is a full-text-word-id, numberOfFulltextIndex
    //! has to be passed correctly to avoid throwing an Exception.
    //! In general, callers should prefer calling getFulltextWordById and
    //! getOntologyWordById depending on what is needed.
    const string& getWordById(Id id, size_t numberOfFulltextIndex = 0) const {
      if (isIdOfType<IdType::ONTOLOGY_ELEMENT_ID>(id)) {
        return getOntologyWordById(id);
      } else {
        return getFulltextWordById(id, numberOfFulltextIndex);
      }
    }

    //! Get an Id from the vocabulary for some ontology word.
    bool getIdForOntologyWord(const string& word, Id* id) const {
      return _ontologyVocabulary.getIdForOntologyWord(word, id);
    }

    bool getIdForFullTextWord(const string& word, Id* id) {
      return _fulltextVocabularies[0].getIdForFullTextWord(word, id);
    }

    //! Get an IdRange from the vocabulary for some full-text word-prefix.
    //! If the word is no prefix first and last ID in the range will be equal.
    //! The return value signals if the word / prefix have been found at all.
    bool getIdRangeForFullTextWordOrPrefix(const string& word,
        IdRange* range) const {
      AD_CHECK_GT(word.size(), 0);
      if (word[word.size() - 1] == PREFIX_CHAR) {
        return _fulltextVocabularies[0].getIdRangeForFullTextPrefix(word,
            range);
      } else {
        Id wordId;
        bool success = _fulltextVocabularies[0].getIdForFullTextWord(word,
            &wordId);
        range->_first = wordId;
        range->_last = wordId;
        return success;
      }
    }

    //! Get an IdRange from the vocabulary for some ontology word-prefix.
    //! If the word is no prefix first and last ID in the range will be equal.
    //! The return value signals if the word / prefix have been found at all.
    bool getIdRangeForOntologyWordOrPrefix(const string& word,
        IdRange* range) const {
      AD_CHECK_GT(word.size(), 0);
      if (word[word.size() - 1] == PREFIX_CHAR) {
        return _ontologyVocabulary.getIdRangeForOntologyPrefix(word,
            range);
      } else {
        Id id;
        bool success = _ontologyVocabulary.getIdForOntologyWord(word, &id);
        range->_first = id;
        range->_last = id;
        return success;
      }
    }

    //! Gets block meta data for a given range of full-text word ids.
    const BlockMetaData& getBlockInfoByWordRange(IdRange idRange) const {
      return _fulltextMetaData[0].getBlockInfoByWordRange(idRange._first,
          idRange._last);
    }

    //! Gets block meta data for a single full-text word id.
    const BlockMetaData& getBlockInfoByFulltextWordId(Id wordId) const {
      return _fulltextMetaData[0].getBlockInfoByWordRange(wordId, wordId);
    }

    //! Gets a word from the full-text vocabulary represented by that particular
    //! Id. Take care that the correct full-text index is used or else
    //! an exception will be thrown.
    const string& getFulltextWordById(Id wordId,
        size_t numberOfFulltextIndex = 0) const {
      AD_CHECK_LT(numberOfFulltextIndex, _fulltextVocabularies.size());
      AD_CHECK_LT(wordId, _fulltextVocabularies[numberOfFulltextIndex].size());
      return _fulltextVocabularies[numberOfFulltextIndex][wordId];
    }

    //! Gets the ontology word represented by the ID passed.
    const string& getOntologyWordById(Id ontologyWordId) const {
      AD_CHECK_LT(getPureValue(ontologyWordId), _ontologyVocabulary.size());
      return _ontologyVocabulary[getPureValue(ontologyWordId)];
    }

    //! Get the meta data for a given relation by relationId.
    const RelationMetaData& getRelationMetaData(Id relationId) const {
      return _ontologyMetaData.getRelationMetaData(relationId);
    }

    //! Get an id range comprised by the two values.
    //! return false if value strings are invalid
    //! or the range does not contain an element.
    bool getIdRangeForValueRange(const string& lower,
        const string& upper, IdRange* idRange) const;

    //! Get a (sorted) list of ontology WordIds for a given pseudo prefix.
    void getIdListForPseudoPrefix(const string& prefix,
        vector<Id>* result) const;

    //! Registers the ontology-index and vocabulary.
    //! Note that only one ontology-index is supported at a time
    //! and registering an index will remove any old one.
    void registerOntologyIndex(const string& baseName,
        bool alsoRegisterPPandES = true);

    //! Initialize this object properly, i.e. read the has-relations relation.
    void initInMemoryRelations();

    void readStopWordsFromFile(const string& file);

    //! Gets an url for the given entity.
    string getUrlForEntity(Id entityId) const;

    //! Reverse a relation. mkes you of a mapping, alternatively
    //! appends / removes a reversed suffix.
    string reverseRelation(const string& orig) const;

    // Friend tests (most tests don't need friendship so list is short):
    friend class IndexPerformanceTest;
    FRIEND_TEST(IndexTest, registerFulltextIndexTest);
    FRIEND_TEST(IndexTest, registerOntologyIndexTest);
    FRIEND_TEST(IndexTest, readFullRelationTest);
    FRIEND_TEST(IndexTest, readRelationBlockTest);

    const ad_utility::Timer& getReadIndexListsTimer() const {
      return _readIndexListsTimer;
    }

    const ad_utility::Timer& getReadExcerptsTimer() const {
      return _readExcerptsTimer;
    }

    const ad_utility::Timer& getPseudoPrefixTimer() const {
      return _pseudoPrefixTimer;
    }

    const ad_utility::Timer& getWordDecompressionTimer() const {
      return _wDecompressionTimer;
    }

    const ad_utility::Timer& getContextDecompressionTimer() const {
      return _cDecompressionTimer;
    }

    const ad_utility::Timer& getScoreDecompressionTimer() const {
      return _sDecompressionTimer;
    }

    const ad_utility::Timer& getPositionDecompressionTimer() const {
      return _pDecompressionTimer;
    }

    const ad_utility::Timer& getRemapRestoreTimer() const {
      return _remapRestoreTimer;
    }

    const ad_utility::Timer& getRemapTimer() const {
      return _remapTimer;
    }

    const ad_utility::Timer& getRestoreTimer() const {
      return _restoreTimer;
    }

    const ad_utility::Timer& getDiskTimer() const {
      return _diskTimer;
    }

    void resetTimers() const {
      _readIndexListsTimer.reset();
      _readExcerptsTimer.reset();
      _wDecompressionTimer.reset();
      _cDecompressionTimer.reset();
      _sDecompressionTimer.reset();
      _pDecompressionTimer.reset();
      _remapRestoreTimer.reset();
      _remapTimer.reset();
      _restoreTimer.reset();
      _pseudoPrefixTimer.reset();
      _diskTimer.reset();
    }

    const vector<AggregatedScore>& getEntityScores() const {
      return _entityScores;
    }

    const vector<Id>& getContextDocumentMapping() const {
      return _contextDocumentMapping;
    }

    const Vocabulary& getFulltextVocabulary(size_t i) const {
      return _fulltextVocabularies[i];
    }

    const Vocabulary& getOntologyVocabulary() const {
      return _ontologyVocabulary;
    }


    size_t getSizeOfEntityUniverse() const {
      return _ontologyVocabulary.size();
    }

    void setEntityUrlPrefix(const string& prefix) {
      _entityUrlPrefix = prefix;
    }

    void setEntityUrlSuffix(const string& suffix) {
      _entityUrlSuffix = suffix;
    }

    void readEntityUrlMapFromFile(const string& fileName) {
      LOG(DEBUG) << "Reading URL map from file: " << fileName << "\n";
      ad_utility::File in(fileName.c_str(), "r");
      char* buf = new char[BUFFER_SIZE_ONTOLOGY_WORD];
      in.readIntoVector(&_entityUrlMap, buf, BUFFER_SIZE_ONTOLOGY_WORD);
      delete[] buf;
      LOG(DEBUG) << "Done.\n";
    }

    Id getFirstRelId() const {
      return _firstRelId;
    }
    Id getLastRelId() const {
      return _lastRelId;
    }
    const vector<RelationPattern>& getEntityIdToRelationPatternVec() const {
      return _entityIdToRelationPattern;
    }
    const vector<ClassPattern>& getEntityIdToClassPatternVec() const {
      return _entityIdToClassPattern;
    }
    const vector<vector<Id> >& getRelationPatternToRelIdListVec() const {
      return _relationPatternToIdList;
    }
    const vector<vector<Id> >& getClassPatternToIdListVec() const {
      return _classPatternToIdList;
    }
    const vector<Id>& getClassIdToEntityIdVec() const {
      return _classIdToEntityId;
    }

  private:

    bool _initialized;
    bool _usesCompression;

    // The single registered ontology index and related members.
    mutable File _ontologyIndex;
    Vocabulary _ontologyVocabulary;
    OntologyMetaData _ontologyMetaData;

    // A list of registered full-text indexes and associated information.
    mutable vector<File> _fullTextIndexes;
    vector<Vocabulary> _fulltextVocabularies;
    vector<FulltextMetaData> _fulltextMetaData;

    mutable vector<File> _excerptFiles;

    //! Always keep in memory:
    Relation _hasRelationsRelation;
    Relation _isARelation;
    EntityList _availableClasses;
    EntityList _allEntities;
    vector<AggregatedScore> _entityScores;
    vector<Id> _contextDocumentMapping;
    Vocabulary _pseudoPrefixKeys;
    vector<Id> _pseudoPrefixValues;
    vector<RelationPattern> _entityIdToRelationPattern;
    vector<ClassPattern> _entityIdToClassPattern;
    vector<vector<Id> > _relationPatternToIdList;
    vector<vector<Id> > _classPatternToIdList;
    vector<Id> _classIdToEntityId;
    Id _firstRelId;
    Id _lastRelId;
    char* _docsFileBuffer;

    ReversedRelationNameProvider _reversedRelationProvider;

    //! Reads the has-instances relation from disc and drives an
    //! EntityList of available classes.
    //! Use the registered ontologyIndex as file.
    void readAvailableClasses(File* ontolgyIndex,
        const RelationMetaData& relMetaData,
        EntityList* availableClassesList) const;

    void readPseudoPrefixes(const string& fileName);
    void readContextDocumentMappingFromFile(const string& fileName);

    //! Read the entity scores from file.
    void readEntityScores(const string& fileName);

    mutable ad_utility::Timer _readIndexListsTimer;
    mutable ad_utility::Timer _wDecompressionTimer;
    mutable ad_utility::Timer _cDecompressionTimer;
    mutable ad_utility::Timer _sDecompressionTimer;
    mutable ad_utility::Timer _pDecompressionTimer;
    mutable ad_utility::Timer _remapRestoreTimer;
    mutable ad_utility::Timer _remapTimer;
    mutable ad_utility::Timer _restoreTimer;
    mutable ad_utility::Timer _pseudoPrefixTimer;
    mutable ad_utility::Timer _readExcerptsTimer;
    mutable ad_utility::Timer _diskTimer;

    string _entityUrlPrefix;
    string _entityUrlSuffix;
    vector<string> _entityUrlMap;

    //! Forbid copying & assignment
    Index& operator=(const Index&);
    Index(const Index& orig);
};
}

#endif  // SERVER_INDEX_H_
