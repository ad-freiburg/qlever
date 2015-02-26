// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./IndexMock.h"


// _____________________________________________________________________________
void IndexMock::scanPSO(const string& predicate, IndexMock::WidthTwoList*
result) const {
//  ad_semsearch::Id relId;
//  _broccoliIndex.getIdForOntologyWord(predicate, &relId);
//  auto rmd = _broccoliIndex.getRelationMetaData(relId);
//  ad_semsearch::Relation rel;
//  _broccoliIndex.readFullRelation(rmd, &rel);
//  for (size_t i = 0; i < rel.size(); ++i) {
//    result->emplace_back(array<Id, 2>{rel[i]._lhs, rel[i]._rhs});
//  }
}

// _____________________________________________________________________________
void IndexMock::scanPSO(const string& predicate, const string& subject,
    IndexMock::WidthOneList* result) const {
//  ad_semsearch::Id relId;
//  _broccoliIndex.getIdForOntologyWord(predicate, &relId);
//  auto rmd = _broccoliIndex.getRelationMetaData(relId);
//  ad_semsearch::Relation rel;
//  _broccoliIndex.readFullRelation(rmd, &rel);
//  ad_semsearch::Id subId;
//  _broccoliIndex.getIdForOntologyWord(subject, &subId);
//  getRelationRhsBySingleLhs(rel, subId, result);
}

// _____________________________________________________________________________
void IndexMock::scanPOS(const string& predicate, IndexMock::WidthTwoList*
result) const {
//  ad_semsearch::Id relId;
//  _broccoliIndex.getIdForOntologyWord(_broccoliIndex.reverseRelation(
//      predicate), &relId);
//  auto rmd = _broccoliIndex.getRelationMetaData(relId);
//  ad_semsearch::Relation rel;
//  _broccoliIndex.readFullRelation(rmd, &rel);
//  result->reserve(rel.size());
//  for (size_t i = 0; i < rel.size(); ++i) {
//    result->emplace_back(array<Id, 2>{rel[i]._lhs, rel[i]._rhs});
//  }
}

// _____________________________________________________________________________
void IndexMock::scanPOS(const string& predicate, const string& object,
    IndexMock::WidthOneList* result) const {
//  ad_semsearch::Id relId;
//  _broccoliIndex.getIdForOntologyWord(_broccoliIndex.reverseRelation(
//      predicate), &relId);
//  auto rmd = _broccoliIndex.getRelationMetaData(relId);
//  ad_semsearch::Relation rel;
//  _broccoliIndex.readFullRelation(rmd, &rel);
//  ad_semsearch::Id obId;
//  _broccoliIndex.getIdForOntologyWord(object, &obId);
//  getRelationRhsBySingleLhs(rel, obId, result);
}

//// _____________________________________________________________________________
//void IndexMock::getRelationRhsBySingleLhs(
//    const ad_semsearch::Relation& relation, Id key,
//    IndexMock::WidthOneList* result) const {
//  LOG(DEBUG)
//    << "Accessing relation with " << relation.size() << " elements by an Id "
//        "key (" << key << ").\n";
//  AD_CHECK(result);
//  AD_CHECK_EQ(0, result->size());
//
//  auto it = std::lower_bound(relation.begin(),
//      relation.end(), ad_semsearch::RelationEntry(key, 0, 1),
//      ad_utility::CompareLhsComparator());
//
//  while (it != relation.end() && it->_lhs == key) {
//    result->emplace_back(array<Id, 1>{it->_rhs});
//    ++it;
//  }
//
//  LOG(DEBUG)
//    << "Done accessing relation. Matching right-hand-side EntityList now has "
//        << result->size() << " elements." << "\n";
//}

// _____________________________________________________________________________
//const string& IndexMock::idToString(Id id) const {
//  return _broccoliIndex.getOntologyWordById(id);
//}

// _____________________________________________________________________________
const string& IndexMock::idToString(Id id) const {
  return _dummy;
}