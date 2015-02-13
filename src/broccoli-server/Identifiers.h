// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#ifndef SERVER_IDENTIFIERS_H_
#define SERVER_IDENTIFIERS_H_

#include "./Globals.h"
#include "./ListIdProvider.h"

namespace ad_semsearch {
//! Available types of IDs. The semantic search processes lists of
//! arbitrary IDs. IDs are represented as 64bit integers.
//! However, sometimes ontology elements (i.e. entities) have to
//! be distinguished from words although they occur in the same list.
//! Therefore the most significant bits are reserved as type-flags.
class IdType {
  public:
    enum TYPE {
      WORD_ID, ONTOLOGY_ELEMENT_ID, CONTEXT_ID, DOCUMENT_ID
    };
};

//! Gets the Id "0". In fact, numbers greater than 0 may be returned
//! because the most significant bits represent the type of the ID.
inline Id getFirstId(IdType::TYPE type) {
  switch (type) {
  case IdType::WORD_ID:
    return 0;
  case IdType::DOCUMENT_ID:
    return 0;
  case IdType::ONTOLOGY_ELEMENT_ID:
    return (Id(1) << ((sizeof(Id) * 8) - 1));
  case IdType::CONTEXT_ID:
    // Currently, context IDs can always be distinguished from
    // other IDs from the context they occur in. May be changed in
    // the future, though;
    return 0;
  default:
    return 0;
  }
}

/// /! Check whether a given id is of the specified type.
// inline bool isIdOfType(Id id, IdType::TYPE type)
// {
//  switch (type)
//  {
//  case IdType::WORD_ID:
//    return (id & (Id(1) << ((sizeof(Id) * 8) - 1))) == 0;
//  case IdType::ONTOLOGY_ELEMENT_ID:
//    return (id & (Id(1) << ((sizeof(Id) * 8) - 1))) > 0;
//  case IdType::CONTEXT_ID:
//    return (id & (Id(1) << ((sizeof(Id) * 8) - 1))) == 0;
//  default:
//    return false;
//  }
// }

static const Id FIRST_ENTITY_ID = getFirstId(IdType::ONTOLOGY_ELEMENT_ID);
static const Id FIRST_WORD_ID = getFirstId(IdType::WORD_ID);

template<IdType::TYPE idType>
inline bool isIdOfType(Id id) {
  if (idType == IdType::WORD_ID) {
    return id < FIRST_ENTITY_ID;
  }

  if (idType == IdType::DOCUMENT_ID) {
    return true;
  }

  if (idType == IdType::ONTOLOGY_ELEMENT_ID) {
    return id >= FIRST_ENTITY_ID;
  }

  if (idType == IdType::CONTEXT_ID) {
    return true;
  }

  return false;
}

static const Id PURE_VALUE_MASK = ~(Id(1) << ((sizeof(Id) * 8) - 1));
//! Get the pure value of the Id without any flag bits set.
inline Id getPureValue(Id id) {
  return id & PURE_VALUE_MASK;
}
}
#endif  // SERVER_IDENTIFIERS_H_
