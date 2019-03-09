// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./Engine.h"
#include <algorithm>
#include "../util/Exception.h"

// _____________________________________________________________________________
template <typename E, size_t N>
vector<array<E, N>> Engine::filterRelationWithSingleId(
    const vector<array<E, N>>& relation, E entityId, size_t checkColumn) {
  // AD_CHECK_LT(checkColumn, N);
  switch (checkColumn) {
    case 0:
      return doFilterRelationWithSingleId<E, N, 0>(relation, entityId);
    case 1:
      return doFilterRelationWithSingleId<E, N, 1>(relation, entityId);
    case 2:
      return doFilterRelationWithSingleId<E, N, 2>(relation, entityId);
    case 3:
      return doFilterRelationWithSingleId<E, N, 3>(relation, entityId);
    case 4:
      return doFilterRelationWithSingleId<E, N, 4>(relation, entityId);
    case 5:
      return doFilterRelationWithSingleId<E, N, 5>(relation, entityId);
    case 6:
      return doFilterRelationWithSingleId<E, N, 6>(relation, entityId);
    case 7:
      return doFilterRelationWithSingleId<E, N, 7>(relation, entityId);
    case 8:
      return doFilterRelationWithSingleId<E, N, 8>(relation, entityId);
    case 9:
      return doFilterRelationWithSingleId<E, N, 9>(relation, entityId);
    case 10:
      return doFilterRelationWithSingleId<E, N, 10>(relation, entityId);
    default:
      AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
               "Not supporting a filter column with index > 10 at the moment");
  }
}

template vector<array<Id, 2>> Engine::filterRelationWithSingleId(
    const vector<array<Id, 2>>& relation, Id entityId, size_t checkColumn);

template vector<array<Id, 3>> Engine::filterRelationWithSingleId(
    const vector<array<Id, 3>>& relation, Id entityId, size_t checkColumn);

template vector<array<Id, 4>> Engine::filterRelationWithSingleId(
    const vector<array<Id, 4>>& relation, Id entityId, size_t checkColumn);

template vector<array<Id, 5>> Engine::filterRelationWithSingleId(
    const vector<array<Id, 5>>& relation, Id entityId, size_t checkColumn);

template vector<array<Id, 6>> Engine::filterRelationWithSingleId(
    const vector<array<Id, 6>>& relation, Id entityId, size_t checkColumn);

template vector<array<Id, 7>> Engine::filterRelationWithSingleId(
    const vector<array<Id, 7>>& relation, Id entityId, size_t checkColumn);

template vector<array<Id, 8>> Engine::filterRelationWithSingleId(
    const vector<array<Id, 8>>& relation, Id entityId, size_t checkColumn);

template vector<array<Id, 9>> Engine::filterRelationWithSingleId(
    const vector<array<Id, 9>>& relation, Id entityId, size_t checkColumn);

template vector<array<Id, 10>> Engine::filterRelationWithSingleId(
    const vector<array<Id, 10>>& relation, Id entityId, size_t checkColumn);
