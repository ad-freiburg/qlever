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

template <typename E, size_t N, size_t M>
void Engine::join(const vector<array<E, N>>& a, size_t joinColumn1,
                  const vector<array<E, M>>& b, size_t joinColumn2,
                  vector<array<E, (N + M - 1)>>* result) {
  if (a.size() == 0 || b.size() == 0) {
    return;
  }
  if (joinColumn1 == 0) {
    if (joinColumn2 == 0) {
      doJoin<E, N, 0, M, 0>(a, b, result);
    } else if (joinColumn2 == 1) {
      doJoin<E, N, 0, M, 1>(a, b, result);
    } else if (M >= 3 && joinColumn2 == 2) {
      doJoin<E, N, 0, M, 2>(a, b, result);
    } else if (M >= 4 && joinColumn2 == 3) {
      doJoin<E, N, 0, M, 3>(a, b, result);
    } else if (M >= 5 && joinColumn2 == 4) {
      doJoin<E, N, 0, M, 4>(a, b, result);
    }
  } else if (joinColumn1 == 1) {
    if (joinColumn2 == 0) {
      doJoin<E, N, 1, M, 0>(a, b, result);
    } else if (joinColumn2 == 1) {
      doJoin<E, N, 1, M, 1>(a, b, result);
    } else if (joinColumn2 == 2) {
      doJoin<E, N, 1, M, 2>(a, b, result);
    } else if (joinColumn2 == 3) {
      doJoin<E, N, 1, M, 3>(a, b, result);
    } else if (joinColumn2 == 4) {
      doJoin<E, N, 1, M, 4>(a, b, result);
    }
  } else if (joinColumn1 == 2) {
    if (joinColumn2 == 0) {
      doJoin<E, N, 2, M, 0>(a, b, result);
    } else if (joinColumn2 == 1) {
      doJoin<E, N, 2, M, 1>(a, b, result);
    } else if (joinColumn2 == 2) {
      doJoin<E, N, 2, M, 2>(a, b, result);
    } else if (joinColumn2 == 3) {
      doJoin<E, N, 2, M, 3>(a, b, result);
    } else if (joinColumn2 == 4) {
      doJoin<E, N, 2, M, 4>(a, b, result);
    }
  } else if (joinColumn1 == 3) {
    if (joinColumn2 == 0) {
      doJoin<E, N, 3, M, 0>(a, b, result);
    } else if (joinColumn2 == 1) {
      doJoin<E, N, 3, M, 1>(a, b, result);
    } else if (joinColumn2 == 2) {
      doJoin<E, N, 3, M, 2>(a, b, result);
    } else if (joinColumn2 == 3) {
      doJoin<E, N, 3, M, 3>(a, b, result);
    } else if (joinColumn2 == 4) {
      doJoin<E, N, 3, M, 4>(a, b, result);
    }
  } else if (joinColumn1 == 4) {
    if (joinColumn2 == 0) {
      doJoin<E, N, 4, M, 0>(a, b, result);
    } else if (joinColumn2 == 1) {
      doJoin<E, N, 4, M, 1>(a, b, result);
    } else if (joinColumn2 == 2) {
      doJoin<E, N, 4, M, 2>(a, b, result);
    } else if (joinColumn2 == 3) {
      doJoin<E, N, 4, M, 3>(a, b, result);
    } else if (joinColumn2 == 4) {
      doJoin<E, N, 4, M, 4>(a, b, result);
    }
  } else {
    AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
             "Currently only join column indexes for 0 to 4 are supported");
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

template void Engine::join(const vector<array<Id, 1>>& a, size_t joinColumn1,
                           const vector<array<Id, 1>>& b, size_t joinColumn2,
                           vector<array<Id, 1>>* result);

template void Engine::join(const vector<array<Id, 2>>& a, size_t joinColumn1,
                           const vector<array<Id, 1>>& b, size_t joinColumn2,
                           vector<array<Id, 2>>* result);

template void Engine::join(const vector<array<Id, 1>>& a, size_t joinColumn1,
                           const vector<array<Id, 2>>& b, size_t joinColumn2,
                           vector<array<Id, 2>>* result);

template void Engine::join(const vector<array<Id, 1>>& a, size_t joinColumn1,
                           const vector<array<Id, 3>>& b, size_t joinColumn2,
                           vector<array<Id, 3>>* result);

template void Engine::join(const vector<array<Id, 3>>& a, size_t joinColumn1,
                           const vector<array<Id, 1>>& b, size_t joinColumn2,
                           vector<array<Id, 3>>* result);

template void Engine::join(const vector<array<Id, 2>>& a, size_t joinColumn1,
                           const vector<array<Id, 2>>& b, size_t joinColumn2,
                           vector<array<Id, 3>>* result);

template void Engine::join(const vector<array<Id, 4>>& a, size_t joinColumn1,
                           const vector<array<Id, 1>>& b, size_t joinColumn2,
                           vector<array<Id, 4>>* result);

template void Engine::join(const vector<array<Id, 1>>& a, size_t joinColumn1,
                           const vector<array<Id, 4>>& b, size_t joinColumn2,
                           vector<array<Id, 4>>* result);

template void Engine::join(const vector<array<Id, 3>>& a, size_t joinColumn1,
                           const vector<array<Id, 2>>& b, size_t joinColumn2,
                           vector<array<Id, 4>>* result);

template void Engine::join(const vector<array<Id, 2>>& a, size_t joinColumn1,
                           const vector<array<Id, 3>>& b, size_t joinColumn2,
                           vector<array<Id, 4>>* result);

template void Engine::join(const vector<array<Id, 3>>& a, size_t joinColumn1,
                           const vector<array<Id, 3>>& b, size_t joinColumn2,
                           vector<array<Id, 5>>* result);

template void Engine::join(const vector<array<Id, 5>>& a, size_t joinColumn1,
                           const vector<array<Id, 1>>& b, size_t joinColumn2,
                           vector<array<Id, 5>>* result);

template void Engine::join(const vector<array<Id, 1>>& a, size_t joinColumn1,
                           const vector<array<Id, 5>>& b, size_t joinColumn2,
                           vector<array<Id, 5>>* result);

template void Engine::join(const vector<array<Id, 2>>& a, size_t joinColumn1,
                           const vector<array<Id, 4>>& b, size_t joinColumn2,
                           vector<array<Id, 5>>* result);

template void Engine::join(const vector<array<Id, 4>>& a, size_t joinColumn1,
                           const vector<array<Id, 2>>& b, size_t joinColumn2,
                           vector<array<Id, 5>>* result);
