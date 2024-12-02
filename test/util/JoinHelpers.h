// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022,
// schlegea@informatik.uni-freiburg.de)

#pragma once

#include <algorithm>

#include "../test/util/IdTableHelpers.h"
#include "AllocatorTestHelpers.h"
#include "IndexTestHelpers.h"
#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/Join.h"
#include "engine/idTable/IdTable.h"
#include "util/Forward.h"
#include "util/Random.h"

/*
 * @brief Join two IdTables using the given join function and return
 * the result.
 *
 * @tparam JOIN_FUNCTION is used to allow the transfer of any type of
 *  lambda function, that could contain a join function. You never have to
 *  actually specify this parameter , just let inference do its job.
 *
 * @param tableA, tableB the tables with their join columns.
 * @param func the function, that will be used for joining the two tables
 *  together. Look into src/engine/Join.h for how it should look like.
 *
 * @returns tableA and tableB joined together in a IdTable.
 */
template <typename JOIN_FUNCTION>
IdTable useJoinFunctionOnIdTables(const IdTableAndJoinColumn& tableA,
                                  const IdTableAndJoinColumn& tableB,
                                  JOIN_FUNCTION func) {
  int resultWidth{static_cast<int>(tableA.idTable.numColumns() +
                                   tableB.idTable.numColumns() - 1)};
  IdTable result{static_cast<size_t>(resultWidth),
                 ad_utility::testing::makeAllocator()};

  // You need to use this special function for executing lambdas. The normal
  // function for functions won't work.
  // Additionally, we need to cast the two size_t, because callFixedSize only
  // takes arrays of int.
  ad_utility::callFixedSize(
      (std::array{static_cast<int>(tableA.idTable.numColumns()),
                  static_cast<int>(tableB.idTable.numColumns()), resultWidth}),
      func, tableA.idTable, tableA.joinColumn, tableB.idTable,
      tableB.joinColumn, &result);

  return result;
}

/*
 * @brief Returns a lambda for calling `Join::hashJoin` via
 *  `ad_utility::callFixedSize`.
 */
inline auto makeHashJoinLambda() {
  return []<int A, int B, int C>(auto&&... args) {
    return Join::hashJoin(AD_FWD(args)...);
  };
}

/*
 * @brief Returns a lambda for calling `Join::join` via
 *  `ad_utility::callFixedSize`.
 */
inline auto makeJoinLambda() {
  return [J = Join{Join::InvalidOnlyForTestingJoinTag{},
                   ad_utility::testing::getQec()}]<int A, int B, int C>(
             auto&&... args) { return J.join(AD_FWD(args)...); };
}
