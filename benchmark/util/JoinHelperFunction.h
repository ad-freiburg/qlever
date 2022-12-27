// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <algorithm>

#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/Join.h"
#include "util/Random.h"
#include "engine/idTable/IdTable.h"

/*
 * @brief Joins two IdTables togehter with the given join function and returns
 * the result.
 *
 * @tparam JOIN_FUNCTION is used to allow the transfer of any type of
 *  lambda function, that could contain a join function. You never have to
 *  actually specify this parameter , just let inference do its job.
 *
 * @param tableA, tableB the tables. 
 * @param jcA, jcB are the join columns for the tables.
 * @param func the function, that will be used for joining the two tables
 *  together. Look into src/engine/Join.h for how it should look like.
 *
 * @returns tableA and tableB joined together in a IdTable.
 */
template<typename JOIN_FUNCTION>
IdTable useJoinFunctionOnIdTables(
        const IdTable& tableA,
        const size_t jcA,
        const IdTable& tableB,
        const size_t jcB,
        JOIN_FUNCTION func
        ) {
  
  int reswidth = tableA.numColumns() + tableB.numColumns()  - 1;
  IdTable res(reswidth, allocator());
 
  // You need to use this special function for executing lambdas. The normal
  // function for functions won't work.
  // Additionaly, we need to cast the two size_t, because callFixedSize only takes arrays of int.
  ad_utility::callFixedSize((std::array{static_cast<int>(tableA.numColumns()), static_cast<int>(tableB.numColumns()), reswidth}), func, tableA, jcA, tableB, jcB, &res);

  return res;
}

