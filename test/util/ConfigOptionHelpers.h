// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (August of 2023, schlegea@informatik.uni-freiburg.de)

#ifndef QLEVER_TEST_UTIL_CONFIGOPTIONHELPERS_H
#define QLEVER_TEST_UTIL_CONFIGOPTIONHELPERS_H

#include "util/ConfigManager/ConfigOption.h"
#include "util/ConstexprUtils.h"

/*
@brief Call the function with each of the alternatives in
`ConfigOption::AvailableTypes` as template parameter.

@tparam Function The loop body should be a templated function, with one
`typename` template argument and no more. It also shouldn't take any function
arguments. Should be passed per deduction.
*/
template <typename Function>
static void doForTypeInConfigOptionValueType(Function function) {
  ad_utility::forEachTypeInTemplateType<
      ad_utility::ConfigOption::AvailableTypes>(function);
}

#endif  // QLEVER_TEST_UTIL_CONFIGOPTIONHELPERS_H
