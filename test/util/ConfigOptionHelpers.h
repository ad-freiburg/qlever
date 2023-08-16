// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (August of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once
#include "util/ConfigManager/ConfigOption.h"

/*
@brief Call the function with each of the alternatives in
`ConfigOption::AvailableTypes` as template parameter.

@tparam Function The loop body should be a templated function, with one
`typename` template argument and no more. It also shouldn't take any function
arguments. Should be passed per deduction.
*/
template <typename Function>
static void doForTypeInConfigOptionValueType(Function function) {
  ad_utility::ConstexprForLoop(
      std::make_index_sequence<std::variant_size_v<ad_utility::ConfigOption::AvailableTypes>>{},
      [&function]<size_t index, typename IndexType = std::variant_alternative_t<
                                    index, ad_utility::ConfigOption::AvailableTypes>>() {
        function.template operator()<IndexType>();
      });
}
