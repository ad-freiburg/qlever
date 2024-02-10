// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2023,
// schlegea@informatik.uni-freiburg.de)

#include "../test/util/BenchmarkMeasurementContainerHelpers.h"

#include "util/TypeTraits.h"

using namespace std::string_literals;

// ____________________________________________________________________________
template <
    ad_utility::SimilarToAnyTypeIn<ad_benchmark::ResultTable::EntryType> Type>
Type createDummyValueEntryType() {
  if constexpr (ad_utility::isSimilar<Type, float>) {
    return 4.2f;
  } else if constexpr (ad_utility::isSimilar<Type, std::string>) {
    return "test"s;
  } else if constexpr (ad_utility::isSimilar<Type, bool>) {
    return true;
  } else if constexpr (ad_utility::isSimilar<Type, size_t>) {
    return 17361644613946UL;
  } else if constexpr (ad_utility::isSimilar<Type, int>) {
    return -42;
  } else {
    // Not a supported type.
    AD_FAIL();
  }
}

/*
Explicit instantiation for all types in `ad_benchmark::ResultTable::EntryType`.
*/
template bool createDummyValueEntryType<bool>();
template std::string createDummyValueEntryType<std::string>();
template int createDummyValueEntryType<int>();
template size_t createDummyValueEntryType<size_t>();
template float createDummyValueEntryType<float>();
