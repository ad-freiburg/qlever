// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_ENGINE_IDTABLE_IDCOLUMNTESTHELPERS_H
#define QLEVER_TEST_ENGINE_IDTABLE_IDCOLUMNTESTHELPERS_H

#include <vector>

#include "../../../util/AllocatorTestHelpers.h"
#include "global/Id.h"
#include "util/AllocatorWithLimit.h"
#include "util/UninitializedAllocator.h"

namespace columnBasedIdTable::splitLayout::testHelpers {

using TestAllocator =
    ad_utility::default_init_allocator<Id, ad_utility::AllocatorWithLimit<Id>>;

inline TestAllocator testAllocator() {
  return TestAllocator{ad_utility::testing::makeAllocator()};
}

// A representative sample of `Id`s, covering all the datatypes for which the
// bit representation is not just a pointer.
inline std::vector<Id> sampleIds() {
  return {Id::makeUndefined(),
          Id::makeFromBool(true),
          Id::makeFromBool(false),
          Id::makeFromInt(42),
          Id::makeFromInt(-42),
          Id::makeFromDouble(13.37),
          Id::makeFromVocabIndex(VocabIndex::make(123)),
          Id::makeFromBlankNodeIndex(BlankNodeIndex::make(7))};
}

}  // namespace columnBasedIdTable::splitLayout::testHelpers

#endif  // QLEVER_TEST_ENGINE_IDTABLE_IDCOLUMNTESTHELPERS_H
