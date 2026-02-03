// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_TABLEWITHRANGE_H
#define QLEVER_SRC_ENGINE_TABLEWITHRANGE_H

#include "backports/algorithm.h"
#include "engine/LocalVocab.h"
#include "engine/idTable/IdTable.h"

// Helper type that provides const ref access to the underlying `IdTable` and
// `LocalVocab` associated with the `IdTable`.
struct TableConstRefWithVocab {
  std::reference_wrapper<const IdTable> idTable_;
  std::reference_wrapper<const LocalVocab> localVocab_;

  const IdTable& idTable() const { return idTable_.get(); }

  const LocalVocab& localVocab() const { return localVocab_.get(); }
};

// Helper type that contains an `IdTable` and a view with related indices to
// access the `IdTable` with.
struct TableWithRange {
  TableConstRefWithVocab tableWithVocab_;
  ql::ranges::iota_view<uint64_t, uint64_t> view_;
};

#endif  // QLEVER_SRC_ENGINE_TABLEWITHRANGE_H
