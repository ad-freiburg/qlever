// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_TABLEWITHRANGE_H
#define QLEVER_SRC_ENGINE_TABLEWITHRANGE_H

#include "backports/algorithm.h"
#include "engine/idTable/IdTable.h"
#include "index/LocalVocab.h"

namespace qlever {

// Helper type that provides non-owning view access to the underlying `IdTable`
// and `LocalVocab` associated with the `IdTable`.
struct TableConstRefWithVocab {
  IdTableView<0> idTable_;
  std::reference_wrapper<const LocalVocab> localVocab_;

  const IdTableView<0>& idTable() const { return idTable_; }

  const LocalVocab& localVocab() const { return localVocab_.get(); }
};

// Helper type that contains an `IdTable` and a view with related indices to
// access the `IdTable` with.
struct TableWithRange {
  TableConstRefWithVocab tableWithVocab_;
  ql::ranges::iota_view<uint64_t, uint64_t> view_;
};

}  // namespace qlever

#endif  // QLEVER_SRC_ENGINE_TABLEWITHRANGE_H
