// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_IDTABLEORSHAREDIDTABLEVIEW_H
#define QLEVER_SRC_ENGINE_IDTABLE_IDTABLEORSHAREDIDTABLEVIEW_H

#include <memory>
#include <variant>

#include "engine/idTable/IdTable.h"
#include "util/Exception.h"

namespace ad_utility {

// A table of `Id`s that either owns its rows (as an `IdTable`), or is a
// non-owning view of rows that are owned elsewhere, together with a
// `shared_ptr` that keeps that owner alive for as long as this object lives.
// The latter allows passing on a range of rows of a larger table without
// copying them, even if the rows are consumed asynchronously.
class IdTableOrSharedIdTableView {
 public:
  // A type-erased owner of the rows of a non-owning table.
  using Owner = std::shared_ptr<const void>;

 private:
  // A view together with the owner of the rows that it points to.
  struct SharedView {
    IdTableView<0> view_;
    Owner owner_;
  };
  std::variant<IdTable, SharedView> data_;

 public:
  // Construct an owning table.
  IdTableOrSharedIdTableView(IdTable table) : data_{std::move(table)} {}

  // Construct a non-owning table. The `owner` has to keep the memory that the
  // `view` points to alive.
  IdTableOrSharedIdTableView(IdTableView<0> view, Owner owner)
      : data_{SharedView{std::move(view), std::move(owner)}} {}

  // Return true iff this object owns its rows.
  bool ownsRows() const { return std::holds_alternative<IdTable>(data_); }

  // Return a view of the rows. The view is valid for as long as this object
  // lives. Moving this object doesn't invalidate the view either, because the
  // rows of an `IdTable` are stored on the heap.
  IdTableView<0> view() const {
    if (ownsRows()) {
      return std::get<IdTable>(data_).asStaticView<0>();
    }
    return std::get<SharedView>(data_).view_;
  }

  // Return the owned rows. May only be called if `ownsRows()` is true.
  IdTable extractTable() && {
    AD_CONTRACT_CHECK(ownsRows());
    return std::move(std::get<IdTable>(data_));
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDTABLEORSHAREDIDTABLEVIEW_H
