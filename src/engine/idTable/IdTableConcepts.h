//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_IDTABLE_IDTABLECONCEPTS_H
#define QLEVER_SRC_ENGINE_IDTABLE_IDTABLECONCEPTS_H

#include "backports/concepts.h"

// Concepts related to `IdTable` wrappers that are being used for join
// operations.
namespace ad_utility::detail {

namespace concepts {

// This concept is being used to acquire a uniform view type for the wrapper. If
// no such function exists, on the type it is assumed that T already is the
// correct view type.
template <typename T>
CPP_requires(HasAsStaticView,
             requires(T& table)(table.template asStaticView<0>()));

// This concept is being used for wrappers that might also hold a `LocalVocab`
// to ensure they are properly merged during joins.
template <typename T>
CPP_requires(HasGetLocalVocab, requires(T& table)(table.getLocalVocab()));
}  // namespace concepts

// Unwrap type `T` to get an `IdTableView<0>`, even if it's not an
// `IdTableView<0>`. Identity for `IdTableView<0>`.
template <typename T>
static IdTableView<0> toView(const T& table) {
  if constexpr (CPP_requires_ref(concepts::HasAsStaticView, T)) {
    return table.template asStaticView<0>();
  } else {
    return table;
  }
}

// Merge the local vocab contained in `T` with the `targetVocab` and set the
// passed pointer reference to that vocab.
template <typename T>
static void mergeVocabInto(const T& table, const LocalVocab*& currentVocab,
                           LocalVocab& targetVocab) {
  AD_CORRECTNESS_CHECK(currentVocab == nullptr);
  if constexpr (CPP_requires_ref(detail::concepts::HasGetLocalVocab, T)) {
    currentVocab = &table.getLocalVocab();
    targetVocab.mergeWith(table.getLocalVocab());
  }
}
}  // namespace ad_utility::detail

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDTABLECONCEPTS_H
