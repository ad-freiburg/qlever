//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef IDTABLECONCEPTS_H
#define IDTABLECONCEPTS_H

#include "backports/concepts.h"

namespace ad_utility::detail::concepts {
template <typename T>
CPP_requires(HasAsStaticView,
             requires(T& table)(table.template asStaticView<0>()));

template <typename T>
CPP_requires(HasGetLocalVocab, requires(T& table)(table.getLocalVocab()));
}  // namespace ad_utility::detail::concepts

#endif  // IDTABLECONCEPTS_H
