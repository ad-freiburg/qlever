// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/BlankNodeAdder.h"

#include "backports/StartsWithAndEndsWith.h"

// _____________________________________________________________________________
Id BlankNodeAdder::getBlankNodeIndex(std::string_view label) {
  AD_CORRECTNESS_CHECK(ql::starts_with(label, "_:"));
  return getBlankNodeIndexForLabelWithoutPrefix(label.substr(2));
}

// _____________________________________________________________________________
Id BlankNodeAdder::getBlankNodeIndexForLabelWithoutPrefix(
    std::string_view label) {
  // NOTE: `ql::unordered::try_emplace` materializes the `label` only if the
  // `map_` doesn't support heterogeneous lookups.
  auto [it, isNew] =
      ql::unordered::try_emplace(map_, label, Id::makeUndefined());
  auto& id = it->second;
  if (isNew) {
    id = Id::makeFromBlankNodeIndex(
        localVocab_.getBlankNodeIndex(bnodeManager_));
  }
  return id;
}

// _____________________________________________________________________________
TripleComponent BlankNodeAdder::resolveParsedComponent(
    TripleComponent&& tripleComponent) {
  if (tripleComponent.isString()) {
    return getBlankNodeIndex(tripleComponent.getString());
  }
  return std::move(tripleComponent);
}
