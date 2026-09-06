// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
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
  auto [it, isNew] = map_.try_emplace(label.substr(2), Id::makeUndefined());
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
