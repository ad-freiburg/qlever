// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include "engine/binaryExport/BlankNodeRewriter.h"

namespace qlever::binary_export {

// _____________________________________________________________________________
Id BlankNodeRewriter::operator()(Id id) {
  auto [it, inserted] = mapping_.try_emplace(id.getBits(), Id{});
  if (inserted) {
    it->second = Id::makeFromBlankNodeIndex(
        blankNodes_.getBlankNodeIndex(blankNodeManager_));
  }
  return it->second;
}

// _____________________________________________________________________________
void BlankNodeRewriter::addBlankNodesTo(LocalVocab& localVocab) {
  // NOTE: `mergeWith` marks a merged vocab as copied, which forbids adding
  // words to it, but only if the vocab contains words. The `blankNodes_`
  // contain none, so only their blank nodes are shared, and later batches can
  // still add blank nodes to them.
  localVocab.mergeWith(blankNodes_);
}

}  // namespace qlever::binary_export

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
