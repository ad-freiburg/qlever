// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_BINARYEXPORT_BLANKNODEREWRITER_H
#define QLEVER_SRC_ENGINE_BINARYEXPORT_BLANKNODEREWRITER_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include "global/Id.h"
#include "index/LocalVocab.h"
#include "util/BlankNodeManager.h"
#include "util/HashMap.h"

namespace qlever::binary_export {

// Replace the blank nodes of the exporting instance, which sends their IDs as
// they are (see `toExportableId` in `BinaryExport.cpp`), by local ones. The
// same remote blank node is always replaced by the same local one.
class BlankNodeRewriter {
  ad_utility::BlankNodeManager* blankNodeManager_;
  ad_utility::HashMap<Id::T, Id> mapping_;
  // The local blank nodes are allocated in this local vocab, which only
  // contains blank nodes (see `addBlankNodesTo`).
  LocalVocab blankNodes_;

 public:
  explicit BlankNodeRewriter(ad_utility::BlankNodeManager* blankNodeManager)
      : blankNodeManager_{blankNodeManager} {}

  // Return the local blank node for the remote blank node `id`.
  Id operator()(Id id);

  // Make all the local blank nodes so far part of the `localVocab`. This
  // includes the blank nodes of previous batches, as they might occur again.
  // As `blankNodes_` contains no words, the batches don't keep each other's
  // words alive.
  void addBlankNodesTo(LocalVocab& localVocab);
};

}  // namespace qlever::binary_export

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_ENGINE_BINARYEXPORT_BLANKNODEREWRITER_H
