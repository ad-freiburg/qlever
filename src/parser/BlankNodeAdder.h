// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_BLANKNODEADDER_H
#define QLEVER_SRC_PARSER_BLANKNODEADDER_H

#include <string>
#include <string_view>

#include "global/Id.h"
#include "index/LocalVocab.h"
#include "parser/TripleComponent.h"
#include "util/BlankNodeManager.h"
#include "util/HashMap.h"

// Consistently maps blank node labels (like `_:b0`) to blank node `Id`s. All
// occurrences of the same label that are passed to the same `BlankNodeAdder`
// yield the same `Id`, and labels that are passed to different
// `BlankNodeAdder`s always yield different `Id`s. Used by all the places that
// turn blank nodes from a query, an update, or an RDF document into `Id`s at
// query time. NOTE: The index builder deliberately does not use this class, as
// it assigns its own dense range of blank node indices, see
// `VocabularyMerger::getNextBlankNodeIndex`.
struct BlankNodeAdder {
  // The used blank node IDs are stored in the `LocalVocab` via the
  // `LocalBlankNodeManager`.
  LocalVocab localVocab_;
  // Store the mapping from labels to IDs.
  ad_utility::HashMap<std::string, Id> map_;
  // The (global) blank node manager used to obtain new unique blank node IDs.
  ad_utility::BlankNodeManager* bnodeManager_;

  // Get an `Id` for the `label`. If the same `label` was previously passed to
  // the same `BlankNodeAdder`, this will result in the same `Id`.
  Id getBlankNodeIndex(std::string_view label);

  // Resolve a `TripleComponent` that was produced by one of the RDF parsers
  // (see `RdfParser.h`). Those represent blank nodes as plain strings (all
  // other components are strongly typed), so a `TripleComponent` that holds a
  // string is converted via `getBlankNodeIndex`, and all others are passed
  // through unchanged. Every consumer of parsed triples has to call this (or
  // handle blank nodes itself), because the conversions in
  // `TripleComponentConversions.h` reject strings.
  TripleComponent resolveParsedComponent(TripleComponent&& tripleComponent);
};

#endif  // QLEVER_SRC_PARSER_BLANKNODEADDER_H
