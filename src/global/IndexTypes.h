//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_INDEXTYPES_H
#define QLEVER_INDEXTYPES_H

#include "./TypedIndex.h"
#include "parser/LiteralOrIri.h"

// Typedefs for several kinds of typed indices that are used across QLever.

// TODO<joka921> Rename `VocabIndex` to `RdfVocabIndex` at a point in time where
// this (very intrusive) renaming doesn't interfere with too many open pull
// requests.
using VocabIndex = ad_utility::TypedIndex<uint64_t, "VocabIndex">;

using LocalVocabIndex = const ad_utility::triple_component::LiteralOrIri*;
using TextRecordIndex = ad_utility::TypedIndex<uint64_t, "TextRecordIndex">;
using WordVocabIndex = ad_utility::TypedIndex<uint64_t, "WordVocabIndex">;
using BlankNodeIndex = ad_utility::TypedIndex<uint64_t, "BlankNodeIndex">;

#endif  // QLEVER_INDEXTYPES_H
