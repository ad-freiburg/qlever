// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_LOCALVOCABENTRYTESTHELPERS_H
#define QLEVER_TEST_UTIL_LOCALVOCABENTRYTESTHELPERS_H

#include <memory>
#include <string>
#include <string_view>
#include <variant>

#include "./AllocatorTestHelpers.h"
#include "global/Id.h"
#include "index/LocalVocab.h"
#include "index/LocalVocabContext.h"
#include "index/LocalVocabEntry.h"
#include "index/TripleComponentConversions.h"
#include "parser/LiteralOrIri.h"
#include "util/Exception.h"

namespace ad_utility::testing {

// Create a `LocalVocabEntry` for `word` via `toIdOrLocalVocabEntry`, which is
// the only way to create a `LocalVocabEntry` that stores an IRI. Throw if
// `word` is an IRI that is encoded directly in an `Id` (see
// `LocalVocabContext::encodeAsId`), because then there is no such entry.
inline LocalVocabEntry localVocabEntry(
    ad_utility::triple_component::LiteralOrIri word,
    const LocalVocabContext& context) {
  auto result = toIdOrLocalVocabEntry(std::move(word), context);
  AD_CONTRACT_CHECK(std::holds_alternative<LocalVocabEntry>(result),
                    "The word is encoded directly in an `Id`, so it can't be "
                    "stored in a `LocalVocabEntry`");
  return std::get<LocalVocabEntry>(std::move(result));
}

// Like `localVocabEntry` above, but for an `Iri`.
inline LocalVocabEntry localVocabEntry(ad_utility::triple_component::Iri iri,
                                       const LocalVocabContext& context) {
  return localVocabEntry(
      ad_utility::triple_component::LiteralOrIri{std::move(iri)}, context);
}

// Like `localVocabEntry` above, but for the IRI `<iriref>`.
inline LocalVocabEntry iriLocalVocabEntry(std::string_view iriref,
                                          const LocalVocabContext& context) {
  return localVocabEntry(ad_utility::triple_component::Iri::fromIriref(iriref),
                         context);
}

// Like `localVocabEntry` above, but for the literal or IRI with the given
// string representation (see `LiteralOrIri::fromStringRepresentation`).
inline LocalVocabEntry localVocabEntryFromStringRepresentation(
    std::string s, const LocalVocabContext& context) {
  return localVocabEntry(
      ad_utility::triple_component::LiteralOrIri::fromStringRepresentation(
          std::move(s)),
      context);
}

// Return a function that maps a `LocalVocabEntry` to an `Id` of type
// `LocalVocabIndex`. The words are stored in a `LocalVocab` that is owned by
// the function (and shared by its copies), so the `Id`s are valid as long as
// the function lives. Equal entries are mapped to the same `Id`. This is useful
// for tests that need such `Id`s, but don't have a `LocalVocab` at hand.
//
// NOTE: The `Id` is always of type `LocalVocabIndex`, even if the word is
// contained in the vocabulary of the index (unlike for
// `getIdAndAddIfNotContained`).
inline auto makeGetLocalVocabId() {
  return [localVocab = std::make_shared<LocalVocab>(makeAllocator())](
             const LocalVocabEntry& entry) {
    return Id::makeFromLocalVocabIndex(
        localVocab->getIndexAndAddIfNotContained(entry));
  };
}

}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_LOCALVOCABENTRYTESTHELPERS_H
