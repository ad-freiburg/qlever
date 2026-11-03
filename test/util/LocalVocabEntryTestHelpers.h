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

#include <string>
#include <string_view>
#include <variant>

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

}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_LOCALVOCABENTRYTESTHELPERS_H
