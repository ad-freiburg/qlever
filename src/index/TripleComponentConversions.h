// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_TRIPLECOMPONENTCONVERSIONS_H
#define QLEVER_SRC_INDEX_TRIPLECOMPONENTCONVERSIONS_H

#include <optional>
#include <string>
#include <utility>
#include <variant>

#include "global/Id.h"
#include "global/VocabIndex.h"
#include "index/EncodedIriManager.h"
#include "index/LocalVocab.h"
#include "parser/TripleComponent.h"

class IndexImpl;

// The conversions of a `TripleComponent` that require an index. They are free
// functions in this library and not member functions of `TripleComponent`,
// because `TripleComponent` is a purely syntactic type from the parser that
// must not depend on the `index` library. They are in the global namespace,
// just like `TripleComponent` itself, so that they can be called unqualified.

// Convert `tripleComponent` to an `Id` if it is not a string. In case of a
// string return `std::nullopt`. This is used by `toValueId` below and during
// the index building, when the vocabulary hasn't been built yet.
[[nodiscard]] std::optional<Id> toValueIdIfNotString(
    const TripleComponent& tripleComponent,
    const EncodedIriManager* encodedIriManager);

// Convert `tripleComponent` to an `Id`. If it is a literal or IRI, resolve it
// using the vocabulary of `index`. If it is not found there, return the
// positions of the two neighboring entries.
[[nodiscard]] std::variant<Id, std::pair<VocabIndex, VocabIndex>>
toValueIdOrBounds(const TripleComponent& tripleComponent,
                  const IndexImpl& index);

// Like `toValueIdOrBounds`, but return `std::nullopt` if not found.
[[nodiscard]] std::optional<Id> toValueId(
    const TripleComponent& tripleComponent, const IndexImpl& index);

// Like `toValueIdOrBounds`, but also take the given `localVocab` into account.
// If `tripleComponent` is found neither in the vocabulary of `index` nor in
// `localVocab`, it is added to `localVocab`. That way, we always get a valid
// `Id`.
//
// NOTE: `tripleComponent` is taken by rvalue reference because at the call
// sites it is created solely for this call, and we want to avoid copying the
// literal or IRI when passing it to the local vocabulary.
[[nodiscard]] Id toValueId(TripleComponent&& tripleComponent,
                           const IndexImpl& index, LocalVocab& localVocab);

// Convert `tripleComponent` to an RDF literal, where an `int64_t` becomes an
// `xsd:integer` literal and a `double` becomes an `xsd:double` literal.
// TODO<joka921> This function is used in only few places and ignores the strong
// typing of `Literal`s etc. It should be removed and its calls be replaced by
// calls that work on the strongly typed `TripleComponent` directly.
[[nodiscard]] std::string toRdfLiteral(const TripleComponent& tripleComponent);

#endif  // QLEVER_SRC_INDEX_TRIPLECOMPONENTCONVERSIONS_H
