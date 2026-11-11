// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_RDFTYPES_TRIPLETERM_H
#define QLEVER_SRC_RDFTYPES_TRIPLETERM_H

#include <absl/strings/str_cat.h>

#include <array>
#include <memory>
#include <string>
#include <utility>

#include "rdfTypes/Iri.h"
#include "util/Exception.h"

namespace ad_utility::triple_component {

// An RDF 1.2 triple term `<<( subject predicate object )>>` as it is parsed,
// before it gets an ID during index building. The prototype only supports
// non-nested terms whose three components are IRIs. The IRIs are stored
// unencoded, even if the parser would encode them into an `Id`: the index
// builder encodes them in one place, exactly like the IRIs of ordinary triples.
//
// Mentioning a triple term does not assert its triple: a parser that emits an
// ordinary triple with a `TripleTerm` object emits exactly that triple.
//
// The components are boxed, so that this type does not enlarge the variant of
// `TripleComponent`, which is stored for every term the parser produces. The
// box is immutable and shared between copies, which keeps copies cheap.
class TripleTerm {
 public:
  using Components = std::array<Iri, 3>;  // Subject, predicate, object.

 private:
  std::shared_ptr<const Components> components_;

 public:
  TripleTerm(Iri subject, Iri predicate, Iri object)
      : components_{std::make_shared<const Components>(Components{
            std::move(subject), std::move(predicate), std::move(object)})} {}

  const Components& components() const {
    // Only a moved-from `TripleTerm` has no components.
    AD_CORRECTNESS_CHECK(components_ != nullptr);
    return *components_;
  }
  const Iri& subject() const { return components()[0]; }
  const Iri& predicate() const { return components()[1]; }
  const Iri& object() const { return components()[2]; }

  // Equality and hashing are by value (structural identity), not by box.
  friend bool operator==(const TripleTerm& a, const TripleTerm& b) {
    return a.components() == b.components();
  }
  friend bool operator!=(const TripleTerm& a, const TripleTerm& b) {
    return !(a == b);
  }
  template <typename H>
  friend H AbslHashValue(H h, const TripleTerm& term) {
    const auto& [s, p, o] = term.components();
    return H::combine(std::move(h), s, p, o);
  }

  // The N-Triples 1.2 form, for example `<<( <s> <p> <o> )>>`.
  std::string toStringRepresentation() const {
    const auto& [s, p, o] = components();
    return absl::StrCat("<<( ", s.toStringRepresentation(), " ",
                        p.toStringRepresentation(), " ",
                        o.toStringRepresentation(), " )>>");
  }
};

}  // namespace ad_utility::triple_component

#endif  // QLEVER_SRC_RDFTYPES_TRIPLETERM_H
