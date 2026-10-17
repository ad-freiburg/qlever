// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_ORDERBYCOMPARATOR_H
#define QLEVER_SRC_ENGINE_ORDERBYCOMPARATOR_H

#include <cmath>
#include <cstdint>

#include "backports/StartsWithAndEndsWith.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "global/ValueIdComparators.h"
#include "index/LocalVocabContext.h"
#include "index/LocalVocabEntry.h"
#include "util/ComparisonOfIntAndDouble.h"
#include "util/Exception.h"

// The order of `ORDER BY` (https://www.w3.org/TR/sparql11-query/#modOrderBy),
// which the standard also uses for `MIN` and `MAX`. First by the kind of RDF
// term: unbound < blank nodes < IRIs < literals, then within one kind by
// `compareIds` in the `CompareByType` mode (which e.g. puts numbers before
// strings, the standard leaves that open), except that an `Int` and a `Double`
// are compared exactly (see `compareIntAndDouble`).
//
// NOTE: Encoded IRIs (`EncodedVal`) are ordered by their encoding (#2448),
// which can even break the strict weak ordering, see
// `valueIdComparators::detail::compareIdsImpl`. With a secondary vocabulary,
// the order is not by string value, and an `Id` of type `SecondaryVocabIndex`
// always counts as an IRI (its word is not at hand).
class OrderByComparator {
 public:
  // The kinds of RDF terms, in the order of `ORDER BY`.
  enum struct TermKind : uint8_t { Undefined, BlankNode, Iri, Literal };

 private:
  // See `LocalVocabContext::getBeginOfIrisInVocabulary`.
  VocabIndex beginOfIrisInVocabulary_;

 public:
  explicit OrderByComparator(const LocalVocabContext& context)
      : beginOfIrisInVocabulary_{context.getBeginOfIrisInVocabulary()} {}

  // Return the kind of the RDF term that `id` represents.
  TermKind getTermKind(Id id) const {
    switch (id.getDatatype()) {
      case Datatype::Undefined:
        return TermKind::Undefined;
      case Datatype::BlankNodeIndex:
        return TermKind::BlankNode;
      case Datatype::VocabIndex:
        return id.getVocabIndex() < beginOfIrisInVocabulary_ ? TermKind::Literal
                                                             : TermKind::Iri;
      case Datatype::LocalVocabIndex: {
        const LocalVocabEntry& entry = *id.getLocalVocabIndex();
        if (entry.isLiteral()) {
          return TermKind::Literal;
        }
        // `BNODE` with an argument creates blank nodes as IRIs with this
        // prefix, see `BlankNodeExpression.cpp`.
        return ql::starts_with(entry.toStringRepresentation(),
                               QLEVER_INTERNAL_BLANK_NODE_IRI_PREFIX)
                   ? TermKind::BlankNode
                   : TermKind::Iri;
      }
      case Datatype::EncodedVal:
      case Datatype::SecondaryVocabIndex:
        return TermKind::Iri;
      case Datatype::Bool:
      case Datatype::Int:
      case Datatype::Double:
      case Datatype::Date:
      case Datatype::GeoPoint:
      case Datatype::TextRecordIndex:
      case Datatype::WordVocabIndex:
        return TermKind::Literal;
    }
    // NOTE: Listing all datatypes above (instead of a `default`) lets the
    // compiler warn about new ones.
    AD_FAIL();
  }

  // Return a negative number if `a` comes before `b`, a positive number if `b`
  // comes before `a`, and zero otherwise.
  int compare(Id a, Id b) const {
    if (a == b) {
      return 0;
    }
    auto kindA = getTermKind(a);
    auto kindB = getTermKind(b);
    if (kindA != kindB) {
      return kindA < kindB ? -1 : 1;
    }
    // Compare an `Int` and a `Double` exactly. A NaN is left to `compareIds`,
    // which puts it after all numbers.
    Datatype typeA = a.getDatatype();
    Datatype typeB = b.getDatatype();
    if (typeA == Datatype::Int && typeB == Datatype::Double &&
        !std::isnan(b.getDouble())) {
      return ad_utility::compareIntAndDouble(a.getInt(), b.getDouble());
    }
    if (typeA == Datatype::Double && typeB == Datatype::Int &&
        !std::isnan(a.getDouble())) {
      return -ad_utility::compareIntAndDouble(b.getInt(), a.getDouble());
    }
    if (isLessThan(a, b)) {
      return -1;
    }
    return isLessThan(b, a) ? 1 : 0;
  }

  // Return true iff `a` comes before `b`.
  bool operator()(Id a, Id b) const { return compare(a, b) < 0; }

 private:
  // The order within one kind of RDF terms.
  static bool isLessThan(Id a, Id b) {
    using namespace valueIdComparators;
    return toBoolNotUndef(
        compareIds<ComparisonForIncompatibleTypes::CompareByType>(
            a, b, Comparison::LT));
  }
};

#endif  // QLEVER_SRC_ENGINE_ORDERBYCOMPARATOR_H
