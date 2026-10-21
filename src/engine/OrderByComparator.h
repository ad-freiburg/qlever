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
#include <utility>

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
// term: unbound < blank nodes < IRIs < literals. Within one kind, numbers
// (`Int` and `Double`) are compared exactly (see `compareIntAndDouble`), with
// all NaNs equal to each other and after all other numbers. All other `Id`s
// with compatible datatypes are compared by `compareIds`, and `Id`s with
// incompatible datatypes by their datatype (which e.g. puts numbers before
// strings, the standard leaves that open).
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
  // See `LocalVocabContext::getRangeOfIrisInVocabulary`.
  std::pair<VocabIndex, VocabIndex> rangeOfIrisInVocabulary_;

 public:
  explicit OrderByComparator(const LocalVocabContext& context)
      : rangeOfIrisInVocabulary_{context.getRangeOfIrisInVocabulary()} {}

  // Return the kind of the RDF term that `id` represents.
  TermKind getTermKind(Id id) const {
    switch (id.getDatatype()) {
      case Datatype::Undefined:
        return TermKind::Undefined;
      case Datatype::BlankNodeIndex:
        return TermKind::BlankNode;
      case Datatype::VocabIndex: {
        // Outside of the range of the IRIs are the literals, in particular
        // also the WKT literals of a geo-split vocabulary, whose indices lie
        // above all other indices (see `Vocabulary::getRangeOfIris`).
        auto [begin, end] = rangeOfIrisInVocabulary_;
        VocabIndex index = id.getVocabIndex();
        return begin <= index && index < end ? TermKind::Iri
                                             : TermKind::Literal;
      }
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
    return compareWithinKind(a, b);
  }

  // Return true iff `a` comes before `b`.
  bool operator()(Id a, Id b) const { return compare(a, b) < 0; }

 private:
  // The order within one kind of RDF terms, see the class comment.
  static int compareWithinKind(Id a, Id b) {
    Datatype typeA = a.getDatatype();
    Datatype typeB = b.getDatatype();
    auto isNumeric = [](Datatype type) {
      return type == Datatype::Int || type == Datatype::Double;
    };
    if (isNumeric(typeA) && isNumeric(typeB)) {
      return compareNumbers(a, b);
    }
    // Two `Id`s of the same datatype are ordered by their bits, which is what
    // `compareIds` does for them (they hold an index, a date, or a geo point),
    // except for `LocalVocabIndex` (the words have to be compared) and `Bool`
    // (the bits also store whether it was written as `0` or `1`, see
    // `ValueId::makeBoolFromZeroOrOne`). Compare the bits directly,
    // `compareIds` would need two calls to tell `>` from `==`.
    if (typeA == typeB && typeA != Datatype::LocalVocabIndex &&
        typeA != Datatype::Bool) {
      return threeWay(a.getBits(), b.getBits());
    }
    using namespace valueIdComparators;
    auto isLessThan = compareIds(a, b, Comparison::LT);
    // `Undef` means that the datatypes are incompatible.
    if (isLessThan == ComparisonResult::Undef) {
      return threeWay(typeA, typeB);
    }
    if (isLessThan == ComparisonResult::True) {
      return -1;
    }
    return toBoolNotUndef(compareIds(b, a, Comparison::LT)) ? 1 : 0;
  }

  // Return -1, 0, or 1 if `x` is less than, equal to, or greater than `y`.
  template <typename T>
  static int threeWay(T x, T y) {
    return (x > y) - (x < y);
  }

  // Compare two `Id`s of type `Int` or `Double`.
  static int compareNumbers(Id a, Id b) {
    auto isNan = [](Id id) {
      return id.getDatatype() == Datatype::Double && std::isnan(id.getDouble());
    };
    bool aIsNan = isNan(a);
    bool bIsNan = isNan(b);
    if (aIsNan || bIsNan) {
      return static_cast<int>(aIsNan) - static_cast<int>(bIsNan);
    }
    bool aIsInt = a.getDatatype() == Datatype::Int;
    bool bIsInt = b.getDatatype() == Datatype::Int;
    if (aIsInt && bIsInt) {
      return threeWay(a.getInt(), b.getInt());
    }
    if (aIsInt) {
      return ad_utility::compareIntAndDouble(a.getInt(), b.getDouble());
    }
    if (bIsInt) {
      return -ad_utility::compareIntAndDouble(b.getInt(), a.getDouble());
    }
    return threeWay(a.getDouble(), b.getDouble());
  }
};

#endif  // QLEVER_SRC_ENGINE_ORDERBYCOMPARATOR_H
