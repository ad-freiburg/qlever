// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/ParsedTripleTerm.h"

#include "backports/three_way_comparison.h"
#include "parser/TripleComponent.h"
#include "util/Exception.h"

// ____________________________________________________________________________
struct ParsedTripleTerm::Components {
  TripleComponent subject_, predicate_, object_;

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(Components, subject_, predicate_,
                                              object_)
};

// ____________________________________________________________________________
ParsedTripleTerm::ParsedTripleTerm(TripleComponent subject,
                                   TripleComponent predicate,
                                   TripleComponent object)
    : components_{std::make_shared<const Components>(Components{
          std::move(subject), std::move(predicate), std::move(object)})} {}

// ____________________________________________________________________________
const TripleComponent& ParsedTripleTerm::subject() const {
  AD_CORRECTNESS_CHECK(components_ != nullptr);
  return components_->subject_;
}

// ____________________________________________________________________________
const TripleComponent& ParsedTripleTerm::predicate() const {
  AD_CORRECTNESS_CHECK(components_ != nullptr);
  return components_->predicate_;
}

// ____________________________________________________________________________
const TripleComponent& ParsedTripleTerm::object() const {
  AD_CORRECTNESS_CHECK(components_ != nullptr);
  return components_->object_;
}

// ____________________________________________________________________________
bool operator==(const ParsedTripleTerm& a, const ParsedTripleTerm& b) {
  // Moved-from terms compare equal only to other moved-from terms.
  if (!a.components_ || !b.components_) {
    return a.components_ == b.components_;
  }
  return *a.components_ == *b.components_;
}
