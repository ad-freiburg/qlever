// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
#include "PropertyPath.h"

// _____________________________________________________________________________
PropertyPath::PropertyPath(Operation op, std::string iri,
                           std::initializer_list<PropertyPath> children)
    : operation_(op), iri_(std::move(iri)), children_(children) {}

// _____________________________________________________________________________
PropertyPath PropertyPath::makeModified(PropertyPath child,
                                        std::string_view modifier) {
  if (modifier == "+") {
    return makeWithChildren({std::move(child)}, Operation::ONE_OR_MORE);
  } else if (modifier == "*") {
    return makeWithChildren({std::move(child)}, Operation::ZERO_OR_MORE);
  } else {
    AD_CORRECTNESS_CHECK(modifier == "?");
    return makeWithChildren({std::move(child)}, Operation::ZERO_OR_ONE);
  }
}

// _____________________________________________________________________________
void PropertyPath::writeToStream(std::ostream& out) const {
  switch (operation_) {
    case Operation::ALTERNATIVE:
      out << "(";
      if (!children_.empty()) {
        children_[0].writeToStream(out);
      } else {
        out << "missing" << std::endl;
      }
      out << ")|(";
      if (children_.size() > 1) {
        children_[1].writeToStream(out);
      } else {
        out << "missing" << std::endl;
      }
      out << ")";
      break;
    case Operation::INVERSE:
      out << "^(";
      if (!children_.empty()) {
        children_[0].writeToStream(out);
      } else {
        out << "missing" << std::endl;
      }
      out << ")";
      break;
    case Operation::NEGATED: {
      out << "!(";
      bool firstRun = true;
      for (const auto& child : children_) {
        if (!firstRun) {
          out << '|';
        }
        firstRun = false;
        child.writeToStream(out);
      }
      out << ")";
    } break;
    case Operation::IRI:
      out << iri_;
      break;
    case Operation::SEQUENCE:
      out << "(";
      if (!children_.empty()) {
        children_[0].writeToStream(out);
      } else {
        out << "missing" << std::endl;
      }
      out << ")/(";
      if (children_.size() > 1) {
        children_[1].writeToStream(out);
      } else {
        out << "missing" << std::endl;
      }
      out << ")";
      break;
    case Operation::ZERO_OR_MORE:
      out << "(";
      if (!children_.empty()) {
        children_[0].writeToStream(out);
      } else {
        out << "missing" << std::endl;
      }
      out << ")*";
      break;
    case Operation::ONE_OR_MORE:
      out << "(";
      if (!children_.empty()) {
        children_[0].writeToStream(out);
      } else {
        out << "missing" << std::endl;
      }
      out << ")+";
      break;
    case Operation::ZERO_OR_ONE:
      out << "(";
      if (!children_.empty()) {
        children_[0].writeToStream(out);
      } else {
        out << "missing" << std::endl;
      }
      out << ")?";
      break;
  }
}

// _____________________________________________________________________________
std::string PropertyPath::asString() const {
  std::stringstream s;
  writeToStream(s);
  return std::move(s).str();
}

// _____________________________________________________________________________
void PropertyPath::computeCanBeNull() {
  canBeNull_ = !children_.empty();
  for (PropertyPath& p : children_) {
    p.computeCanBeNull();
    canBeNull_ &= p.canBeNull_;
  }

  if (operation_ == Operation::ZERO_OR_MORE ||
      operation_ == Operation::ZERO_OR_ONE) {
    canBeNull_ = true;
  }
}

// _____________________________________________________________________________
const std::string& PropertyPath::getIri() const {
  AD_CONTRACT_CHECK(isIri());
  return iri_;
}

// _____________________________________________________________________________
bool PropertyPath::isIri() const { return operation_ == Operation::IRI; }

// _____________________________________________________________________________
std::ostream& operator<<(std::ostream& out, const PropertyPath& p) {
  p.writeToStream(out);
  return out;
}
