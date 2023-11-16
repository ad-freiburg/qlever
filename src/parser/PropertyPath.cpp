// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
#include "PropertyPath.h"

// _____________________________________________________________________________
PropertyPath::PropertyPath(Operation op, std::string iri,
                           std::initializer_list<PropertyPath> children)
    : _operation(op), _iri(std::move(iri)), _children(children) {}

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
  switch (_operation) {
    case Operation::ALTERNATIVE:
      out << "(";
      if (!_children.empty()) {
        _children[0].writeToStream(out);
      } else {
        out << "missing" << std::endl;
      }
      out << ")|(";
      if (_children.size() > 1) {
        _children[1].writeToStream(out);
      } else {
        out << "missing" << std::endl;
      }
      out << ")";
      break;
    case Operation::INVERSE:
      out << "^(";
      if (!_children.empty()) {
        _children[0].writeToStream(out);
      } else {
        out << "missing" << std::endl;
      }
      out << ")";
      break;
    case Operation::IRI:
      out << _iri;
      break;
    case Operation::SEQUENCE:
      out << "(";
      if (!_children.empty()) {
        _children[0].writeToStream(out);
      } else {
        out << "missing" << std::endl;
      }
      out << ")/(";
      if (_children.size() > 1) {
        _children[1].writeToStream(out);
      } else {
        out << "missing" << std::endl;
      }
      out << ")";
      break;
    case Operation::ZERO_OR_MORE:
      out << "(";
      if (!_children.empty()) {
        _children[0].writeToStream(out);
      } else {
        out << "missing" << std::endl;
      }
      out << ")*";
      break;
    case Operation::ONE_OR_MORE:
      out << "(";
      if (!_children.empty()) {
        _children[0].writeToStream(out);
      } else {
        out << "missing" << std::endl;
      }
      out << ")+";
      break;
    case Operation::ZERO_OR_ONE:
      out << "(";
      if (!_children.empty()) {
        _children[0].writeToStream(out);
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
  can_be_null_ = !_children.empty();
  for (PropertyPath& p : _children) {
    p.computeCanBeNull();
    can_be_null_ &= p.can_be_null_;
  }

  if (_operation == Operation::ZERO_OR_MORE ||
      _operation == Operation::ZERO_OR_ONE) {
    can_be_null_ = true;
  }
}

// _____________________________________________________________________________
const std::string& PropertyPath::getIri() const {
  AD_CONTRACT_CHECK(_operation == Operation::IRI);
  return _iri;
}

// _____________________________________________________________________________
std::ostream& operator<<(std::ostream& out, const PropertyPath& p) {
  p.writeToStream(out);
  return out;
}
