// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
#include "PropertyPath.h"

// _____________________________________________________________________________
PropertyPath::PropertyPath(Operation op, uint16_t limit, std::string iri,
                           std::initializer_list<PropertyPath> children)
    : _operation(op),
      _limit(limit),
      _iri(std::move(iri)),
      _children(children),
      _can_be_null(false) {}

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
    case Operation::TRANSITIVE:
      out << "(";
      if (!_children.empty()) {
        _children[0].writeToStream(out);
      } else {
        out << "missing" << std::endl;
      }
      out << ")*";
      break;
    case Operation::TRANSITIVE_MAX:
      out << "(";
      if (!_children.empty()) {
        _children[0].writeToStream(out);
      } else {
        out << "missing" << std::endl;
      }
      out << ")";
      if (_limit == 1) {
        out << "?";
      } else {
        out << "*" << _limit;
      }
      break;
    case Operation::TRANSITIVE_MIN:
      out << "(";
      if (!_children.empty()) {
        _children[0].writeToStream(out);
      } else {
        out << "missing" << std::endl;
      }
      out << ")+";
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
  _can_be_null = !_children.empty();
  for (PropertyPath& p : _children) {
    p.computeCanBeNull();
    _can_be_null &= p._can_be_null;
  }
  if (_operation == Operation::TRANSITIVE ||
      _operation == Operation::TRANSITIVE_MAX ||
      (_operation == Operation::TRANSITIVE_MIN && _limit == 0)) {
    _can_be_null = true;
  }
}

// _____________________________________________________________________________
const std::string& PropertyPath::getIri() const {
  AD_CHECK(_operation == Operation::IRI);
  return _iri;
}

// _____________________________________________________________________________
std::ostream& operator<<(std::ostream& out, const PropertyPath& p) {
  p.writeToStream(out);
  return out;
}
