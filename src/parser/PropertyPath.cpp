//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "parser/PropertyPath.h"

// _____________________________________________________________________________
PropertyPath::MinMaxPath& PropertyPath::MinMaxPath::operator=(
    const MinMaxPath& other) {
  min_ = other.min_;
  max_ = other.max_;
  child_ = std::make_unique<PropertyPath>(*other.child_);
  return *this;
}

// _____________________________________________________________________________
bool PropertyPath::MinMaxPath::operator==(const MinMaxPath& other) const {
  return min_ == other.min_ && max_ == other.max_ && *child_ == *other.child_;
}

// _____________________________________________________________________________
PropertyPath::PropertyPath(
    std::variant<ad_utility::triple_component::Iri, ModifiedPath, MinMaxPath>
        path)
    : path_{std::move(path)} {}

// _____________________________________________________________________________
PropertyPath PropertyPath::fromIri(ad_utility::triple_component::Iri iri) {
  return PropertyPath{std::move(iri)};
}

// _____________________________________________________________________________
PropertyPath PropertyPath::makeWithLength(PropertyPath child, size_t min,
                                          size_t max) {
  return PropertyPath{
      MinMaxPath{min, max, std::make_unique<PropertyPath>(std::move(child))}};
}

// _____________________________________________________________________________
PropertyPath PropertyPath::makeAlternative(std::vector<PropertyPath> children) {
  AD_CONTRACT_CHECK(children.size() > 1,
                    "Alternative paths must have at least two children.");
  return PropertyPath{ModifiedPath{std::move(children), Modifier::ALTERNATIVE}};
}

// _____________________________________________________________________________
PropertyPath PropertyPath::makeSequence(std::vector<PropertyPath> children) {
  AD_CONTRACT_CHECK(children.size() > 1,
                    "Sequence paths must have at least two children.");
  return PropertyPath{ModifiedPath{std::move(children), Modifier::SEQUENCE}};
}

// _____________________________________________________________________________
PropertyPath PropertyPath::makeInverse(PropertyPath child) {
  return PropertyPath{ModifiedPath{{std::move(child)}, Modifier::INVERSE}};
}

// _____________________________________________________________________________
PropertyPath PropertyPath::makeNegated(std::vector<PropertyPath> children) {
  return PropertyPath{ModifiedPath{std::move(children), Modifier::NEGATED}};
}

// _____________________________________________________________________________
void PropertyPath::ModifiedPath::writeToStream(std::ostream& out) const {
  if (modifier_ == Modifier::INVERSE) {
    out << '^';
  } else if (modifier_ == Modifier::NEGATED) {
    out << '!';
  }
  if (children_.empty()) {
    out << "()";
    return;
  }

  char separator = modifier_ == Modifier::SEQUENCE ? '/' : '|';

  if (modifier_ == Modifier::NEGATED) {
    out << '(';
  }
  bool firstRun = true;
  for (const auto& child : children_) {
    if (!firstRun) {
      out << separator;
    }
    firstRun = false;
    bool needsBraces = std::holds_alternative<ModifiedPath>(child.path_);
    if (needsBraces) {
      out << '(';
    }
    child.writeToStream(out);
    if (needsBraces) {
      out << ')';
    }
  }
  if (modifier_ == Modifier::NEGATED) {
    out << ')';
  }
}

// _____________________________________________________________________________
void PropertyPath::writeToStream(std::ostream& out) const {
  std::visit(ad_utility::OverloadCallOperator{
                 [&out](const ad_utility::triple_component::Iri& iri) {
                   out << iri.toStringRepresentation();
                 },
                 [&out](const ModifiedPath& path) { path.writeToStream(out); },
                 [&out](const MinMaxPath& path) {
                   out << '(';
                   path.child_->writeToStream(out);
                   out << "){" << path.min_ << ',' << path.max_ << '}';
                 }},
             path_);
}

// _____________________________________________________________________________
std::string PropertyPath::asString() const {
  std::stringstream s;
  writeToStream(s);
  return std::move(s).str();
}

// _____________________________________________________________________________
const ad_utility::triple_component::Iri& PropertyPath::getIri() const {
  AD_CONTRACT_CHECK(isIri());
  return std::get<ad_utility::triple_component::Iri>(path_);
}

// _____________________________________________________________________________
bool PropertyPath::isIri() const {
  return std::holds_alternative<ad_utility::triple_component::Iri>(path_);
}

// _____________________________________________________________________________
std::optional<std::reference_wrapper<const PropertyPath>>
PropertyPath::getChildOfInvertedPath() const {
  if (std::holds_alternative<ModifiedPath>(path_)) {
    const auto& path = std::get<ModifiedPath>(path_);
    if (path.modifier_ == Modifier::INVERSE) {
      AD_CORRECTNESS_CHECK(path.children_.size() == 1);
      return path.children_.at(0);
    }
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::ostream& operator<<(std::ostream& out, const PropertyPath& p) {
  p.writeToStream(out);
  return out;
}
