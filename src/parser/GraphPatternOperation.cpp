// Copyright 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "parser/GraphPatternOperation.h"

#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "parser/ParsedQuery.h"
#include "util/Forward.h"

namespace parsedQuery {

// _____________________________________________________________________________
std::string SparqlValues::variablesToString() const {
  return absl::StrJoin(_variables, "\t", Variable::AbslFormatter);
}

// _____________________________________________________________________________
std::string SparqlValues::valuesToString() const {
  auto tripleComponentVectorAsValueTuple =
      [](const std::vector<TripleComponent>& v) -> std::string {
    return absl::StrCat(
        "(",
        absl::StrJoin(v, " ",
                      [](std::string* out, const TripleComponent& tc) {
                        out->append(tc.toString());
                      }),
        ")");
  };
  return absl::StrJoin(
      _values, " ",
      [&](std::string* out, const std::vector<TripleComponent>& v) {
        out->append(tripleComponentVectorAsValueTuple(v));
      });
}

// Small anonymous helper function that is used in the definition of the member
// functions of the `Subquery` class.
namespace {
auto m(auto&&... args) {
  return std::make_unique<ParsedQuery>(AD_FWD(args)...);
}
}  // namespace

// Special member functions for the `Subquery` class
Subquery::Subquery() : _subquery{m()} {}
Subquery::Subquery(const ParsedQuery& pq) : _subquery{m(pq)} {}
Subquery::Subquery(ParsedQuery&& pq) : _subquery{m(std::move(pq))} {}
Subquery::Subquery(Subquery&& pq) : _subquery{m(std::move(pq.get()))} {}
Subquery::Subquery(const Subquery& pq) : _subquery{m(pq.get())} {}
Subquery& Subquery::operator=(const Subquery& pq) {
  _subquery = m(pq.get());
  return *this;
}
Subquery& Subquery::operator=(Subquery&& pq) {
  _subquery = m(std::move(pq.get()));
  return *this;
}
Subquery::~Subquery() = default;
ParsedQuery& Subquery::get() { return *_subquery; }
const ParsedQuery& Subquery::get() const { return *_subquery; }

// _____________________________________________________________________________
void BasicGraphPattern::appendTriples(BasicGraphPattern other) {
  ad_utility::appendVector(_triples, std::move(other._triples));
}

// ____________________________________________________________________________
void PathQuery::addParameter(const SparqlTriple& triple) {
  auto simpleTriple = triple.getSimple();
  TripleComponent predicate = simpleTriple.p_;
  TripleComponent object = simpleTriple.o_;
  AD_CORRECTNESS_CHECK(predicate.isIri());
  if (predicate.getIri().toStringRepresentation().ends_with("source>")) {
    AD_CORRECTNESS_CHECK(object.isIri());
    sources_.push_back(std::move(object));
  } else if (predicate.getIri().toStringRepresentation().ends_with("target>")) {
    AD_CORRECTNESS_CHECK(object.isIri());
    targets_.push_back(std::move(object));
  } else if (predicate.getIri().toStringRepresentation().ends_with("start>")) {
    AD_CORRECTNESS_CHECK(object.isVariable());
    start_ = object.getVariable();
  } else if (predicate.getIri().toStringRepresentation().ends_with("end>")) {
    AD_CORRECTNESS_CHECK(object.isVariable());
    end_ = object.getVariable();
  } else if (predicate.getIri().toStringRepresentation().ends_with(
                 "pathColumn>")) {
    AD_CORRECTNESS_CHECK(object.isVariable());
    pathColumn_ = object.getVariable();
  } else if (predicate.getIri().toStringRepresentation().ends_with(
                 "edgeColumn>")) {
    AD_CORRECTNESS_CHECK(object.isVariable());
    edgeColumn_ = object.getVariable();
  } else if (predicate.getIri().toStringRepresentation().ends_with(
                 "edgeProperty>")) {
    AD_CORRECTNESS_CHECK(object.isVariable());
    edgeProperties_.push_back(object.getVariable());
  } else if (predicate.getIri().toStringRepresentation().ends_with(
                 "algorithm>")) {
    AD_CORRECTNESS_CHECK(object.isIri());
    if (object.getIri().toStringRepresentation().ends_with("allPaths>")) {
      algorithm_ = PathSearchAlgorithm::ALL_PATHS;
    } else if (object.getIri().toStringRepresentation().ends_with(
                   "shortestPaths>")) {
      algorithm_ = PathSearchAlgorithm::SHORTEST_PATHS;
    } else {
      AD_THROW("Unsupported algorithm in PathSearch");
    }
  } else {
    AD_THROW("Unsupported argument in PathSearch");
  }
}

// ____________________________________________________________________________
void PathQuery::fromBasicPattern(const BasicGraphPattern& pattern) {
  for (SparqlTriple triple : pattern._triples) {
    addParameter(triple);
  }
}

// ____________________________________________________________________________
cppcoro::generator<const Variable> Bind::containedVariables() const {
  for (const auto* ptr : _expression.containedVariables()) {
    co_yield *ptr;
  }
  co_yield _target;
}

// ____________________________________________________________________________
[[nodiscard]] string Bind::getDescriptor() const {
  auto inner = _expression.getDescriptor();
  return "BIND (" + inner + " AS " + _target.name() + ")";
}
}  // namespace parsedQuery
