// Copyright 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG

#include "parser/GraphPatternOperation.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <string_view>
#include <vector>

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "parser/ParsedQuery.h"
#include "parser/TripleComponent.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/VariantRangeFilter.h"

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
template <typename... Args>
auto m(Args&&... args) {
  return std::make_unique<ParsedQuery>(AD_FWD(args)...);
}
}  // namespace

// Special member functions for the `Subquery` class
Subquery::Subquery() : _subquery{m()} {}
Subquery::Subquery(const ParsedQuery& pq) : _subquery{m(pq)} {}
Subquery::Subquery(ParsedQuery&& pq) noexcept : _subquery{m(std::move(pq))} {}
Subquery::Subquery(Subquery&& pq) noexcept
    : _subquery{m(std::move(pq.get()))} {}
Subquery::Subquery(const Subquery& pq) : _subquery{m(pq.get())} {}
Subquery& Subquery::operator=(const Subquery& pq) {
  _subquery = m(pq.get());
  return *this;
}
Subquery& Subquery::operator=(Subquery&& pq) noexcept {
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
std::optional<TripleComponent> Bind::getConstant() const {
  using namespace sparqlExpression;
  const auto* expression = _expression.getPimpl();
  if (const auto* iri = dynamic_cast<const IriExpression*>(expression)) {
    return TripleComponent{iri->value()};
  } else if (const auto* literal =
                 dynamic_cast<const StringLiteralExpression*>(expression)) {
    return TripleComponent{literal->value()};
  } else if (const auto* id = dynamic_cast<const IdExpression*>(expression);
             id != nullptr && !id->value().isUndefined()) {
    return TripleComponent{id->value()};
  }
  return std::nullopt;
}

// ____________________________________________________________________________
[[nodiscard]] std::string Bind::getDescriptor() const {
  auto inner = _expression.getDescriptor();
  return "BIND (" + inner + " AS " + _target.name() + ")";
}

// ____________________________________________________________________________
void BasicGraphPattern::collectAllContainedVariables(
    ad_utility::HashSet<Variable>& vars) const {
  for (const SparqlTriple& t : _triples) {
    t.forEachVariable([&vars](const auto& var) { vars.insert(var); });
  }
}

}  // namespace parsedQuery
