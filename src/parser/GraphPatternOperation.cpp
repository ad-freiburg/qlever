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

// _____________________________________________________________________________
void GraphPatternOperation::toString(std::ostringstream& os,
                                     int indentation) const {
  for (int j = 1; j < indentation; ++j) os << "  ";

  visit([&os, indentation](auto&& arg) {
    using T = std::decay_t<decltype(arg)>;
    if constexpr (std::is_same_v<T, Optional>) {
      os << "OPTIONAL ";
      arg._child.toString(os, indentation);
    } else if constexpr (std::is_same_v<T, GroupGraphPattern>) {
      os << "GROUP ";
      arg._child.toString(os, indentation);
    } else if constexpr (std::is_same_v<T, Union>) {
      arg._child1.toString(os, indentation);
      os << " UNION ";
      arg._child2.toString(os, indentation);
    } else if constexpr (std::is_same_v<T, Subquery>) {
      // TODO<joka921> make the subquery a value-semantics type.
      os << arg.get().asString();
    } else if constexpr (std::is_same_v<T, Values>) {
      os << "VALUES (" << arg._inlineValues.variablesToString() << ") "
         << arg._inlineValues.valuesToString();
    } else if constexpr (std::is_same_v<T, Service>) {
      os << "SERVICE " << arg.serviceIri_.toSparql() << " { "
         << arg.graphPatternAsString_ << " }";
    } else if constexpr (std::is_same_v<T, BasicGraphPattern>) {
      for (size_t i = 0; i + 1 < arg._triples.size(); ++i) {
        os << "\n";
        for (int j = 0; j < indentation; ++j) os << "  ";
        os << arg._triples[i].asString() << ',';
      }
      if (arg._triples.size() > 0) {
        os << "\n";
        for (int j = 0; j < indentation; ++j) os << "  ";
        os << arg._triples.back().asString();
      }

    } else if constexpr (std::is_same_v<T, Bind>) {
      os << "BIND " << arg._expression.getDescriptor() << " as "
         << arg._target.name() << "\n";
      // TODO<joka921> proper ToString (are they used for something?)
    } else if constexpr (std::is_same_v<T, Minus>) {
      os << "MINUS ";
      arg._child.toString(os, indentation);
    } else {
      static_assert(std::is_same_v<T, TransPath>);
      /*
      os << "TRANS PATH from " << arg._left << " to " << arg._right
         << " with at least " << arg._min << " and at most " << arg._max
         << " steps of ";
      arg._childGraphPattern.toString(os, indentation);
       */
    }
  });
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
