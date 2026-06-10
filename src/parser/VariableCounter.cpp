// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/VariableCounter.h"

namespace parsedQuery {

// _____________________________________________________________________________
void VariableCounter::operator()(const GraphPattern& gp) {
  (*this)(gp._filters);
  (*this)(gp._graphPatterns);
  (*this)(gp.textLimits_ | ql::views::keys);
}

// _____________________________________________________________________________
CPP_template_def(typename T)(
    requires ql::ranges::input_range<T>) void VariableCounter::
operator()(const T& range) {
  for (const auto& elem : range) {
    (*this)(elem);  // dispatch each element to existing overloads
  }
}

// _____________________________________________________________________________
void VariableCounter::operator()(const Variable& var) { count_[var]++; }

// _____________________________________________________________________________
void VariableCounter::operator()(const Variable* var) {
  AD_CORRECTNESS_CHECK(var != nullptr);
  (*this)(*var);
}

// _____________________________________________________________________________
void VariableCounter::operator()(const SparqlFilter& filter) {
  (*this)(filter.expression_.containedVariables());
}

// _____________________________________________________________________________
void VariableCounter::operator()(const GraphPatternOperation& gpo) {
  std::visit(*this, gpo);
}

// _____________________________________________________________________________
void VariableCounter::operator()(const Optional& op) { (*this)(op._child); }

// _____________________________________________________________________________
void VariableCounter::operator()(const Union& op) {
  (*this)(op._child1);
  (*this)(op._child2);
}

// _____________________________________________________________________________
void VariableCounter::operator()(const TripleComponent& tc) {
  if (tc.isVariable()) {
    (*this)(tc.getVariable());
  }
}

// _____________________________________________________________________________
void VariableCounter::operator()(const TransPath& op) {
  (*this)(op._childGraphPattern);
  (*this)(op._innerLeft);
  (*this)(op._innerRight);
  (*this)(op._left);
  (*this)(op._right);
}

// _____________________________________________________________________________
void VariableCounter::operator()(const Bind& op) {
  (*this)(op.containedVariables());
}

// _____________________________________________________________________________
void VariableCounter::operator()(const SparqlTriple& var) {
  var.forEachVariable(*this);
}

// _____________________________________________________________________________
void VariableCounter::operator()(const BasicGraphPattern& op) {
  (*this)(op._triples);
}

// _____________________________________________________________________________
void VariableCounter::operator()(const Values& op) {
  (*this)(op._inlineValues._variables);
}

// _____________________________________________________________________________
void VariableCounter::operator()(const Service& op) {
  (*this)(op.visibleVariables_);
}

// _____________________________________________________________________________
void VariableCounter::operator()(const Minus& op) { (*this)(op._child); }

// _____________________________________________________________________________
void VariableCounter::operator()(const GroupGraphPattern& op) {
  (*this)(op._child);
  // Graph variable.
  std::visit(
      [this](const auto& gs) {
        using T = std::decay_t<decltype(gs)>;
        if constexpr (std::is_same_v<
                          T, std::pair<Variable, GroupGraphPattern::
                                                     GraphVariableBehaviour>>) {
          (*this)(gs.first);
        }
      },
      op.graphSpec_);
}

// TODO
void VariableCounter::operator()(const PathQuery& op) {}
void VariableCounter::operator()(const SpatialQuery& op) {}
void VariableCounter::operator()(const TextSearchQuery& op) {}
void VariableCounter::operator()(const Describe& op) {}
void VariableCounter::operator()(const Load& op) {}

void VariableCounter::operator()(const NamedCachedResult& op) {
  // TODO
}

// _____________________________________________________________________________
void VariableCounter::operator()(const MaterializedViewQuery& op) {
  (*this)(op.scanCol_);
  (*this)(op.requestedColumns_ | ql::views::keys);
}

// _____________________________________________________________________________
void VariableCounter::operator()(const ExternalValuesQuery& op) {
  (*this)(op.variables_);
}

// _____________________________________________________________________________
template <typename T>
void VariableCounter::operator()(const std::optional<T>& opt) {
  if (opt.has_value()) {
    (*this)(opt.value());
  }
}

}  // namespace parsedQuery
