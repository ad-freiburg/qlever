// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/VariableCounter.h"

#include "util/VariantRangeFilter.h"

namespace parsedQuery {

// _____________________________________________________________________________
void VariableCounter::operator()(const GraphPattern& gp) {
  (*this)(gp._filters);
  (*this)(gp._graphPatterns);
  (*this)(gp.textLimits_ | ql::views::keys);
}

// _____________________________________________________________________________
void VariableCounter::operator()(const Variable& var) { counts_[var]++; }

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
void VariableCounter::operator()(const Subquery& op) {
  (*this)(op.get()._rootGraphPattern);
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
void VariableCounter::operator()(const SparqlTriple& triple) {
  // `std::ref` is important here as we would otherwise pass a copy which would
  // count on its own.
  triple.forEachVariable(std::ref(*this));
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
void VariableCounter::operator()(const GroupGraphPattern& pattern) {
  (*this)(pattern._child);
  // Graph variable.
  if (std::holds_alternative<GroupGraphPattern::GraphVar>(pattern.graphSpec_)) {
    (*this)(std::get<GroupGraphPattern::GraphVar>(pattern.graphSpec_).first);
  }
}

// _____________________________________________________________________________
void VariableCounter::operator()(const PathQuery& op) {
  (*this)(op.pathColumn_);
  (*this)(op.edgeColumn_);
  (*this)(op.edgeProperties_);
  (*this)(op.end_);
  (*this)(op.sources_);
  (*this)(op.start_);
  (*this)(op.targets_);
  (*this)(op.childGraphPattern_);
}

// _____________________________________________________________________________
void VariableCounter::operator()(const SpatialQuery& op) {
  (*this)(op.distanceVariable_);
  if (!op.payloadVariables_.isAll()) {
    (*this)(op.payloadVariables_.getVariables());
  }
  (*this)(op.left_);
  (*this)(op.right_);
  (*this)(op.childGraphPattern_);
}

// _____________________________________________________________________________
void VariableCounter::operator()(const TextSearchQuery& op) {
  (*this)(op.configVarToConfigs_ | ql::views::keys);
  (*this)(op.configVarToConfigs_ | ql::views::values);
  (*this)(op.childGraphPattern_);
}

// _____________________________________________________________________________
void VariableCounter::operator()(const TextSearchConfig& op) {
  (*this)(op.textVar_);
  (*this)(op.matchVar_);
  (*this)(op.scoreVar_);
  if (op.entity_.has_value()) {
    const auto& variant = op.entity_.value();
    if (std::holds_alternative<Variable>(variant)) {
      (*this)(std::get<Variable>(variant));
    }
  }
}

// _____________________________________________________________________________
void VariableCounter::operator()(const Describe& op) {
  (*this)(op.whereClause_);
  (*this)(ad_utility::filterRangeOfVariantsByType<Variable>(op.resources_));
}

// _____________________________________________________________________________
void VariableCounter::operator()(const Load&) {
  // `LOAD` does not have any variables.
}

// _____________________________________________________________________________
void VariableCounter::operator()(const NamedCachedResult& op) {
  // `NamedCachedResult` does not know any variables at parsing time if the user
  // does not fill in a no-op child graph pattern to make them visible.
  (*this)(op.childGraphPattern_);
}

// _____________________________________________________________________________
void VariableCounter::operator()(const MaterializedViewQuery& op) {
  (*this)(op.scanCol_);
  (*this)(op.requestedColumns_ | ql::views::values);
}

// _____________________________________________________________________________
void VariableCounter::operator()(const ExternalValuesQuery& op) {
  (*this)(op.variables_);
}

}  // namespace parsedQuery
