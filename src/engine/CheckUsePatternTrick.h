//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#pragma once
#include <optional>

#include "parser/ParsedQuery.h"

namespace checkUsePatternTrick {

// If the pattern trick can be applied, then this struct is used to communicate
// the subject and predicate variable for the pattern trick.
struct PatternTrickTuple {
  Variable subject_;
  Variable predicate_;
};
/**
 * @brief Determines if the pattern trick (and in turn the
 * CountAvailablePredicates operation) are applicable to the given
 * parsed query. If a ql:has-predicate triple is found and
 * CountAvailblePredicates can be used for it, the triple will be removed from
 * the parsed query.
 * @param parsedQuery The parsed query.
 * @param patternTrickTriple An output parameter in which the triple that
 * satisfies the requirements for the pattern trick is stored.
 * @return True if the pattern trick should be used.
 */
std::optional<PatternTrickTuple> checkUsePatternTrick(ParsedQuery* parsedQuery);

// TODO<joka921> Comment.
std::optional<PatternTrickTuple> isTripleSuitableForPatternTrick(
    const SparqlTriple& triple, const ParsedQuery* parsedQuery,
    const std::optional<
        sparqlExpression::SparqlExpressionPimpl::VariableAndDistinctness>&
        countedVariable);

// Return true if and only if the `variable` is contained and visible (not
// inside a subquery) anywhere in the `graphPattern`. If the only occurrence of
// the variable is in the `ignoredTriple`, then false will be returned. This
// comparison is done on the pointer level. It is thus safe to pass `nullptr`
// as the `ignoredTriple` if no such triple exists. This function is used for
// checking if a certain query is eligible for the pattern trick (see
// `QueryPlanner::checkUsePatternTrick`).
bool isVariableContainedInGraphPattern(
    const Variable& variable, const parsedQuery::GraphPattern& graphPattern,
    const SparqlTriple* tripleToIgnore);

// Similar to `isVariableContainedInGraphPattern`, but works on a
// `GraphPatternOperation`.
bool isVariableContainedInGraphPatternOperation(
    const Variable& variable,
    const parsedQuery::GraphPatternOperation& operation,
    const SparqlTriple* tripleToIgnore);

}  // namespace checkUsePatternTrick
