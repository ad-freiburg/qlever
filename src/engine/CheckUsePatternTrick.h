//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_CHECKUSEPATTERNTRICK_H
#define QLEVER_SRC_ENGINE_CHECKUSEPATTERNTRICK_H

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
 * CountAvailablePredicates operation) is applicable to the given
 * parsed query. If a ql:has-predicate triple is found and
 * CountAvailablePredicates can be used for it, the triple's predicate will be
 * replaced by `ql:has-pattern`. If possible, then this rewrite is performed by
 * completely removing the triple and adding the pattern as an
 * additional scan column to one of the other triples (note that we have folded
 * the patterns for the subject and object into the PSO and POS permutation).
 * The mapping from the pattern to the predicates contained in that pattern will
 * later be done by the `CountAvailablePredicates` operation.
 */
std::optional<PatternTrickTuple> checkUsePatternTrick(ParsedQuery* parsedQuery);

// Internal helper function used by `checkUsePatternTrick`. Check whether the
// given `triple` is suitable as a pattern trick triple in the given
// `parsedQuery`. If the `countedVariable` is not `nullopt`, it also has to
// match the given triple and parsedQuery.
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

#endif  // QLEVER_SRC_ENGINE_CHECKUSEPATTERNTRICK_H
