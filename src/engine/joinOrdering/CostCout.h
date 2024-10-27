// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#pragma once

#include <map>
#include <memory>

#include "JoinNode.h"
#include "JoinTree.h"
#include "QueryGraph.h"

namespace JoinOrdering::Cost {

template <typename N>
requires RelationAble<N>
double Cout(const JoinTree<N>& t, const QueryGraph<N>& q);

/**
 *
 * Basic Cost Function that returns an estimate on how expensive to
 * evaluate a given JoinTree. low cost implies cheap execution plan.
 *
 * ref: 79/637
 *
 * // TODO: can be inferred by RelationAble::getCardinality
 * // TODO: better use some sort of map of unordered pairs since selectivity is
 * direction-less
 * // TODO: default to 1.0 when the selectivity between 2 relations is not
 * defined.
 *
 * @tparam N type that satisfies RelationAble concept
 * @param t Linear JoinTree (left-deep, right-deep, zigzag, ...)
 * @param cardinalities map of cardinality of each relation in the tree
 * @param selectivities map of selectivity for each pair of relation in the tree
 * @return Cost Evaluation for given JoinTree
 */
template <typename N>
requires RelationAble<N> double Cout(
    const JoinTree<N>& t,
    const std::map<std::string, unsigned long long>& cardinalities,
    const std::map<std::string, std::map<std::string, float>>& selectivities);

/**
 *
 *
 * @tparam N type that satisfies RelationAble concept
 * @param r JoinNode that can be inner (join operators) or leaf node (relations)
 * @param cardinalities map of cardinality of each relation in the tree
 * @param selectivities map of selectivity for each pair of relation in the tree
 * @return Cost Evaluation for given JoinNode
 */
template <typename N>
requires RelationAble<N> double Cout(
    std::shared_ptr<JoinNode<N>> r,
    const std::map<std::string, unsigned long long>& cardinalities,
    const std::map<std::string, std::map<std::string, float>>& selectivities);

// template <typename N>
// requires RelationAble<N> unsigned long long cardinality(
//     std::shared_ptr<JoinNode<N>> r,
//     const std::map<std::string, int>& cardinalities,
//     const std::map<std::string, std::map<std::string, float>>&
//     selectivities);
//
// template <typename N>
// requires RelationAble<N> float selectivity(
//     std::shared_ptr<JoinNode<N>> x, std::shared_ptr<JoinNode<N>> y,
//     const std::map<std::string, std::map<std::string, float>>&
//     selectivities);

}  // namespace JoinOrdering::Cost
