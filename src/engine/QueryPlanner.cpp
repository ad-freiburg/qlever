// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include "engine/QueryPlanner.h"

#include <algorithm>
#include <ctime>

#include "engine/Bind.h"
#include "engine/CartesianProductJoin.h"
#include "engine/CheckUsePatternTrick.h"
#include "engine/CountAvailablePredicates.h"
#include "engine/Distinct.h"
#include "engine/Filter.h"
#include "engine/GroupBy.h"
#include "engine/HasPredicateScan.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/Minus.h"
#include "engine/MultiColumnJoin.h"
#include "engine/NeutralElementOperation.h"
#include "engine/OptionalJoin.h"
#include "engine/OrderBy.h"
#include "engine/Service.h"
#include "engine/Sort.h"
#include "engine/TextIndexScanForEntity.h"
#include "engine/TextIndexScanForWord.h"
#include "engine/TextOperationWithFilter.h"
#include "engine/TextOperationWithoutFilter.h"
#include "engine/TransitivePath.h"
#include "engine/Union.h"
#include "engine/Values.h"
#include "parser/Alias.h"
#include "parser/SparqlParserHelpers.h"

namespace p = parsedQuery;
namespace {

using ad_utility::makeExecutionTree;

template <typename Operation>
QueryPlanner::SubtreePlan makeSubtreePlan(QueryExecutionContext* qec,
                                          auto&&... args) {
  return {qec, std::make_shared<Operation>(qec, AD_FWD(args)...)};
}

// Create a `SubtreePlan` that holds the given `operation`. `Op` must be a class
// inheriting from `Operation`.
template <typename Op>
QueryPlanner::SubtreePlan makeSubtreePlan(std::shared_ptr<Op> operation) {
  auto* qec = operation->getExecutionContext();
  return {qec, std::move(operation)};
}

// Update the `target` query plan such that it knows that it includes all the
// nodes and filters from `a` and `b`. NOTE: This does not actually merge
// the plans from `a` and `b`.
void mergeSubtreePlanIds(QueryPlanner::SubtreePlan& target,
                         const QueryPlanner::SubtreePlan& a,
                         const QueryPlanner::SubtreePlan& b) {
  target._idsOfIncludedNodes = a._idsOfIncludedNodes | b._idsOfIncludedNodes;
  target._idsOfIncludedFilters =
      a._idsOfIncludedFilters | b._idsOfIncludedFilters;
}
}  // namespace

// _____________________________________________________________________________
QueryPlanner::QueryPlanner(QueryExecutionContext* qec)
    : _qec(qec), _internalVarCount(0), _enablePatternTrick(true) {}

// _____________________________________________________________________________
std::vector<QueryPlanner::SubtreePlan> QueryPlanner::createExecutionTrees(
    ParsedQuery& pq) {
  // Look for ql:has-predicate to determine if the pattern trick should be used.
  // If the pattern trick is used, the ql:has-predicate triple will be removed
  // from the list of where clause triples. Otherwise, the ql:has-predicate
  // triple will be handled using a `HasPredicateScan`.
  using checkUsePatternTrick::PatternTrickTuple;
  const auto patternTrickTuple =
      _enablePatternTrick ? checkUsePatternTrick::checkUsePatternTrick(&pq)
                          : std::nullopt;

  // Do GROUP BY if one of the following applies:
  // 1. There is an explicit group by
  // 2. The pattern trick is applied
  // 3. There is an alias with an aggregate expression
  // TODO<joka921> Non-aggretating aliases (for example (?x AS ?y)) are
  // currently not handled properly. When fixing this you have to distinguish
  // the following two cases:
  // 1. Mix of aggregating and non-aggregating aliases without GROUP BY.
  // 2. Only non-aggretating aliases without GROUP BY.
  // Note: When a GROUP BY is present, then all aliases have to be aggregating,
  // this is handled correctly in all cases.
  bool doGroupBy = !pq._groupByVariables.empty() ||
                   patternTrickTuple.has_value() ||
                   std::ranges::any_of(pq.getAliases(), [](const Alias& alias) {
                     return alias._expression.containsAggregate();
                   });

  // Optimize the graph pattern tree
  std::vector<std::vector<SubtreePlan>> plans;
  plans.push_back(optimize(&pq._rootGraphPattern));

  // Add the query level modifications

  // GROUP BY (Either the pattern trick or a "normal" GROUP BY)
  if (patternTrickTuple.has_value()) {
    plans.emplace_back(getPatternTrickRow(pq.selectClause(), plans,
                                          patternTrickTuple.value()));
  } else if (doGroupBy) {
    plans.emplace_back(getGroupByRow(pq, plans));
  }

  // HAVING
  if (!pq._havingClauses.empty()) {
    plans.emplace_back(getHavingRow(pq, plans));
  }

  // DISTINCT
  if (pq.hasSelectClause()) {
    const auto& selectClause = pq.selectClause();
    if (selectClause.distinct_) {
      plans.emplace_back(getDistinctRow(selectClause, plans));
    }
  }

  // ORDER BY
  if (!pq._orderBy.empty()) {
    // If there is an order by clause, add another row to the table and
    // just add an order by / sort to every previous result if needed.
    // If the ordering is perfect already, just copy the plan.
    plans.emplace_back(getOrderByRow(pq, plans));
  }

  // Now find the cheapest execution plan and store that as the optimal
  // plan for this graph pattern.
  vector<SubtreePlan>& lastRow = plans.back();

  for (auto& plan : lastRow) {
    if (plan._qet->getRootOperation()->supportsLimit()) {
      plan._qet->getRootOperation()->setLimit(pq._limitOffset);
    }
  }

  AD_CONTRACT_CHECK(!lastRow.empty());
  if (pq._rootGraphPattern._optional) {
    for (auto& plan : lastRow) {
      plan.type = SubtreePlan::OPTIONAL;
    }
  }

  for (auto& plan : lastRow) {
    plan._qet->setTextLimit(pq._limitOffset._textLimit);
  }
  return lastRow;
}

// _____________________________________________________________________
QueryExecutionTree QueryPlanner::createExecutionTree(ParsedQuery& pq) {
  auto lastRow = createExecutionTrees(pq);
  auto minInd = findCheapestExecutionTree(lastRow);
  LOG(DEBUG) << "Done creating execution plan.\n";
  return *lastRow[minInd]._qet;
}

std::vector<QueryPlanner::SubtreePlan> QueryPlanner::optimize(
    ParsedQuery::GraphPattern* rootPattern) {
  // here we collect a set of possible plans for each of our children.
  // always only holds plans for children that can be joined in an
  // arbitrary order
  std::vector<std::vector<SubtreePlan>> candidatePlans;
  // triples from BasicGraphPatterns that can be joined arbirarily
  // with each other and the contents of  candidatePlans
  p::BasicGraphPattern candidateTriples;

  // all Variables that have been bound be the children we have dealt with
  // so far. TODO<joka921> verify that we get no false positives with plans
  // that create no single binding for a variable "by accident".
  ad_utility::HashSet<Variable> boundVariables;

  // lambda that optimizes a set of triples, other execution plans and filters
  // under the assumption that they are commutative and can be joined in an
  // arbitrary order. When a NON-permuting plan is encountered, then
  // we first  call this function to optimize the preceding permuting plans,
  // and subsequently join in the correct order with the non-permuting plan.
  // Returns the last row of the DP table (a set of possible plans with possibly
  // different costs and different orderings.
  auto optimizeCommutativ = [this](const auto& triples, const auto& plans,
                                   const auto& filters) {
    auto tg = createTripleGraph(&triples);
    // always apply all filters to be safe.
    // TODO<joka921> it could be possible, to allow the DpTab to leave
    // results unfiltered and add the filters later, but this has to be
    // carefully checked and I currently see no benefit.
    // TODO<joka921> In fact, for the case of REGEX filters, it could be
    // beneficial to postpone them if possible
    return fillDpTab(tg, filters, plans).back();
  };

  // find a single best candidate for a given graph pattern
  auto optimizeSingle = [this](const auto pattern) -> SubtreePlan {
    auto v = optimize(pattern);
    if (v.empty()) {
      throw std::runtime_error(
          "grandchildren or lower of a Plan to be optimized may never be "
          "empty");
    }
    auto idx = findCheapestExecutionTree(v);
    return std::move(v[idx]);
  };

  // the callback that is called after dealing with a child pattern.
  // Can either be passed a BasicGraphPattern directly or a set
  // of possible candidate plans for a single child pattern
  auto joinCandidates = [this, &candidatePlans, &candidateTriples,
                         &optimizeCommutativ, &boundVariables,
                         &rootPattern](auto&& v) {
    if constexpr (std::is_same_v<p::BasicGraphPattern,
                                 std::decay_t<decltype(v)>>) {
      // we only consist of triples, store them and all the bound variables.
      for (const SparqlTriple& t : v._triples) {
        if (isVariable(t._s)) {
          boundVariables.insert(t._s.getVariable());
        }
        if (isVariable(t._p)) {
          boundVariables.insert(Variable{t._p._iri});
        }
        if (isVariable(t._o)) {
          boundVariables.insert(t._o.getVariable());
        }
      }
      candidateTriples._triples.insert(
          candidateTriples._triples.end(),
          std::make_move_iterator(v._triples.begin()),
          std::make_move_iterator(v._triples.end()));
    } else if constexpr (std::is_same_v<p::Bind, std::decay_t<decltype(v)>>) {
      if (boundVariables.contains(v._target)) {
        AD_THROW(
            "The target variable of a BIND must not be used before the "
            "BIND clause");
      }
      boundVariables.insert(v._target);

      // Assumption for now: BIND does not commute. This is always safe.
      auto lastRow = optimizeCommutativ(candidateTriples, candidatePlans,
                                        rootPattern->_filters);
      candidateTriples._triples.clear();
      candidatePlans.clear();
      candidatePlans.emplace_back();
      for (const auto& a : lastRow) {
        // create a copy of the Bind prototype and add the corresponding subtree
        SubtreePlan plan = makeSubtreePlan<Bind>(_qec, a._qet, v);
        plan._idsOfIncludedFilters = a._idsOfIncludedFilters;
        candidatePlans.back().push_back(std::move(plan));
      }
      // Handle the case that the BIND clause is the first clause which means
      // that `lastRow` is empty.
      if (lastRow.empty()) {
        auto neutralElement = makeExecutionTree<NeutralElementOperation>(_qec);
        candidatePlans.back().push_back(
            makeSubtreePlan<Bind>(_qec, std::move(neutralElement), v));
      }
      return;
    } else {
      static_assert(
          std::is_same_v<std::vector<SubtreePlan>, std::decay_t<decltype(v)>>);
      if (v.empty()) {
        throw std::runtime_error(
            "grandchildren or lower of a Plan to be optimized may never be "
            "empty. Please report this");
      }

      // optionals that occur before any of their variables have been bound
      // actually behave like ordinary (Group)GraphPatterns
      if (v[0].type == SubtreePlan::OPTIONAL) {
        auto vc = v[0]._qet->getVariableColumns();
        if (std::all_of(vc.begin(), vc.end(),
                        [&boundVariables](const auto& el) {
                          return !boundVariables.contains(Variable{el.first});
                        })) {
          // all variables in the optional are unbound so far, so this optional
          // actually is not an optional.
          for (auto& vec : v) {
            vec.type = SubtreePlan::BASIC;
          }
        }
      }

      // All variables seen so far are considered bound and cannot appear as the
      // RHS of a BIND operation. This is also true for variables from OPTIONALs
      // and MINUS clauses (this was a bug in the previous version of the code).
      {
        auto vc = v[0]._qet->getVariableColumns();
        std::for_each(vc.begin(), vc.end(), [&boundVariables](const auto& el) {
          boundVariables.insert(Variable{el.first});
        });
      }

      // if our input is not optional and not a minus this means we still can
      // arbitrarily optimize among our candidates and just append our new
      // candidates.
      if (v[0].type == SubtreePlan::BASIC) {
        candidatePlans.push_back(std::forward<decltype(v)>(v));
        return;
      }

      // v is an optional or minus join, optimization across is forbidden.
      // optimize all previously collected candidates, and then perform
      // an optional join.
      auto lastRow = optimizeCommutativ(candidateTriples, candidatePlans,
                                        rootPattern->_filters);
      candidateTriples._triples.clear();
      candidatePlans.clear();

      std::vector<SubtreePlan> nextCandidates;
      // For each candidate plan, and each plan from the OPTIONAL, create a
      // new plan with an optional join. Note that createJoinCandidates will
      // know that b is from an OPTIONAL.
      for (const auto& a : lastRow) {
        for (const auto& b : v) {
          auto vec = createJoinCandidates(a, b, std::nullopt);
          nextCandidates.insert(nextCandidates.end(),
                                std::make_move_iterator(vec.begin()),
                                std::make_move_iterator(vec.end()));
        }
      }

      // keep the best found candidate, which is now a non-optional "so far"
      // solution which can be combined with all upcoming children until we
      // hit the next optional
      // TODO<joka921> Also keep one candidate per Ordering to make even
      // better plans at this step
      if (nextCandidates.empty()) {
        throw std::runtime_error(
            "Could not find a single candidate join for two optimized Graph "
            "patterns. Please report to the developers");
      }
      auto idx = findCheapestExecutionTree(nextCandidates);
      candidatePlans.push_back({std::move(nextCandidates[idx])});
      return;
    }
  };  // End of joinCandidates lambda.

  // go through the child patterns in order, set up all their candidatePlans
  // and then call the joinCandidates call back
  for (auto& child : rootPattern->_graphPatterns) {
    child.visit([&optimizeSingle, &joinCandidates, this](auto&& arg) {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, p::Optional> ||
                    std::is_same_v<T, p::GroupGraphPattern>) {
        auto candidates = optimize(&arg._child);
        if constexpr (std::is_same_v<T, p::Optional>) {
          for (auto& c : candidates) {
            c.type = SubtreePlan::OPTIONAL;
          }
        }
        joinCandidates(std::move(candidates));
      } else if constexpr (std::is_same_v<T, p::Union>) {
        // TODO<joka921> here we could keep all the candidates, and create a
        // "sorted union" by merging as additional candidates if the inputs
        // are presorted.
        SubtreePlan left = optimizeSingle(&arg._child1);
        SubtreePlan right = optimizeSingle(&arg._child2);

        // create a new subtree plan
        SubtreePlan candidate =
            makeSubtreePlan<Union>(_qec, left._qet, right._qet);
        joinCandidates(std::vector{std::move(candidate)});
      } else if constexpr (std::is_same_v<T, p::Subquery>) {
        // TODO<joka921> We currently do not optimize across subquery borders
        // but abuse them as "optimization hints". In theory, one could even
        // remove the ORDER BY clauses of a subquery if we can prove that
        // the results will be reordered anyway.

        // For a subquery, make sure that one optimal result for each ordering
        // of the result (by a single column) is contained.
        auto candidatesForSubquery = createExecutionTrees(arg.get());
        // Make sure that variables that are not selected by the subquery are
        // not visible.
        auto setSelectedVariables = [&](SubtreePlan& plan) {
          plan._qet->getRootOperation()->setSelectedVariablesForSubquery(
              arg.get().selectClause().getSelectedVariables());
        };
        std::ranges::for_each(candidatesForSubquery, setSelectedVariables);
        // A subquery must also respect LIMIT and OFFSET clauses
        std::ranges::for_each(candidatesForSubquery, [&](SubtreePlan& plan) {
          plan._qet->getRootOperation()->setLimit(arg.get()._limitOffset);
        });
        joinCandidates(std::move(candidatesForSubquery));
      } else if constexpr (std::is_same_v<T, p::TransPath>) {
        // TODO<kramerfl> This is obviously how you set up transitive paths.
        // maybe factor this out and comment it somewhere
        auto candidatesIn = optimize(&arg._childGraphPattern);
        std::vector<SubtreePlan> candidatesOut;

        for (auto& sub : candidatesIn) {
          TransitivePathSide left;
          TransitivePathSide right;
          // TODO<joka921> Refactor the `TransitivePath` class s.t. we don't
          // have to specify a `Variable` that isn't used at all in the case of
          // a fixed subject or object.
          auto getSideValue = [this](const TripleComponent& side) {
            std::variant<Id, Variable> value;
            if (isVariable(side)) {
              value = Variable{side.getVariable()};
            } else {
              value = generateUniqueVarName();
              if (auto opt = side.toValueId(_qec->getIndex().getVocab());
                  opt.has_value()) {
                value = opt.value();
              } else {
                AD_THROW("No vocabulary entry for " + side.toString());
              }
            }
            return value;
          };

          left.subCol_ =
              sub._qet->getVariableColumn(arg._innerLeft.getVariable());
          left.value_ = getSideValue(arg._left);
          right.subCol_ =
              sub._qet->getVariableColumn(arg._innerRight.getVariable());
          right.value_ = getSideValue(arg._right);
          size_t min = arg._min;
          size_t max = arg._max;
          auto plan = makeSubtreePlan<TransitivePath>(_qec, sub._qet, left,
                                                      right, min, max);
          candidatesOut.push_back(std::move(plan));
        }
        joinCandidates(std::move(candidatesOut));

      } else if constexpr (std::is_same_v<T, p::Values>) {
        SubtreePlan valuesPlan =
            makeSubtreePlan<Values>(_qec, arg._inlineValues);
        joinCandidates(std::vector{std::move(valuesPlan)});
      } else if constexpr (std::is_same_v<T, p::Service>) {
        SubtreePlan servicePlan = makeSubtreePlan<Service>(_qec, arg);
        joinCandidates(std::vector{std::move(servicePlan)});
      } else if constexpr (std::is_same_v<T, p::Bind>) {
        // The logic of the BIND operation is implemented in the joinCandidates
        // lambda. Reason: BIND does not add a new join operation like for the
        // other operations above.
        joinCandidates(arg);
      } else if constexpr (std::is_same_v<T, p::Minus>) {
        auto candidates = optimize(&arg._child);
        for (auto& c : candidates) {
          c.type = SubtreePlan::MINUS;
        }
        joinCandidates(std::move(candidates));
      } else {
        static_assert(std::is_same_v<T, p::BasicGraphPattern>);
        // just add all the triples directly.
        joinCandidates(arg);
      }
    });
  }
  // one last pass in case the last one was not an optional
  // if the last child was not an optional clause we still have unjoined
  // candidates. Do one last pass over them.
  // TODO<joka921> here is a little bit of duplicate code with the end of the
  // joinCandidates lambda;
  if (candidatePlans.size() > 1 || !candidateTriples._triples.empty()) {
    auto tg = createTripleGraph(&candidateTriples);
    auto lastRow = fillDpTab(tg, rootPattern->_filters, candidatePlans).back();
    candidateTriples._triples.clear();
    candidatePlans.clear();
    candidatePlans.push_back(std::move(lastRow));
  }

  // it might be, that we have not yet applied all the filters
  // (it might be, that the last join was optional and introduced new variables)
  if (!candidatePlans.empty()) {
    applyFiltersIfPossible(candidatePlans[0], rootPattern->_filters, true);
  }

  AD_CONTRACT_CHECK(candidatePlans.size() == 1 || candidatePlans.empty());
  if (candidatePlans.empty()) {
    // this case is needed e.g. if we have the empty graph pattern due to a
    // pattern trick
    return std::vector<SubtreePlan>{};
  } else {
    return candidatePlans[0];
  }
}

// _____________________________________________________________________________
vector<QueryPlanner::SubtreePlan> QueryPlanner::getDistinctRow(
    const p::SelectClause& selectClause,
    const vector<vector<SubtreePlan>>& dpTab) const {
  const vector<SubtreePlan>& previous = dpTab[dpTab.size() - 1];
  vector<SubtreePlan> added;
  added.reserve(previous.size());
  for (const auto& parent : previous) {
    SubtreePlan distinctPlan(_qec);
    vector<ColumnIndex> keepIndices;
    ad_utility::HashSet<ColumnIndex> indDone;
    const auto& colMap = parent._qet->getVariableColumns();
    for (const auto& var : selectClause.getSelectedVariables()) {
      // There used to be a special treatment for `?ql_textscore_` variables
      // which was considered a bug.
      if (auto it = colMap.find(var); it != colMap.end()) {
        auto ind = it->second.columnIndex_;
        if (indDone.count(ind) == 0) {
          keepIndices.push_back(ind);
          indDone.insert(ind);
        }
      }
    }
    const std::vector<ColumnIndex>& resultSortedOn =
        parent._qet->getRootOperation()->getResultSortedOn();
    // check if the current result is sorted on all columns of the distinct
    // with the order of the sorting
    bool isSorted = resultSortedOn.size() >= keepIndices.size();
    for (size_t i = 0; isSorted && i < keepIndices.size(); i++) {
      isSorted = isSorted && resultSortedOn[i] == keepIndices[i];
    }
    if (isSorted) {
      distinctPlan._qet =
          makeExecutionTree<Distinct>(_qec, parent._qet, keepIndices);
    } else {
      auto tree = makeExecutionTree<Sort>(_qec, parent._qet, keepIndices);
      distinctPlan._qet = makeExecutionTree<Distinct>(_qec, tree, keepIndices);
    }
    added.push_back(distinctPlan);
  }
  return added;
}

// _____________________________________________________________________________
vector<QueryPlanner::SubtreePlan> QueryPlanner::getPatternTrickRow(
    const p::SelectClause& selectClause,
    const vector<vector<SubtreePlan>>& dpTab,
    const checkUsePatternTrick::PatternTrickTuple& patternTrickTuple) {
  const vector<SubtreePlan>* previous = nullptr;
  auto aliases = selectClause.getAliases();
  if (!dpTab.empty()) {
    previous = &dpTab.back();
  }
  vector<SubtreePlan> added;

  Variable predicateVariable = patternTrickTuple.predicate_;
  Variable countVariable =
      aliases.empty() ? generateUniqueVarName() : aliases[0]._target;
  if (previous != nullptr && !previous->empty()) {
    added.reserve(previous->size());
    for (const auto& parent : *previous) {
      // Determine the column containing the subjects for which we are
      // interested in their predicates.
      auto subjectColumn =
          parent._qet->getVariableColumn(patternTrickTuple.subject_);
      auto patternTrickPlan = makeSubtreePlan<CountAvailablePredicates>(
          _qec, parent._qet, subjectColumn, predicateVariable, countVariable);
      added.push_back(std::move(patternTrickPlan));
    }
  } else {
    // Use the pattern trick without a subtree
    SubtreePlan patternTrickPlan = makeSubtreePlan<CountAvailablePredicates>(
        _qec, predicateVariable, countVariable);
    added.push_back(std::move(patternTrickPlan));
  }
  return added;
}

// _____________________________________________________________________________
vector<QueryPlanner::SubtreePlan> QueryPlanner::getHavingRow(
    const ParsedQuery& pq, const vector<vector<SubtreePlan>>& dpTab) const {
  const vector<SubtreePlan>& previous = dpTab[dpTab.size() - 1];
  vector<SubtreePlan> added;
  added.reserve(previous.size());
  for (const auto& parent : previous) {
    SubtreePlan filtered = parent;
    for (const SparqlFilter& filter : pq._havingClauses) {
      filtered =
          makeSubtreePlan<Filter>(_qec, filtered._qet, filter.expression_);
    }
    added.push_back(std::move(filtered));
  }
  return added;
}

// _____________________________________________________________________________
vector<QueryPlanner::SubtreePlan> QueryPlanner::getGroupByRow(
    const ParsedQuery& pq, const vector<vector<SubtreePlan>>& dpTab) const {
  const vector<SubtreePlan>& previous = dpTab[dpTab.size() - 1];
  vector<SubtreePlan> added;
  added.reserve(previous.size());
  for (auto& parent : previous) {
    // Create a group by operation to determine on which columns the input
    // needs to be sorted
    SubtreePlan groupByPlan(_qec);
    groupByPlan._idsOfIncludedNodes = parent._idsOfIncludedNodes;
    groupByPlan._idsOfIncludedFilters = parent._idsOfIncludedFilters;
    std::vector<Alias> aliases;
    if (pq.hasSelectClause()) {
      aliases = pq.selectClause().getAliases();
    }

    // The GroupBy constructor automatically takes care of sorting the input if
    // necessary.
    groupByPlan._qet = makeExecutionTree<GroupBy>(
        _qec, pq._groupByVariables, std::move(aliases), parent._qet);
    added.push_back(groupByPlan);
  }
  return added;
}

// _____________________________________________________________________________
vector<QueryPlanner::SubtreePlan> QueryPlanner::getOrderByRow(
    const ParsedQuery& pq, const vector<vector<SubtreePlan>>& dpTab) const {
  const vector<SubtreePlan>& previous = dpTab[dpTab.size() - 1];
  vector<SubtreePlan> added;
  added.reserve(previous.size());
  for (const auto& parent : previous) {
    SubtreePlan plan(_qec);
    auto& tree = plan._qet;
    plan._idsOfIncludedNodes = parent._idsOfIncludedNodes;
    plan._idsOfIncludedFilters = parent._idsOfIncludedFilters;
    vector<pair<ColumnIndex, bool>> sortIndices;
    for (auto& ord : pq._orderBy) {
      sortIndices.emplace_back(parent._qet->getVariableColumn(ord.variable_),
                               ord.isDescending_);
    }

    if (pq._isInternalSort == IsInternalSort::True) {
      std::vector<ColumnIndex> sortColumns;
      for (auto& [index, isDescending] : sortIndices) {
        AD_CONTRACT_CHECK(!isDescending);
        sortColumns.push_back(index);
      }
      tree = QueryExecutionTree::createSortedTree(parent._qet, sortColumns);
    } else {
      AD_CONTRACT_CHECK(pq._isInternalSort == IsInternalSort::False);
      // Note: As the internal ordering is different from the semantic ordering
      // needed by `OrderBy`, we always have to instantiate the `OrderBy`
      // operation.
      tree = makeExecutionTree<OrderBy>(_qec, parent._qet, sortIndices);
    }
    added.push_back(plan);
  }
  return added;
}

void QueryPlanner::addNodeToTripleGraph(const TripleGraph::Node& node,
                                        QueryPlanner::TripleGraph& tg) const {
  // TODO<joka921> This needs quite some refactoring: The IDs of the nodes have
  // to be ascending as an invariant, so we can store all the nodes in a
  // vector<unique_ptr> or even a plain vector.
  tg._nodeStorage.emplace_back(node);
  auto& addedNode = tg._nodeStorage.back();
  tg._nodeMap[addedNode.id_] = &addedNode;
  tg._adjLists.emplace_back();
  AD_CORRECTNESS_CHECK(tg._adjLists.size() == tg._nodeStorage.size());
  AD_CORRECTNESS_CHECK(tg._adjLists.size() == addedNode.id_ + 1);
  // Now add an edge between the added node and every node sharing a var.
  for (auto& addedNodevar : addedNode._variables) {
    for (size_t i = 0; i < addedNode.id_; ++i) {
      auto& otherNode = *tg._nodeMap[i];
      if (otherNode._variables.contains(addedNodevar)) {
        // There is an edge between *it->second and the node with id "id".
        tg._adjLists[addedNode.id_].push_back(otherNode.id_);
        tg._adjLists[otherNode.id_].push_back(addedNode.id_);
      }
    }
  }
}

// _____________________________________________________________________________
QueryPlanner::TripleGraph QueryPlanner::createTripleGraph(
    const p::BasicGraphPattern* pattern) const {
  TripleGraph tg;
  size_t numNodesInTripleGraph = 0;
  ad_utility::HashMap<Variable, string> optTermForCvar;
  ad_utility::HashMap<Variable, vector<string>> potentialTermsForCvar;
  vector<const SparqlTriple*> entityTriples;
  // Add one or more nodes for each triple.
  for (auto& t : pattern->_triples) {
    if (t._p._iri == CONTAINS_WORD_PREDICATE) {
      std::string buffer = t._o.toString();
      std::string_view sv{buffer};
      // Add one node for each word
      for (const auto& term :
           absl::StrSplit(sv.substr(1, sv.size() - 2), ' ')) {
        std::string s{ad_utility::utf8ToLower(term)};
        potentialTermsForCvar[t._s.getVariable()].push_back(s);
        addNodeToTripleGraph(
            TripleGraph::Node(tg._nodeStorage.size(), t._s.getVariable(), s, t),
            tg);
        numNodesInTripleGraph++;
      }
    } else if (t._p._iri == CONTAINS_ENTITY_PREDICATE) {
      entityTriples.push_back(&t);
    } else {
      addNodeToTripleGraph(TripleGraph::Node(tg._nodeStorage.size(), t), tg);
      numNodesInTripleGraph++;
    }
  }
  for (const auto& [cvar, terms] : potentialTermsForCvar) {
    optTermForCvar[cvar] =
        terms[_qec->getIndex().getIndexOfBestSuitedElTerm(terms)];
  }
  for (const SparqlTriple* t : entityTriples) {
    Variable currentVar = t->_s.getVariable();
    if (!optTermForCvar.contains(currentVar)) {
      AD_THROW(
          "Missing ql:contains-word statement. A ql:contains-entity "
          "statement always also needs corresponding ql:contains-word "
          "statement.");
    }
    addNodeToTripleGraph(TripleGraph::Node(tg._nodeStorage.size(), currentVar,
                                           optTermForCvar[currentVar], *t),
                         tg);
    numNodesInTripleGraph++;
  }
  if (numNodesInTripleGraph > 64) {
    AD_THROW("At most 64 triples allowed at the moment.");
  }
  return tg;
}

// _____________________________________________________________________________
template <typename PushPlanFunction, typename AddedIndexScanFunction>
void QueryPlanner::indexScanSingleVarCase(
    const TripleGraph::Node& node, const PushPlanFunction& pushPlan,
    const AddedIndexScanFunction& addIndexScan) {
  using enum Permutation::Enum;

  // TODO: The case where the same variable appears in subject + predicate or
  // object + predicate is missing here and leads to an assertion failure.
  if (isVariable(node.triple_._s) && isVariable(node.triple_._o) &&
      node.triple_._s == node.triple_._o) {
    if (isVariable(node.triple_._p._iri)) {
      AD_THROW("Triple with one variable repeated three times");
    }
    LOG(DEBUG) << "Subject variable same as object variable" << std::endl;
    // Need to handle this as IndexScan with a new unique
    // variable + Filter. Works in both directions
    Variable filterVar = generateUniqueVarName();
    auto scanTriple = node.triple_;
    scanTriple._o = filterVar;
    auto scanTree = makeExecutionTree<IndexScan>(_qec, PSO, scanTriple);
    // The simplest way to set up the filtering expression is to use the
    // parser.
    std::string filterString =
        absl::StrCat("FILTER (", scanTriple._s.getVariable().name(), "=",
                     filterVar.name(), ")");
    auto filter = sparqlParserHelpers::ParserAndVisitor{filterString}
                      .parseTypesafe(&SparqlAutomaticParser::filterR)
                      .resultOfParse_;
    auto plan =
        makeSubtreePlan<Filter>(_qec, scanTree, std::move(filter.expression_));
    pushPlan(std::move(plan));
  } else if (isVariable(node.triple_._s)) {
    addIndexScan(POS);
  } else if (isVariable(node.triple_._o)) {
    addIndexScan(PSO);
  } else {
    AD_CONTRACT_CHECK(isVariable(node.triple_._p));
    addIndexScan(SOP);
  }
}

// _____________________________________________________________________________
template <typename AddedIndexScanFunction>
void QueryPlanner::indexScanTwoVarsCase(
    const TripleGraph::Node& node,
    const AddedIndexScanFunction& addIndexScan) const {
  using enum Permutation::Enum;

  // TODO: The case that the same variable appears in more than one position
  // leads (as in indexScanSingleVarCase) to an assertion.
  if (!isVariable(node.triple_._p._iri)) {
    addIndexScan(PSO);
    addIndexScan(POS);
  } else if (!isVariable(node.triple_._s)) {
    addIndexScan(SPO);
    addIndexScan(SOP);
  } else if (!isVariable(node.triple_._o)) {
    addIndexScan(OSP);
    addIndexScan(OPS);
  }
}

// _____________________________________________________________________________
template <typename AddedIndexScanFunction>
void QueryPlanner::indexScanThreeVarsCase(
    const TripleGraph::Node& node,
    const AddedIndexScanFunction& addIndexScan) const {
  using enum Permutation::Enum;

  if (!_qec || _qec->getIndex().hasAllPermutations()) {
    // Add plans for all six permutations.
    addIndexScan(OPS);
    addIndexScan(OSP);
    addIndexScan(PSO);
    addIndexScan(POS);
    addIndexScan(SPO);
    addIndexScan(SOP);
  } else {
    AD_THROW(
        "With only 2 permutations registered (no -a option), "
        "triples should have at most two variables. "
        "Not the case in: " +
        node.triple_.asString());
  }
}

// _____________________________________________________________________________
template <typename PushPlanFunction, typename AddedIndexScanFunction>
void QueryPlanner::seedFromOrdinaryTriple(
    const TripleGraph::Node& node, const PushPlanFunction& pushPlan,
    const AddedIndexScanFunction& addIndexScan) {
  if (node._variables.size() == 1) {
    indexScanSingleVarCase(node, pushPlan, addIndexScan);
  } else if (node._variables.size() == 2) {
    indexScanTwoVarsCase(node, addIndexScan);
  } else {
    indexScanThreeVarsCase(node, addIndexScan);
  }
}

// _____________________________________________________________________________
vector<QueryPlanner::SubtreePlan> QueryPlanner::seedWithScansAndText(
    const QueryPlanner::TripleGraph& tg,
    const vector<vector<QueryPlanner::SubtreePlan>>& children) {
  vector<SubtreePlan> seeds;
  // add all child plans as seeds
  uint64_t idShift = tg._nodeMap.size();
  for (const auto& vec : children) {
    for (const SubtreePlan& plan : vec) {
      SubtreePlan newIdPlan = plan;
      // give the plan a unique id bit
      newIdPlan._idsOfIncludedNodes = uint64_t(1) << idShift;
      newIdPlan._idsOfIncludedFilters = 0;
      seeds.emplace_back(newIdPlan);
    }
    idShift++;
  }
  for (size_t i = 0; i < tg._nodeMap.size(); ++i) {
    const TripleGraph::Node& node = *tg._nodeMap.find(i)->second;

    auto pushPlan = [&seeds, i](SubtreePlan plan) {
      plan._idsOfIncludedNodes = (uint64_t(1) << i);
      seeds.push_back(std::move(plan));
    };

    auto addIndexScan = [this, pushPlan, node](Permutation::Enum permutation) {
      pushPlan(makeSubtreePlan<IndexScan>(_qec, permutation, node.triple_));
    };

    using enum Permutation::Enum;

    if (node.isTextNode()) {
      seeds.push_back(getTextLeafPlan(node));
      continue;
    }
    if (node._variables.empty()) {
      AD_THROW("Triples should have at least one variable. Not the case in: " +
               node.triple_.asString());
    }

    // If the predicate is a property path, we have to recursively set up the
    // index scans.
    if (node.triple_._p._operation != PropertyPath::Operation::IRI) {
      for (SubtreePlan& plan : seedFromPropertyPathTriple(node.triple_)) {
        pushPlan(std::move(plan));
      }
      continue;
    }

    // At this point, we know that the predicate is a simple IRI or a variable.

    if (_qec && !_qec->getIndex().hasAllPermutations() &&
        isVariable(node.triple_._p._iri)) {
      AD_THROW(
          "The query contains a predicate variable, but only the PSO "
          "and POS permutations were loaded. Rerun the server without "
          "the option --only-pso-and-pos-permutations and if "
          "necessary also rebuild the index.");
    }

    if (node.triple_._p._iri == HAS_PREDICATE_PREDICATE) {
      pushPlan(makeSubtreePlan<HasPredicateScan>(_qec, node.triple_));
      continue;
    }

    seedFromOrdinaryTriple(node, pushPlan, addIndexScan);
  }
  return seeds;
}

// _____________________________________________________________________________
vector<QueryPlanner::SubtreePlan> QueryPlanner::seedFromPropertyPathTriple(
    const SparqlTriple& triple) {
  std::shared_ptr<ParsedQuery::GraphPattern> pattern =
      seedFromPropertyPath(triple._s, triple._p, triple._o);
#if LOGLEVEL >= TRACE
  std::ostringstream out;
  pattern->toString(out, 0);
  LOG(TRACE) << "Turned " << triple.asString() << " into " << std::endl;
  LOG(TRACE) << std::move(out).str() << std::endl << std::endl;
#endif
  pattern->recomputeIds();
  return optimize(pattern.get());
}

std::shared_ptr<ParsedQuery::GraphPattern> QueryPlanner::seedFromPropertyPath(
    const TripleComponent& left, const PropertyPath& path,
    const TripleComponent& right) {
  switch (path._operation) {
    case PropertyPath::Operation::ALTERNATIVE:
      return seedFromAlternative(left, path, right);
    case PropertyPath::Operation::INVERSE:
      return seedFromInverse(left, path, right);
    case PropertyPath::Operation::IRI:
      return seedFromIri(left, path, right);
    case PropertyPath::Operation::SEQUENCE:
      return seedFromSequence(left, path, right);
    case PropertyPath::Operation::ZERO_OR_MORE:
      return seedFromTransitive(left, path, right, 0,
                                std::numeric_limits<size_t>::max());
    case PropertyPath::Operation::ONE_OR_MORE:
      return seedFromTransitive(left, path, right, 1,
                                std::numeric_limits<size_t>::max());
    case PropertyPath::Operation::ZERO_OR_ONE:
      return seedFromTransitive(left, path, right, 0, 1);
  }
  AD_FAIL();
}

// _____________________________________________________________________________
std::shared_ptr<ParsedQuery::GraphPattern> QueryPlanner::seedFromSequence(
    const TripleComponent& left, const PropertyPath& path,
    const TripleComponent& right) {
  AD_CORRECTNESS_CHECK(path._children.size() > 1);

  auto joinPattern = ParsedQuery::GraphPattern{};
  TripleComponent innerLeft = left;
  TripleComponent innerRight = generateUniqueVarName();
  for (size_t i = 0; i < path._children.size(); i++) {
    auto child = path._children[i];

    if (i == path._children.size() - 1) {
      innerRight = right;
    }

    auto pattern = seedFromPropertyPath(innerLeft, child, innerRight);
    joinPattern._graphPatterns.insert(joinPattern._graphPatterns.end(),
                                      pattern->_graphPatterns.begin(),
                                      pattern->_graphPatterns.end());
    innerLeft = innerRight;
    innerRight = generateUniqueVarName();
  }

  return std::make_shared<ParsedQuery::GraphPattern>(joinPattern);
}

// _____________________________________________________________________________
std::shared_ptr<ParsedQuery::GraphPattern> QueryPlanner::seedFromAlternative(
    const TripleComponent& left, const PropertyPath& path,
    const TripleComponent& right) {
  if (path._children.empty()) {
    AD_THROW(
        "Tried processing an alternative property path node without any "
        "children.");
  } else if (path._children.size() == 1) {
    LOG(WARN)
        << "Processing an alternative property path that has only one child."
        << std::endl;
    return seedFromPropertyPath(left, path, right);
  }

  std::vector<std::shared_ptr<ParsedQuery::GraphPattern>> childPlans;
  childPlans.reserve(path._children.size());
  for (const auto& child : path._children) {
    childPlans.push_back(seedFromPropertyPath(left, child, right));
  }
  // todo<joka921> refactor this nonsense recursively by getting rid of the
  // shared_ptrs everywhere
  std::vector<ParsedQuery::GraphPattern> tmp;
  for (const auto& el : childPlans) {
    tmp.push_back(*el);
  }

  return std::make_shared<ParsedQuery::GraphPattern>(
      uniteGraphPatterns(std::move(tmp)));
}

// _____________________________________________________________________________
std::shared_ptr<ParsedQuery::GraphPattern> QueryPlanner::seedFromTransitive(
    const TripleComponent& left, const PropertyPath& path,
    const TripleComponent& right, size_t min, size_t max) {
  Variable innerLeft = generateUniqueVarName();
  Variable innerRight = generateUniqueVarName();
  std::shared_ptr<ParsedQuery::GraphPattern> childPlan =
      seedFromPropertyPath(innerLeft, path._children[0], innerRight);
  std::shared_ptr<ParsedQuery::GraphPattern> p =
      std::make_shared<ParsedQuery::GraphPattern>();
  p::TransPath transPath;
  transPath._left = left;
  transPath._right = right;
  transPath._innerLeft = innerLeft;
  transPath._innerRight = innerRight;
  transPath._min = min;
  transPath._max = max;
  transPath._childGraphPattern = *childPlan;
  p->_graphPatterns.emplace_back(std::move(transPath));
  return p;
}

// _____________________________________________________________________________
std::shared_ptr<ParsedQuery::GraphPattern> QueryPlanner::seedFromInverse(
    const TripleComponent& left, const PropertyPath& path,
    const TripleComponent& right) {
  return seedFromPropertyPath(right, path._children[0], left);
}

// _____________________________________________________________________________
std::shared_ptr<ParsedQuery::GraphPattern> QueryPlanner::seedFromIri(
    const TripleComponent& left, const PropertyPath& path,
    const TripleComponent& right) {
  std::shared_ptr<ParsedQuery::GraphPattern> p =
      std::make_shared<ParsedQuery::GraphPattern>();
  p::BasicGraphPattern basic;
  basic._triples.push_back(SparqlTriple(left, path, right));
  p->_graphPatterns.emplace_back(std::move(basic));

  return p;
}

ParsedQuery::GraphPattern QueryPlanner::uniteGraphPatterns(
    std::vector<ParsedQuery::GraphPattern>&& patterns) const {
  using GraphPattern = ParsedQuery::GraphPattern;
  // Build a tree of union operations
  auto p = GraphPattern{};
  p._graphPatterns.emplace_back(
      p::Union{std::move(patterns[0]), std::move(patterns[1])});

  for (size_t i = 2; i < patterns.size(); i++) {
    GraphPattern next;
    next._graphPatterns.emplace_back(
        p::Union{std::move(p), std::move(patterns[i])});
    p = std::move(next);
  }
  return p;
}

// _____________________________________________________________________________
Variable QueryPlanner::generateUniqueVarName() {
  return Variable{"?_qlever_internal_variable_query_planner_" +
                  std::to_string(_internalVarCount++)};
}

// _____________________________________________________________________________
QueryPlanner::SubtreePlan QueryPlanner::getTextLeafPlan(
    const QueryPlanner::TripleGraph::Node& node) const {
  AD_CONTRACT_CHECK(node.wordPart_.has_value());
  string word = node.wordPart_.value();
  SubtreePlan plan(_qec);
  if (node.triple_._p._iri == CONTAINS_ENTITY_PREDICATE) {
    if (node._variables.size() == 2) {
      // TODO<joka921>: This is not nice, refactor the whole TripleGraph class
      // to make these checks more explicity.
      Variable evar = *(node._variables.begin()) == node.cvar_.value()
                          ? *(++node._variables.begin())
                          : *(node._variables.begin());
      plan = makeSubtreePlan<TextIndexScanForEntity>(_qec, node.cvar_.value(),
                                                     evar, word);
    } else {
      // Fixed entity case
      AD_CORRECTNESS_CHECK(node._variables.size() == 1);
      plan = makeSubtreePlan<TextIndexScanForEntity>(
          _qec, node.cvar_.value(), node.triple_._o.toString(), word);
    }
  } else {
    plan =
        makeSubtreePlan<TextIndexScanForWord>(_qec, node.cvar_.value(), word);
  }
  plan._idsOfIncludedNodes |= (size_t(1) << node.id_);
  return plan;
}

// _____________________________________________________________________________
vector<QueryPlanner::SubtreePlan> QueryPlanner::merge(
    const vector<QueryPlanner::SubtreePlan>& a,
    const vector<QueryPlanner::SubtreePlan>& b,
    const QueryPlanner::TripleGraph& tg) const {
  // TODO: Add the following features:
  // If a join is supposed to happen, always check if it happens between
  // a scan with a relatively large result size
  // esp. with an entire relation but also with something like is-a Person
  // If that is the case look at the size estimate for the other side,
  // if that is rather small, replace the join and scan by a combination.
  ad_utility::HashMap<string, vector<SubtreePlan>> candidates;
  // Find all pairs between a and b that are connected by an edge.
  LOG(TRACE) << "Considering joins that merge " << a.size() << " and "
             << b.size() << " plans...\n";
  for (const auto& ai : a) {
    for (const auto& bj : b) {
      LOG(TRACE) << "Creating join candidates for " << ai._qet->getCacheKey()
                 << "\n and " << bj._qet->getCacheKey() << '\n';
      auto v = createJoinCandidates(ai, bj, tg);
      for (auto& plan : v) {
        candidates[getPruningKey(plan, plan._qet->resultSortedOn())]
            .emplace_back(std::move(plan));
      }
    }
  }

  // Duplicates are removed if the same triples are touched,
  // the ordering is the same. Only the best is kept then.

  // Therefore we mapped plans and use contained triples + ordering var
  // as key.
  LOG(TRACE) << "Pruning...\n";
  vector<SubtreePlan> prunedPlans;

  auto pruneCandidates = [&](auto& actualCandidates) {
    for (auto& [key, value] : actualCandidates) {
      (void)key;  // silence unused warning
      size_t minIndex = findCheapestExecutionTree(value);
      prunedPlans.push_back(std::move(value[minIndex]));
    }
  };

  if (isInTestMode()) {
    std::vector<std::pair<string, vector<SubtreePlan>>> sortedCandidates{
        std::make_move_iterator(candidates.begin()),
        std::make_move_iterator(candidates.end())};
    std::sort(sortedCandidates.begin(), sortedCandidates.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    pruneCandidates(sortedCandidates);
  } else {
    pruneCandidates(candidates);
  }

  LOG(TRACE) << "Got " << prunedPlans.size() << " pruned plans from \n";
  return prunedPlans;
}

// _____________________________________________________________________________
string QueryPlanner::TripleGraph::asString() const {
  std::ostringstream os;
  for (size_t i = 0; i < _adjLists.size(); ++i) {
    if (!_nodeMap.find(i)->second->cvar_.has_value()) {
      os << i << " " << _nodeMap.find(i)->second->triple_.asString() << " : (";
    } else {
      os << i << " {TextOP for "
         << _nodeMap.find(i)->second->cvar_.value().name() << ", wordPart: \""
         << _nodeMap.find(i)->second->wordPart_.value() << "\"} : (";
    }

    for (size_t j = 0; j < _adjLists[i].size(); ++j) {
      os << _adjLists[i][j];
      if (j < _adjLists[i].size() - 1) {
        os << ", ";
      }
    }
    os << ')';
    if (i < _adjLists.size() - 1) {
      os << '\n';
    }
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
size_t QueryPlanner::SubtreePlan::getCostEstimate() const {
  return _qet->getCostEstimate();
}

// _____________________________________________________________________________
size_t QueryPlanner::SubtreePlan::getSizeEstimate() const {
  return _qet->getSizeEstimate();
}

// _____________________________________________________________________________
void QueryPlanner::SubtreePlan::addAllNodes(uint64_t otherNodes) {
  _idsOfIncludedNodes |= otherNodes;
}

// _____________________________________________________________________________
bool QueryPlanner::connected(const QueryPlanner::SubtreePlan& a,
                             const QueryPlanner::SubtreePlan& b,
                             const QueryPlanner::TripleGraph& tg) const {
  // Check if there is overlap.
  // If so, don't consider them as properly connected.
  if ((a._idsOfIncludedNodes & b._idsOfIncludedNodes) != 0) {
    return false;
  }

  if (a._idsOfIncludedNodes >= (size_t(1) << tg._nodeMap.size()) ||
      b._idsOfIncludedNodes >= (size_t(1) << tg._nodeMap.size())) {
    return getJoinColumns(a, b).size() > 0;
  }

  for (size_t i = 0; i < tg._nodeMap.size(); ++i) {
    if (((a._idsOfIncludedNodes >> i) & 1) == 0) {
      continue;
    }
    auto& connectedNodes = tg._adjLists[i];
    for (auto targetNodeId : connectedNodes) {
      if ((((a._idsOfIncludedNodes >> targetNodeId) & 1) == 0) &&
          (((b._idsOfIncludedNodes >> targetNodeId) & 1) != 0)) {
        return true;
      }
    }
  }
  return false;
}

// _____________________________________________________________________________
std::vector<std::array<ColumnIndex, 2>> QueryPlanner::getJoinColumns(
    const QueryPlanner::SubtreePlan& a,
    const QueryPlanner::SubtreePlan& b) const {
  AD_CORRECTNESS_CHECK(a._qet && b._qet);
  return QueryExecutionTree::getJoinColumns(*a._qet, *b._qet);
}

// _____________________________________________________________________________
string QueryPlanner::getPruningKey(
    const QueryPlanner::SubtreePlan& plan,
    const vector<ColumnIndex>& orderedOnColumns) const {
  // Get the ordered var
  std::ostringstream os;
  const auto& varCols = plan._qet->getVariableColumns();
  for (ColumnIndex orderedOnCol : orderedOnColumns) {
    for (const auto& [variable, columnIndexWithType] : varCols) {
      if (columnIndexWithType.columnIndex_ == orderedOnCol) {
        os << variable.name() << ", ";
        break;
      }
    }
  }

  os << ' ' << plan._idsOfIncludedNodes;
  os << " f: ";
  os << ' ' << plan._idsOfIncludedFilters;

  return std::move(os).str();
}

// _____________________________________________________________________________
void QueryPlanner::applyFiltersIfPossible(
    vector<QueryPlanner::SubtreePlan>& row, const vector<SparqlFilter>& filters,
    bool replace) const {
  // Apply every filter possible.
  // It is possible when,
  // 1) the filter has not already been applied
  // 2) all variables in the filter are covered by the query so far
  // New 06 May 2016:
  // There is a problem with the so-called (name may be changed)
  // TextOperationWithFilter ops: This method applies SPARQL filters
  // to all the leaf TextOperations (when feasible) and thus
  // prevents the special case from being applied when subtrees are merged.
  // Fix: Also copy (CHANGE not all plans but TextOperation) without applying
  // the filter. Problem: If the method gets called multiple times, plans with
  // filters May be duplicated. To prevent this, calling code has to ensure
  // That the method is only called once on each row. Similarly this affects
  // the (albeit rare) fact that a filter is directly applicable after a scan
  // of a huge relation where a subsequent join with a small result could be
  // translated into one or more scans directly. This also helps with cases
  // where applying the filter later is better. Finally, the replace flag can
  // be set to enforce that all filters are applied. This should be done for
  // the last row in the DPTab so that no filters are missed.

  // Note: we are first collecting the newly added plans and then adding them
  // in one go. Changing `row` inside the loop would invalidate the iterators.
  std::vector<SubtreePlan> addedPlans;
  for (auto& plan : row) {
    if (plan._qet->getType() == QueryExecutionTree::SCAN &&
        plan._qet->getResultWidth() == 3 && !replace) {
      // Do not apply filters to dummies, except at the very end of query
      // planning.
      continue;
    }
    for (size_t i = 0; i < filters.size(); ++i) {
      if (((plan._idsOfIncludedFilters >> i) & 1) != 0) {
        continue;
      }

      if (std::ranges::all_of(filters[i].expression_.containedVariables(),
                              [&plan](const auto& variable) {
                                return plan._qet->isVariableCovered(*variable);
                              })) {
        // Apply this filter.
        SubtreePlan newPlan =
            makeSubtreePlan<Filter>(_qec, plan._qet, filters[i].expression_);
        newPlan._idsOfIncludedFilters = plan._idsOfIncludedFilters;
        newPlan._idsOfIncludedFilters |= (size_t(1) << i);
        newPlan._idsOfIncludedNodes = plan._idsOfIncludedNodes;
        newPlan.type = plan.type;
        if (replace) {
          plan = std::move(newPlan);
        } else {
          addedPlans.push_back(std::move(newPlan));
        }
      }
    }
  }
  row.insert(row.end(), addedPlans.begin(), addedPlans.end());
}

// _____________________________________________________________________________
std::vector<QueryPlanner::SubtreePlan>
QueryPlanner::runDynamicProgrammingOnConnectedComponent(
    std::vector<SubtreePlan> connectedComponent,
    const vector<SparqlFilter>& filters, const TripleGraph& tg) const {
  vector<vector<QueryPlanner::SubtreePlan>> dpTab;
  // find the unique number of nodes in the current connected component
  // (there might be duplicates because we already have multiple candidates
  // for each index scan with different permutations.
  dpTab.push_back(std::move(connectedComponent));
  applyFiltersIfPossible(dpTab.back(), filters, false);
  ad_utility::HashSet<uint64_t> uniqueNodeIds;
  std::ranges::copy(
      dpTab.back() | std::views::transform(&SubtreePlan::_idsOfIncludedNodes),
      std::inserter(uniqueNodeIds, uniqueNodeIds.end()));
  size_t numSeeds = uniqueNodeIds.size();

  for (size_t k = 2; k <= numSeeds; ++k) {
    LOG(TRACE) << "Producing plans that unite " << k << " triples."
               << std::endl;
    dpTab.emplace_back(vector<SubtreePlan>());
    for (size_t i = 1; i * 2 <= k; ++i) {
      auto newPlans = merge(dpTab[i - 1], dpTab[k - i - 1], tg);
      dpTab[k - 1].insert(dpTab[k - 1].end(), newPlans.begin(), newPlans.end());
      applyFiltersIfPossible(dpTab.back(), filters, false);
    }
    // As we only passed in connected components, we expect the result to always
    // be nonempty.
    AD_CORRECTNESS_CHECK(!dpTab[k - 1].empty());
  }
  return std::move(dpTab.back());
}

// _____________________________________________________________________________
vector<vector<QueryPlanner::SubtreePlan>> QueryPlanner::fillDpTab(
    const QueryPlanner::TripleGraph& tg, const vector<SparqlFilter>& filters,
    const vector<vector<QueryPlanner::SubtreePlan>>& children) {
  if (filters.size() > 64) {
    AD_THROW("At most 64 filters allowed at the moment.");
  }
  auto initialPlans = seedWithScansAndText(tg, children);
  auto componentIndices = QueryGraph::computeConnectedComponents(initialPlans);
  ad_utility::HashMap<size_t, std::vector<SubtreePlan>> components;
  for (size_t i = 0; i < componentIndices.size(); ++i) {
    components[componentIndices.at(i)].push_back(std::move(initialPlans.at(i)));
  }
  vector<vector<SubtreePlan>> lastDpRowFromComponents;
  for (auto& component : components | std::views::values) {
    lastDpRowFromComponents.push_back(runDynamicProgrammingOnConnectedComponent(
        std::move(component), filters, tg));
  }
  size_t numConnectedComponents = lastDpRowFromComponents.size();
  if (numConnectedComponents == 0) {
    // This happens for example if there is a BIND right at the beginning of the
    // query
    lastDpRowFromComponents.emplace_back();
    return lastDpRowFromComponents;
  }
  if (numConnectedComponents == 1) {
    // A Cartesian product is not needed if there is only one component.
    applyFiltersIfPossible(lastDpRowFromComponents.back(), filters, true);
    return lastDpRowFromComponents;
  }
  // More than one connected component, set up a Cartesian product.
  std::vector<std::vector<SubtreePlan>> result;
  result.emplace_back();
  std::vector<std::shared_ptr<QueryExecutionTree>> subtrees;
  std::ranges::move(
      lastDpRowFromComponents |
          std::views::transform([this](auto& vec) -> decltype(auto) {
            return vec.at(findCheapestExecutionTree(vec));
          }) |
          std::views::transform(&SubtreePlan::_qet),
      std::back_inserter(subtrees));
  result.at(0).push_back(
      makeSubtreePlan<CartesianProductJoin>(_qec, std::move(subtrees)));
  applyFiltersIfPossible(result.at(0), filters, true);
  return result;
}

// _____________________________________________________________________________
bool QueryPlanner::TripleGraph::isTextNode(size_t i) const {
  return _nodeMap.count(i) > 0 &&
         (_nodeMap.find(i)->second->triple_._p._iri ==
              CONTAINS_ENTITY_PREDICATE ||
          _nodeMap.find(i)->second->triple_._p._iri == CONTAINS_WORD_PREDICATE);
}

// _____________________________________________________________________________
vector<pair<QueryPlanner::TripleGraph, vector<SparqlFilter>>>
QueryPlanner::TripleGraph::splitAtContextVars(
    const vector<SparqlFilter>& origFilters,
    ad_utility::HashMap<string, vector<size_t>>& contextVarToTextNodes) const {
  vector<pair<QueryPlanner::TripleGraph, vector<SparqlFilter>>> retVal;
  // Recursively split the graph a context nodes.
  // Base-case: No no context nodes, return the graph itself.
  if (contextVarToTextNodes.size() == 0) {
    retVal.emplace_back(make_pair(*this, origFilters));
  } else {
    // Just take the first contextVar and split at it.
    ad_utility::HashSet<size_t> textNodeIds;
    textNodeIds.insert(contextVarToTextNodes.begin()->second.begin(),
                       contextVarToTextNodes.begin()->second.end());

    // For the next iteration / recursive call(s):
    // Leave out the first one because it has been worked on in this call.
    ad_utility::HashMap<string, vector<size_t>> cTMapNextIteration;
    cTMapNextIteration.insert(++contextVarToTextNodes.begin(),
                              contextVarToTextNodes.end());

    // Find a node to start the split.
    size_t startNode = 0;
    while (startNode < _adjLists.size() && textNodeIds.count(startNode) > 0) {
      ++startNode;
    }
    // If no start node was found, this means only text triples left.
    // --> don't enter code block below and return empty vector.
    if (startNode != _adjLists.size()) {
      // If we have a start node, do a BFS to obtain a set of reachable nodes
      auto reachableNodes = bfsLeaveOut(startNode, textNodeIds);
      if (reachableNodes.size() == _adjLists.size() - textNodeIds.size()) {
        // Case: cyclic or text operation was on the "outside"
        // -> only one split to work with further.
        // Recursively solve this split
        // (because there may be another context var in it)
        TripleGraph withoutText(*this, reachableNodes);
        vector<SparqlFilter> filters = pickFilters(origFilters, reachableNodes);
        auto recursiveResult =
            withoutText.splitAtContextVars(filters, cTMapNextIteration);
        retVal.insert(retVal.begin(), recursiveResult.begin(),
                      recursiveResult.end());
      } else {
        // Case: The split created two or more non-empty parts.
        // Find all parts so that the number of triples in them plus
        // the number of text triples equals the number of total triples.
        vector<vector<size_t>> setsOfReachablesNodes;
        ad_utility::HashSet<size_t> nodesDone;
        nodesDone.insert(textNodeIds.begin(), textNodeIds.end());
        nodesDone.insert(reachableNodes.begin(), reachableNodes.end());
        setsOfReachablesNodes.emplace_back(reachableNodes);
        assert(nodesDone.size() < _adjLists.size());
        while (nodesDone.size() < _adjLists.size()) {
          while (startNode < _adjLists.size() &&
                 nodesDone.count(startNode) > 0) {
            ++startNode;
          }
          reachableNodes = bfsLeaveOut(startNode, textNodeIds);
          nodesDone.insert(reachableNodes.begin(), reachableNodes.end());
          setsOfReachablesNodes.emplace_back(reachableNodes);
        }
        // Recursively split each part because there may be other context
        // vars.
        for (const auto& rNodes : setsOfReachablesNodes) {
          TripleGraph smallerGraph(*this, rNodes);
          vector<SparqlFilter> filters = pickFilters(origFilters, rNodes);
          auto recursiveResult =
              smallerGraph.splitAtContextVars(filters, cTMapNextIteration);
          retVal.insert(retVal.begin(), recursiveResult.begin(),
                        recursiveResult.end());
        }
      }
    }
  }
  return retVal;
}

// _____________________________________________________________________________
vector<size_t> QueryPlanner::TripleGraph::bfsLeaveOut(
    size_t startNode, ad_utility::HashSet<size_t> leaveOut) const {
  vector<size_t> res;
  ad_utility::HashSet<size_t> visited;
  std::list<size_t> queue;
  queue.push_back(startNode);
  visited.insert(startNode);
  while (!queue.empty()) {
    size_t n = queue.front();
    queue.pop_front();
    res.push_back(n);
    auto& neighbors = _adjLists[n];
    for (size_t v : neighbors) {
      if (visited.count(v) == 0 && leaveOut.count(v) == 0) {
        visited.insert(v);
        queue.push_back(v);
      }
    }
  }
  return res;
}

// _____________________________________________________________________________
vector<SparqlFilter> QueryPlanner::TripleGraph::pickFilters(
    const vector<SparqlFilter>& origFilters,
    const vector<size_t>& nodes) const {
  vector<SparqlFilter> ret;
  ad_utility::HashSet<Variable> coveredVariables;
  for (auto n : nodes) {
    auto& node = *_nodeMap.find(n)->second;
    coveredVariables.insert(node._variables.begin(), node._variables.end());
  }
  for (auto& f : origFilters) {
    if (std::ranges::any_of(
            f.expression_.containedVariables(),
            [&](const auto* var) { return coveredVariables.contains(*var); })) {
      ret.push_back(f);
    }
  }
  return ret;
}

// _____________________________________________________________________________
QueryPlanner::TripleGraph::TripleGraph(
    const std::vector<std::pair<Node, std::vector<size_t>>>& init) {
  for (const std::pair<Node, std::vector<size_t>>& p : init) {
    _nodeStorage.push_back(p.first);
    _nodeMap[p.first.id_] = &_nodeStorage.back();
    _adjLists.push_back(p.second);
  }
}

// _____________________________________________________________________________
QueryPlanner::TripleGraph::TripleGraph(const QueryPlanner::TripleGraph& other,
                                       vector<size_t> keepNodes) {
  ad_utility::HashSet<size_t> keep;
  for (auto v : keepNodes) {
    keep.insert(v);
  }
  // Copy nodes to be kept and assign new node id's.
  // Keep information about the id change in a map.
  ad_utility::HashMap<size_t, size_t> idChange;
  for (size_t i = 0; i < other._nodeMap.size(); ++i) {
    if (keep.count(i) > 0) {
      _nodeStorage.push_back(*other._nodeMap.find(i)->second);
      idChange[i] = _nodeMap.size();
      _nodeStorage.back().id_ = _nodeMap.size();
      _nodeMap[idChange[i]] = &_nodeStorage.back();
    }
  }
  // Adjust adjacency lists accordingly.
  for (size_t i = 0; i < other._adjLists.size(); ++i) {
    if (keep.count(i) > 0) {
      vector<size_t> adjList;
      for (size_t v : other._adjLists[i]) {
        if (keep.count(v) > 0) {
          adjList.push_back(idChange[v]);
        }
      }
      _adjLists.push_back(adjList);
    }
  }
}

// _____________________________________________________________________________
QueryPlanner::TripleGraph::TripleGraph(const TripleGraph& other)
    : _adjLists(other._adjLists), _nodeMap(), _nodeStorage() {
  for (auto it : other._nodeMap) {
    _nodeStorage.push_back(*it.second);
    _nodeMap[it.first] = &_nodeStorage.back();
  }
}

// _____________________________________________________________________________
QueryPlanner::TripleGraph& QueryPlanner::TripleGraph::operator=(
    const TripleGraph& other) {
  _adjLists = other._adjLists;
  for (auto it : other._nodeMap) {
    _nodeStorage.push_back(*it.second);
    _nodeMap[it.first] = &_nodeStorage.back();
  }
  return *this;
}

// _____________________________________________________________________________
QueryPlanner::TripleGraph::TripleGraph()
    : _adjLists(), _nodeMap(), _nodeStorage() {}

// _____________________________________________________________________________
bool QueryPlanner::TripleGraph::isSimilar(
    const QueryPlanner::TripleGraph& other) const {
  // This method is very verbose as it is currently only intended for
  // testing
  if (_nodeStorage.size() != other._nodeStorage.size()) {
    LOG(INFO) << asString() << std::endl;
    LOG(INFO) << other.asString() << std::endl;
    LOG(INFO) << "The two triple graphs are not of the same size: "
              << _nodeStorage.size() << " != " << other._nodeStorage.size()
              << std::endl;
    return false;
  }
  ad_utility::HashMap<size_t, size_t> id_map;
  ad_utility::HashMap<size_t, size_t> id_map_reverse;
  for (const Node& n : _nodeStorage) {
    bool hasMatch = false;
    for (const Node& n2 : other._nodeStorage) {
      if (n.isSimilar(n2)) {
        id_map[n.id_] = n2.id_;
        id_map_reverse[n2.id_] = n.id_;
        hasMatch = true;
        break;
      } else {
      }
    }
    if (!hasMatch) {
      LOG(INFO) << asString() << std::endl;
      LOG(INFO) << other.asString() << std::endl;
      LOG(INFO) << "The node " << n << " has no match in the other graph"
                << std::endl;
      return false;
    }
  }
  if (id_map.size() != _nodeStorage.size() ||
      id_map_reverse.size() != _nodeStorage.size()) {
    LOG(INFO) << asString() << std::endl;
    LOG(INFO) << other.asString() << std::endl;
    LOG(INFO) << "Two nodes in this graph were matches to the same node in "
                 "the other grap"
              << std::endl;
    return false;
  }
  for (size_t id = 0; id < _adjLists.size(); ++id) {
    size_t other_id = id_map[id];
    ad_utility::HashSet<size_t> adj_set;
    ad_utility::HashSet<size_t> other_adj_set;
    for (size_t a : _adjLists[id]) {
      adj_set.insert(a);
    }
    for (size_t a : other._adjLists[other_id]) {
      other_adj_set.insert(a);
    }
    for (size_t a : _adjLists[id]) {
      if (other_adj_set.count(id_map[a]) == 0) {
        LOG(INFO) << asString() << std::endl;
        LOG(INFO) << other.asString() << std::endl;
        LOG(INFO) << "The node with id " << id << " is connected to " << a
                  << " in this graph graph but not to the equivalent "
                     "node in the other graph."
                  << std::endl;
        return false;
      }
    }
    for (size_t a : other._adjLists[other_id]) {
      if (adj_set.count(id_map_reverse[a]) == 0) {
        LOG(INFO) << asString() << std::endl;
        LOG(INFO) << other.asString() << std::endl;
        LOG(INFO) << "The node with id " << id << " is connected to " << a
                  << " in the other graph graph but not to the equivalent "
                     "node in this graph."
                  << std::endl;
        return false;
      }
    }
  }
  return true;
}

// _____________________________________________________________________________
void QueryPlanner::setEnablePatternTrick(bool enablePatternTrick) {
  _enablePatternTrick = enablePatternTrick;
}

// _________________________________________________________________________________
size_t QueryPlanner::findCheapestExecutionTree(
    const std::vector<SubtreePlan>& lastRow) const {
  AD_CONTRACT_CHECK(!lastRow.empty());
  auto compare = [this](const auto& a, const auto& b) {
    auto aCost = a.getCostEstimate(), bCost = b.getCostEstimate();
    if (aCost == bCost && isInTestMode()) {
      // Make the tiebreaking deterministic for the unit tests.
      return a._qet->getCacheKey() < b._qet->getCacheKey();
    } else {
      return aCost < bCost;
    }
  };
  return std::min_element(lastRow.begin(), lastRow.end(), compare) -
         lastRow.begin();
};

// _____________________________________________________________________________
std::vector<QueryPlanner::SubtreePlan> QueryPlanner::createJoinCandidates(
    const SubtreePlan& ain, const SubtreePlan& bin,
    std::optional<TripleGraph> tg) const {
  bool swapForTesting = isInTestMode() && bin.type != SubtreePlan::OPTIONAL &&
                        ain._qet->getCacheKey() < bin._qet->getCacheKey();
  const auto& a = !swapForTesting ? ain : bin;
  const auto& b = !swapForTesting ? bin : ain;
  std::vector<SubtreePlan> candidates;

  // We often query for the type of an operation, so we shorten these checks.
  using enum QueryExecutionTree::OperationType;

  // TODO<joka921> find out, what is ACTUALLY the use case for the triple
  // graph. Is it only meant for (questionable) performance reasons
  // or does it change the meaning.
  std::vector<std::array<ColumnIndex, 2>> jcs;
  if (tg) {
    if (connected(a, b, *tg)) {
      jcs = getJoinColumns(a, b);
    }
  } else {
    jcs = getJoinColumns(a, b);
  }

  if (jcs.empty()) {
    // The candidates are not connected
    return candidates;
  }
  // Find join variable(s) / columns.
  if (jcs.size() == 2 && (a._qet->getType() == TEXT_WITHOUT_FILTER ||
                          b._qet->getType() == TEXT_WITHOUT_FILTER)) {
    LOG(WARN) << "Not considering possible join on "
              << "two columns, if they involve text operations.\n";
    return candidates;
  }

  if (a.type == SubtreePlan::MINUS) {
    AD_THROW(
        "MINUS can only appear after"
        " another graph pattern.");
  }

  if (b.type == SubtreePlan::MINUS) {
    // This case shouldn't happen. If the first pattern is OPTIONAL, it
    // is made non optional earlier. If a minus occurs after an optional
    // further into the query that optional should be resolved by now.
    AD_CONTRACT_CHECK(a.type != SubtreePlan::OPTIONAL);
    return {makeSubtreePlan<Minus>(_qec, a._qet, b._qet)};
  }

  // OPTIONAL JOINS are not symmetric!
  AD_CONTRACT_CHECK(a.type != SubtreePlan::OPTIONAL);
  if (b.type == SubtreePlan::OPTIONAL) {
    // Join the two optional columns using an optional join
    return {makeSubtreePlan<OptionalJoin>(_qec, a._qet, b._qet)};
  }

  if (jcs.size() >= 2) {
    // If there are two or more join columns and we are not using the
    // TwoColumnJoin (the if part before this comment), use a multiColumnJoin.
    try {
      SubtreePlan plan = makeSubtreePlan<MultiColumnJoin>(_qec, a._qet, b._qet);
      mergeSubtreePlanIds(plan, a, b);
      return {plan};
    } catch (const std::exception& e) {
      return {};
    }
  }

  // CASE: JOIN ON ONE COLUMN ONLY.

  // Skip if we have two operations, where all three positions are variables.
  if (a._qet->getType() == SCAN && a._qet->getResultWidth() == 3 &&
      b._qet->getType() == SCAN && b._qet->getResultWidth() == 3) {
    return candidates;
  }

  // If one of the join results is a text operation without filter
  // also consider using the other one as filter and thus
  // turning this join into a text operation with filter, instead.
  if (auto opt = createJoinAsTextFilter(a, b, jcs)) {
    // It might still be cheaper to perform a "normal" join, so we are simply
    // adding this to the candidate plans and not returning.
    candidates.push_back(std::move(opt.value()));
  }
  // Check if one of the two operations is a HAS_PREDICATE_SCAN.
  // If the join column corresponds to the has-predicate scan's
  // subject column we can use a specialized join that avoids
  // loading the full has-predicate predicate.
  if (auto opt = createJoinWithHasPredicateScan(a, b, jcs)) {
    candidates.push_back(std::move(opt.value()));
  }

  // Test if one of `a` or `b` is a transitive path to which we can bind the
  // other one.
  if (auto opt = createJoinWithTransitivePath(a, b, jcs)) {
    candidates.push_back(std::move(opt.value()));
  }

  // "NORMAL" CASE:
  // The join class takes care of sorting the subtrees if necessary
  SubtreePlan plan =
      makeSubtreePlan<Join>(_qec, a._qet, b._qet, jcs[0][0], jcs[0][1]);
  mergeSubtreePlanIds(plan, a, b);
  candidates.push_back(std::move(plan));

  return candidates;
}

// __________________________________________________________________________________________________________________
auto QueryPlanner::createJoinWithTransitivePath(
    SubtreePlan a, SubtreePlan b,
    const std::vector<std::array<ColumnIndex, 2>>& jcs)
    -> std::optional<SubtreePlan> {
  using enum QueryExecutionTree::OperationType;
  const bool aIsTransPath = a._qet->getType() == TRANSITIVE_PATH;
  const bool bIsTransPath = b._qet->getType() == TRANSITIVE_PATH;

  if (!(aIsTransPath || bIsTransPath)) {
    return std::nullopt;
  }
  std::shared_ptr<QueryExecutionTree> otherTree =
      aIsTransPath ? b._qet : a._qet;
  auto& transPathTree = aIsTransPath ? a._qet : b._qet;
  auto transPathOperation = std::dynamic_pointer_cast<TransitivePath>(
      transPathTree->getRootOperation());

  // TODO: Handle the case of two or more common variables
  if (jcs.size() > 1) {
    AD_THROW(
        "Transitive Path operation with more than"
        " two common variables is not supported");
  }
  const size_t otherCol = aIsTransPath ? jcs[0][1] : jcs[0][0];
  const size_t thisCol = aIsTransPath ? jcs[0][0] : jcs[0][1];
  // Do not bind the side of a path twice
  if (transPathOperation->isBoundOrId()) {
    return std::nullopt;
  }
  // An unbound transitive path has at most two columns.
  AD_CONTRACT_CHECK(thisCol <= 1);
  // The left or right side is a TRANSITIVE_PATH and its join column
  // corresponds to the left side of its input.
  SubtreePlan plan = [&]() {
    if (thisCol == 0) {
      return makeSubtreePlan(
          transPathOperation->bindLeftSide(otherTree, otherCol));
    } else {
      return makeSubtreePlan(
          transPathOperation->bindRightSide(otherTree, otherCol));
    }
  }();
  mergeSubtreePlanIds(plan, a, b);
  return plan;
}

// ______________________________________________________________________________________
auto QueryPlanner::createJoinWithHasPredicateScan(
    SubtreePlan a, SubtreePlan b,
    const std::vector<std::array<ColumnIndex, 2>>& jcs)
    -> std::optional<SubtreePlan> {
  // Check if one of the two operations is a HAS_PREDICATE_SCAN.
  // If the join column corresponds to the has-predicate scan's
  // subject column we can use a specialized join that avoids
  // loading the full has-predicate predicate.
  using enum QueryExecutionTree::OperationType;
  auto isSuitablePredicateScan = [](const auto& tree, size_t joinColumn) {
    return tree._qet->getType() == HAS_PREDICATE_SCAN && joinColumn == 0 &&
           static_cast<HasPredicateScan*>(tree._qet->getRootOperation().get())
                   ->getType() == HasPredicateScan::ScanType::FULL_SCAN;
  };

  const bool aIsSuitablePredicateScan = isSuitablePredicateScan(a, jcs[0][0]);
  const bool bIsSuitablePredicateScan = isSuitablePredicateScan(b, jcs[0][1]);
  if (!(aIsSuitablePredicateScan || bIsSuitablePredicateScan)) {
    return std::nullopt;
  }
  auto hasPredicateScanTree = aIsSuitablePredicateScan ? a._qet : b._qet;
  auto otherTree = aIsSuitablePredicateScan ? b._qet : a._qet;
  size_t otherTreeJoinColumn = aIsSuitablePredicateScan ? jcs[0][1] : jcs[0][0];
  auto qec = otherTree->getRootOperation()->getExecutionContext();
  // Note that this is a new operation.
  auto object = static_cast<HasPredicateScan*>(
                    hasPredicateScanTree->getRootOperation().get())
                    ->getObject();
  auto plan = makeSubtreePlan<HasPredicateScan>(
      qec, std::move(otherTree), otherTreeJoinColumn, std::move(object));
  mergeSubtreePlanIds(plan, a, b);
  return plan;
}

// ______________________________________________________________________________________
auto QueryPlanner::createJoinAsTextFilter(
    SubtreePlan a, SubtreePlan b,
    const std::vector<std::array<ColumnIndex, 2>>& jcs)
    -> std::optional<SubtreePlan> {
  using enum QueryExecutionTree::OperationType;
  if (!(a._qet->getType() == TEXT_WITHOUT_FILTER ||
        b._qet->getType() == TEXT_WITHOUT_FILTER)) {
    return std::nullopt;
  }
  // If one of the join results is a text operation without filter
  // also consider using the other one as filter and thus
  // turning this join into a text operation with filter, instead,
  bool aTextOp = true;
  // If both are TextOps, the smaller one will be used as filter.
  if (a._qet->getType() != TEXT_WITHOUT_FILTER ||
      (b._qet->getType() == TEXT_WITHOUT_FILTER &&
       b._qet->getSizeEstimate() > a._qet->getSizeEstimate())) {
    aTextOp = false;
  }
  const auto& textPlanTree = aTextOp ? a._qet : b._qet;
  const auto& filterTree = aTextOp ? b._qet : a._qet;
  size_t otherPlanJc = aTextOp ? jcs[0][1] : jcs[0][0];
  const TextOperationWithoutFilter& noFilter =
      *static_cast<const TextOperationWithoutFilter*>(
          textPlanTree->getRootOperation().get());
  auto qec = textPlanTree->getRootOperation()->getExecutionContext();
  SubtreePlan plan = makeSubtreePlan<TextOperationWithFilter>(
      qec, noFilter.getWordPart(), noFilter.getVars(), noFilter.getCVar(),
      filterTree, otherPlanJc);
  mergeSubtreePlanIds(plan, a, b);
  return plan;
}

// _____________________________________________________________________
void QueryPlanner::QueryGraph::setupGraph(
    const std::vector<SubtreePlan>& leafOperations) {
  // Prepare the `nodes_` vector for the graph. We have one node for each leaf
  // of what later becomes the `QueryExecutionTree`.
  for (const auto& leafOperation : leafOperations) {
    nodes_.push_back(std::make_shared<Node>(&leafOperation));
  }

  // Set up a hash map from variables to nodes that contain this variable.
  const ad_utility::HashMap<Variable, std::vector<Node*>> varToNode = [this]() {
    ad_utility::HashMap<Variable, std::vector<Node*>> result;
    for (const auto& node : nodes_) {
      for (const auto& var :
           node->plan_->_qet->getVariableColumns() | std::views::keys) {
        result[var].push_back(node.get());
      }
    }
    return result;
  }();
  // Set up a hash map from nodes to their adjacentNodes_. Two nodes are
  // adjacent if they share a variable. The adjacentNodes_ are stored as hash
  // sets so we don't need to worry about duplicates.
  ad_utility::HashMap<Node*, ad_utility::HashSet<Node*>> adjacentNodes =
      [&varToNode]() {
        ad_utility::HashMap<Node*, ad_utility::HashSet<Node*>> result;
        for (auto& nodesThatContainSameVar : varToNode | std::views::values) {
          // TODO<C++23> Use std::views::cartesian_product
          for (auto* n1 : nodesThatContainSameVar) {
            for (auto* n2 : nodesThatContainSameVar) {
              if (n1 != n2) {
                result[n1].insert(n2);
                result[n2].insert(n1);
              }
            }
          }
        }
        return result;
      }();
  // For each node move the set of adjacentNodes_ from the global hash map to
  // the node itself.
  for (const auto& node : nodes_) {
    if (adjacentNodes.contains(node.get())) {
      node->adjacentNodes_ = std::move(adjacentNodes.at(node.get()));
    }
  }
}

// _______________________________________________________________
void QueryPlanner::QueryGraph::dfs(Node* startNode, size_t componentIndex) {
  // Simple recursive DFS.
  if (startNode->visited_) {
    return;
  }
  startNode->componentIndex_ = componentIndex;
  startNode->visited_ = true;
  for (auto* adjacentNode : startNode->adjacentNodes_) {
    dfs(adjacentNode, componentIndex);
  }
}
// _______________________________________________________________
std::vector<size_t> QueryPlanner::QueryGraph::dfsForAllNodes() {
  std::vector<size_t> result;
  size_t nextIndex = 0;
  for (const auto& node : nodes_) {
    if (node->visited_) {
      // The node is part of a connected component that was already found.
      result.push_back(node->componentIndex_);
    } else {
      // The node is part of a yet unknown component, run a DFS.
      dfs(node.get(), nextIndex);
      result.push_back(node->componentIndex_);
      ++nextIndex;
    }
  }
  return result;
}
