// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include <engine/Bind.h>
#include <engine/CountAvailablePredicates.h>
#include <engine/Distinct.h>
#include <engine/Filter.h>
#include <engine/GroupBy.h>
#include <engine/HasPredicateScan.h>
#include <engine/IndexScan.h>
#include <engine/Join.h>
#include <engine/Minus.h>
#include <engine/MultiColumnJoin.h>
#include <engine/NeutralElementOperation.h>
#include <engine/OptionalJoin.h>
#include <engine/OrderBy.h>
#include <engine/QueryPlanner.h>
#include <engine/Sort.h>
#include <engine/TextOperationWithFilter.h>
#include <engine/TextOperationWithoutFilter.h>
#include <engine/TransitivePath.h>
#include <engine/Union.h>
#include <engine/Values.h>
#include <parser/Alias.h>

#include <algorithm>
#include <ctime>

namespace p = parsedQuery;
namespace {
// All the operations take a `QueryExecutionContext` as a first argument.
// Todo: Continue the comment.
template <typename Operation>
std::shared_ptr<QueryExecutionTree> makeExecutionTree(
    QueryExecutionContext* qec, auto&&... args) {
  return std::make_shared<QueryExecutionTree>(
      qec, std::make_shared<Operation>(qec, AD_FWD(args)...));
}

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
  // If the pattern trick is used the ql:has-predicate triple will be removed
  // from the list of where clause triples. Otherwise the ql:has-relation triple
  // will be handled using a HasRelationScan.
  SparqlTriple patternTrickTriple("", PropertyPath(), "");
  bool usePatternTrick =
      _enablePatternTrick && checkUsePatternTrick(&pq, &patternTrickTriple);

  bool doGrouping = !pq._groupByVariables.empty() || usePatternTrick;
  if (!doGrouping && pq.hasSelectClause()) {
    // if there is no group by statement, but an aggregate alias is used
    // somewhere do grouping anyways.
    for (const Alias& alias : pq.selectClause().getAliases()) {
      if (alias._expression.isAggregate({})) {
        doGrouping = true;
        break;
      }
    }
  }

  // Optimize the graph pattern tree
  std::vector<std::vector<SubtreePlan>> plans;
  plans.push_back(optimize(&pq._rootGraphPattern));

  // Add the query level modifications

  // GROUP BY
  if (doGrouping && !usePatternTrick) {
    plans.emplace_back(getGroupByRow(pq, plans));
  } else if (usePatternTrick) {
    plans.emplace_back(
        getPatternTrickRow(pq.selectClause(), plans, patternTrickTriple));
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
      (plan._qet->getRootOperation()->setLimit(pq._limitOffset._limit));
    }
  }

  AD_CHECK_GT(lastRow.size(), 0);
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
  ad_utility::HashSet<std::string> boundVariables;

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
    LOG(TRACE) << "Collapse text cliques..." << std::endl;
    tg.collapseTextCliques();
    LOG(TRACE) << "Collapse text cliques done." << std::endl;
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
          boundVariables.insert(t._s.getString());
        }
        if (isVariable(t._p)) {
          boundVariables.insert(t._p._iri);
        }
        if (isVariable(t._o)) {
          boundVariables.insert(t._o.getString());
        }
      }
      candidateTriples._triples.insert(
          candidateTriples._triples.end(),
          std::make_move_iterator(v._triples.begin()),
          std::make_move_iterator(v._triples.end()));
    } else if constexpr (std::is_same_v<p::Bind, std::decay_t<decltype(v)>>) {
      if (boundVariables.count(v._target.name())) {
        AD_THROW(ad_semsearch::Exception::BAD_QUERY,
                 "The target variable of a BIND must not be used before the "
                 "BIND clause");
      }
      boundVariables.insert(v._target.name());

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
                          return !boundVariables.count(el.first);
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
          boundVariables.insert(el.first);
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
        joinCandidates(createExecutionTrees(arg.get()));
      } else if constexpr (std::is_same_v<T, p::TransPath>) {
        // TODO<kramerfl> This is obviously how you set up transitive paths.
        // maybe factor this out and comment it somewhere
        auto candidatesIn = optimize(&arg._childGraphPattern);
        std::vector<SubtreePlan> candidatesOut;

        for (auto& sub : candidatesIn) {
          size_t leftCol, rightCol;
          Id leftValue, rightValue;
          std::string leftColName, rightColName;
          size_t min, max;
          bool leftVar, rightVar;
          if (isVariable(arg._left)) {
            leftVar = true;
            leftCol = sub._qet->getVariableColumn(arg._innerLeft);
            leftColName = arg._left.getString();
          } else {
            leftVar = false;
            leftColName = generateUniqueVarName();
            leftCol = sub._qet->getVariableColumn(arg._innerLeft);
            if (auto opt = arg._left.toValueId(_qec->getIndex().getVocab());
                opt.has_value()) {
              leftValue = opt.value();
            } else {
              AD_THROW(ad_semsearch::Exception::BAD_QUERY,
                       "No vocabulary entry for " + arg._left.toString());
            }
          }
          // TODO<joka921> This is really much code duplication, get rid of it!
          if (isVariable(arg._right)) {
            rightVar = true;
            rightCol = sub._qet->getVariableColumn(arg._innerRight);
            rightColName = arg._right.getString();
          } else {
            rightVar = false;
            rightCol = sub._qet->getVariableColumn(arg._innerRight);
            rightColName = generateUniqueVarName();
            if (auto opt = arg._right.toValueId(_qec->getIndex().getVocab());
                opt.has_value()) {
              rightValue = opt.value();
            } else {
              AD_THROW(ad_semsearch::Exception::BAD_QUERY,
                       "No vocabulary entry for " + arg._right.toString());
            }
          }
          min = arg._min;
          max = arg._max;
          auto plan = makeSubtreePlan<TransitivePath>(
              _qec, sub._qet, leftVar, rightVar, leftCol, rightCol, leftValue,
              rightValue, leftColName, rightColName, min, max);
          candidatesOut.push_back(std::move(plan));
        }
        joinCandidates(std::move(candidatesOut));

      } else if constexpr (std::is_same_v<T, p::Values>) {
        SubtreePlan valuesPlan =
            makeSubtreePlan<Values>(_qec, arg._inlineValues);
        joinCandidates(std::vector{std::move(valuesPlan)});

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
    LOG(TRACE) << "Collapse text cliques..." << std::endl;
    tg.collapseTextCliques();
    LOG(TRACE) << "Collapse text cliques done." << std::endl;
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

  AD_CHECK(candidatePlans.size() == 1 || candidatePlans.empty());
  if (candidatePlans.empty()) {
    // this case is needed e.g. if we have the empty graph pattern due to a
    // pattern trick
    return std::vector<SubtreePlan>{};
  } else {
    return candidatePlans[0];
  }
}

bool QueryPlanner::checkUsePatternTrick(
    ParsedQuery* pq, SparqlTriple* patternTrickTriple) const {
  //   TODO<joka921> does the pattern trick behave correctly if the elements
  //   appear in a value clause?
  // Check if the query has the right number of variables for aliases and
  // group by.
  if (!pq->hasSelectClause()) {
    return false;
  }
  const auto& selectClause = pq->selectClause();
  auto aliases = selectClause.getAliases();
  if (pq->_groupByVariables.size() != 1 || aliases.size() > 1) {
    return false;
  }

  bool returns_counts = aliases.size() == 1;

  // These will only be set if the query returns the count of predicates
  // The variable the COUNT alias counts.
  std::string counted_var_name;
  // The variable holding the counts
  std::string count_var_name;

  if (returns_counts) {
    // We have already verified above that there is exactly one alias.
    // TODO<joka921> this should be `DISTINCT` not `nonDistinct`.
    const Alias& alias = aliases.front();
    auto countVariable =
        alias._expression.getVariableForNonDistinctCountOrNullopt();
    if (!countVariable.has_value()) {
      return false;
    }
    counted_var_name = countVariable.value().name();
    count_var_name = alias._target.name();
  }

  // The first possibility for using the pattern trick is having a
  // ql:has-predicate predicate in the query

  // look for a HAS_RELATION_PREDICATE triple which satisfies all constraints
  // check in all the basic graph patterns that are direct children.
  // TODO<joka921, kramerfl> verify and proof that this is always legal
  for (auto& child : pq->children()) {
    auto* curPattern = std::get_if<p::BasicGraphPattern>(&child);
    if (!curPattern) {
      continue;
    }
    for (size_t i = 0; i < curPattern->_triples.size(); i++) {
      bool usePatternTrick = true;
      const SparqlTriple& t = curPattern->_triples[i];
      // Check that the triples predicates is the HAS_PREDICATE_PREDICATE.
      // Also check that the triples object or subject matches the aliases input
      // variable and the group by variable.
      if (t._p._iri != HAS_PREDICATE_PREDICATE || !isVariable(t._o) ||
          (returns_counts &&
           !(counted_var_name == t._o || counted_var_name == t._s)) ||
          pq->_groupByVariables[0].name() != t._o) {
        usePatternTrick = false;
        continue;
      }

      // Check that all selected variables are outputs of
      // CountAvailablePredicates
      if (selectClause.isAsterisk()) {
        return false;
      }

      const auto& selectedVariables =
          selectClause.getSelectedVariablesAsStrings();
      for (const std::string& s : selectedVariables) {
        if (s != t._o && s != count_var_name) {
          usePatternTrick = false;
          break;
        }
      }
      if (!usePatternTrick) {
        continue;
      }

      // Check for triples containing the ql:has-predicate triple's
      // object.
      for (auto& otherChild : pq->children()) {
        auto* otherPattern = std::get_if<p::BasicGraphPattern>(&otherChild);
        if (!otherPattern) {
          continue;
        }
        for (size_t j = 0; usePatternTrick && j < otherPattern->_triples.size();
             j++) {
          const SparqlTriple& other = otherPattern->_triples[j];
          if ((&child != &otherChild || j != i) &&
              (other._s == t._o || other._p._iri == t._o || other._o == t._o)) {
            usePatternTrick = false;
          }
        }
      }
      if (!usePatternTrick) {
        continue;
      }

      // Check for filters on the ql:has-predicate triple's subject or
      // object.
      // Filters that filter on the triple's object but have a static
      // rhs will be transformed to a having clause later on.
      for (const SparqlFilter& filter : pq->_rootGraphPattern._filters) {
        if (!(filter._lhs == t._o && filter._rhs[0] != '?') &&
            (filter._lhs == t._s || filter._lhs == t._o ||
             filter._rhs == t._o || filter._rhs == t._s)) {
          usePatternTrick = false;
          break;
        }
      }

      if (!usePatternTrick) {
        continue;
      }

      // Check for sub graph patterns containing the ql:has-predicate
      // triple's object
      std::vector<const ParsedQuery::GraphPattern*> graphsToProcess;
      for (const auto& op : pq->_rootGraphPattern._graphPatterns) {
        op.visit([&](auto&& arg) {
          using T = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<T, p::Optional> ||
                        std::is_same_v<T, p::GroupGraphPattern>) {
            graphsToProcess.push_back(&arg._child);
          } else if constexpr (std::is_same_v<T, p::Union>) {
            graphsToProcess.push_back(&arg._child1);
            graphsToProcess.push_back(&arg._child2);
          } else if constexpr (std::is_same_v<T, p::Subquery>) {
            if (!arg.get().hasSelectClause()) {
              usePatternTrick = false;
              return;
            }
            const auto& selectClause = arg.get().selectClause();
            for (const auto& v : selectClause.getSelectedVariablesAsStrings()) {
              if (v == t._o) {
                usePatternTrick = false;
                break;
              }
            }
          } else if constexpr (std::is_same_v<T, p::Bind>) {
            // If the object variable of ql:has-predicate is used somewhere in a
            // BIND, we cannot use the pattern trick.
            for (const std::string* v : arg.strings()) {
              if (*v == t._o) {
                usePatternTrick = false;
                break;
              }
            }

          } else {
            static_assert(std::is_same_v<T, p::TransPath> ||
                          std::is_same_v<T, p::BasicGraphPattern> ||
                          std::is_same_v<T, p::Values> ||
                          std::is_same_v<T, p::Minus>);
          }
          // Transitive paths cannot yet exist in the query. They could also
          // not contain the variables we are interested in.
          // and the
        });
      }
      while (!graphsToProcess.empty() && usePatternTrick) {
        const ParsedQuery::GraphPattern* pattern = graphsToProcess.back();
        graphsToProcess.pop_back();

        for (const auto& op : pattern->_graphPatterns) {
          op.visit([&](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, p::Optional> ||
                          std::is_same_v<T, p::GroupGraphPattern> ||
                          std::is_same_v<T, p::Minus>) {
              graphsToProcess.push_back(&arg._child);

            } else if constexpr (std::is_same_v<T, p::Union>) {
              graphsToProcess.push_back(&arg._child1);
              graphsToProcess.push_back(&arg._child2);
            } else if constexpr (std::is_same_v<T, p::Subquery>) {
              if (!arg.get().hasSelectClause()) {
                usePatternTrick = false;
                return;
              }
              const auto& selectClause = arg.get().selectClause();
              for (const auto& v :
                   selectClause.getSelectedVariablesAsStrings()) {
                if (v == t._o) {
                  usePatternTrick = false;
                  break;
                }
              }
            } else if constexpr (std::is_same_v<T, p::BasicGraphPattern>) {
              for (const SparqlTriple& other : arg._triples) {
                if (other._s == t._o || other._p._iri == t._o ||
                    other._o == t._o) {
                  usePatternTrick = false;
                  break;
                }
              }
            } else if constexpr (std::is_same_v<T, p::Values>) {
              for (const auto& var : arg._inlineValues._variables) {
                if (var == t._o) {
                  usePatternTrick = false;
                  break;
                }
              }
            } else if constexpr (std::is_same_v<T, p::Bind>) {
              // If the object variable of ql:has-predicate is used somewhere in
              // a BIND, we cannot use the pattern trick.
              for (const std::string* v : arg.strings()) {
                if (*v == t._o) {
                  usePatternTrick = false;
                  break;
                }
              }
            } else {
              static_assert(std::is_same_v<T, p::TransPath>);
            }
            // Transitive paths cannot yet exist in the query. They could also
            // not contain the variables we are interested in.
          });
        }

        if (!usePatternTrick) {
          break;
        }
      }
      if (!usePatternTrick) {
        continue;
      }

      LOG(DEBUG) << "Using the pattern trick to answer the query." << endl;
      // Transform filters on the ql:has-relation triple's object that
      // have a static rhs to having clauses
      // Filters are only scoped within a GraphPattern, so we only
      // have to  check curPattern
      auto& filters = pq->_rootGraphPattern._filters;
      auto it = std::remove_if(
          filters.begin(), filters.end(), [&t](const SparqlFilter& filter) {
            return filter._lhs == t._o && filter._rhs[0] != '?';
          });
      std::for_each(it, filters.end(), [&pq](const SparqlFilter& f) {
        pq->_havingClauses.push_back(f);
      });
      filters.erase(it, filters.end());

      *patternTrickTriple = t;
      // Remove the triple from the graph. Note that this invalidates the
      // reference `t`, so we perform this step at the very end.
      curPattern->_triples.erase(curPattern->_triples.begin() + i);
      return true;
    }
  }

  return false;
  // TODO<joka921>, <kramerfl> this second possibility is
  // disabled for now, since we first have to discuss, whether
  // the checks below suffice. IMHO (johannes) We have to distinguish,
  // whether the special triple is "constrained" by an optional
  // or by something else.
  /*

  // The second possibility for using the pattern trick is a subquery.

  LOG(TRACE) << "Considering a subquery as a patterntrick candidate"
             << std::endl;

  // Check if the queries single child is a subquery that contains a triple
  // of the form ?s ?p ?o with constraints solely on ?s and a select on
  // distinct ?s and ?p
  std::string predVar = pq->_selectClause._selectedVariables[0];
  std::string subjVar = counted_var_name;
  LOG(TRACE) << "The subject is " << subjVar << " the predicate " << predVar
             << std::endl;
  std::string objVar;
  auto& root = pq->_rootGraphPattern;

  // Check that pq does not have where clause triples or filters, but
  // contains a single subquery child
  if (root._graphPatterns.size() != 1 ||
      !std::holds_alternative<p::Subquery>(
          root._graphPatterns[0].variant_)) {
    return false;
  }

  LOG(TRACE) << "Query has a single subquery of the right type" << std::endl;

  // Check that the query is distinct and does not do any grouping and returns 2
  // variables.
  // Here we need to take a copy, since we modify it and merge it back
  // into the root
  auto sub =
  root._graphPatterns[0].get<p::Subquery>().get(); if
  (!sub.distinct_ || sub._groupByVariables.size() > 0 ||
      sub._selectClause.aliases_.size() > 0 ||
  sub._selectClause._selectedVariables.size() != 2) { return false;
  }
  // Also check that it returns the correct variables
  for (const std::string& v : sub._selectClause._selectedVariables) {
    if (v != predVar && v != subjVar) {
      return false;
    }
  }

  LOG(TRACE) << "The subquery has the correct variables" << std::endl;

  // Look for a triple in the subquery of the form 'predVar subjVar ?o'
  auto& subroot = sub._rootGraphPattern;
  for (size_t i = 0; i < subroot._triples.size(); i++) {
    const SparqlTriple& t = subroot._triples[i];
    if ((returns_counts && t._s != subjVar) || t._p._iri != predVar ||
        !isVariable(t._o)) {
      continue;
    }

    LOG(TRACE) << "Found a triple matching the subject and predicate "
                  "with object "
               << t._o << std::endl;
    if (!returns_counts) {
      subjVar = t._s;
    }
    // The triple at i has the correct subject and predicate and a
    // variable as an object
    objVar = t._o;

    // Check if either the predicate or the object are constrained in
    // any way
    bool is_constrained = false;
    for (size_t j = 0; j < subroot._triples.size(); j++) {
      if (j != i) {
        const SparqlTriple& t2 = subroot._triples[j];
        if (t2._s == predVar || t2._p._iri == predVar || t2._o == predVar ||
            t2._s == objVar || t2._p._iri == objVar || t2._o == objVar) {
          LOG(TRACE) << "There is another triple " << t2.asString()
                     << " which constraints " << predVar << " or " << objVar
                     << std::endl;
          is_constrained = true;
          break;
        }
      }
    }
    if (is_constrained) {
      continue;
    }

    // Ensure the triple is not being filtered on either
    for (size_t j = 0; j < subroot._filters.size(); j++) {
      const SparqlFilter& f = subroot._filters[j];
      if (f._lhs == subjVar || f._lhs == predVar || f._lhs == objVar ||
          f._rhs == subjVar || f._rhs == predVar || f._rhs == objVar) {
        LOG(TRACE) << "There is a filter on one of the three variables"
                   << std::endl;
        is_constrained = true;
        break;
      }
    }
    if (is_constrained) {
      continue;
    }

    LOG(TRACE) << "Removing the triple and merging the subquery "
                  "with its parent."
               << std::endl;
    LOG(DEBUG) << "Using the pattern trick to answer the query." << endl;
    // If this used ql:has-predicate predVar would be the object
    patternTrickTriple->_s = subjVar;
    patternTrickTriple->_o = predVar;
    // merge the subquery without the selected triple into the
    // parent.
    subroot._triples.erase(subroot._triples.begin() + i);
    root._graphPatterns.clear();
    pq->merge(sub);
    break;
  }
  return true;
   */
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
    vector<size_t> keepIndices;
    ad_utility::HashSet<size_t> indDone;
    const auto& colMap = parent._qet->getVariableColumns();
    for (const auto& var : selectClause.getSelectedVariablesAsStrings()) {
      // There used to be a special treatment for `?ql_textscore_` variables
      // which was considered a bug.
      if (auto it = colMap.find(var); it != colMap.end()) {
        auto ind = it->second;
        if (indDone.count(ind) == 0) {
          keepIndices.push_back(ind);
          indDone.insert(ind);
        }
      }
    }
    const std::vector<size_t>& resultSortedOn =
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
      if (keepIndices.size() == 1) {
        auto tree = makeExecutionTree<Sort>(_qec, parent._qet, keepIndices[0]);
        distinctPlan._qet =
            makeExecutionTree<Distinct>(_qec, tree, keepIndices);
      } else {
        vector<pair<size_t, bool>> obCols;
        for (auto& i : keepIndices) {
          obCols.emplace_back(std::make_pair(i, false));
        }
        auto tree = makeExecutionTree<OrderBy>(_qec, parent._qet, obCols);
        distinctPlan._qet =
            makeExecutionTree<Distinct>(_qec, tree, keepIndices);
      }
    }
    added.push_back(distinctPlan);
  }
  return added;
}

// _____________________________________________________________________________
vector<QueryPlanner::SubtreePlan> QueryPlanner::getPatternTrickRow(
    const p::SelectClause& selectClause,
    const vector<vector<SubtreePlan>>& dpTab,
    const SparqlTriple& patternTrickTriple) {
  const vector<SubtreePlan>* previous = nullptr;
  auto aliases = selectClause.getAliases();
  if (!dpTab.empty()) {
    previous = &dpTab.back();
  }
  vector<SubtreePlan> added;

  std::string predicateVariable = patternTrickTriple._o.getString();
  std::string countVariable =
      aliases.empty() ? generateUniqueVarName() : aliases[0]._target.name();
  if (previous != nullptr && !previous->empty()) {
    added.reserve(previous->size());
    for (const auto& parent : *previous) {
      // Determine the column containing the subjects for which we are
      // interested in their predicates.
      auto subjectColumn =
          parent._qet->getVariableColumn(patternTrickTriple._s.getString());
      auto patternTrickPlan = makeSubtreePlan<CountAvailablePredicates>(
          _qec, parent._qet, subjectColumn, predicateVariable, countVariable);
      added.push_back(std::move(patternTrickPlan));
    }
  } else if (!patternTrickTriple._s.isVariable()) {
    // The subject of the pattern trick is not a variable
    SubtreePlan patternTrickPlan = makeSubtreePlan<CountAvailablePredicates>(
        _qec, patternTrickTriple._s, predicateVariable, countVariable);
    added.push_back(std::move(patternTrickPlan));
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
      SubtreePlan plan(_qec);
      auto& tree = *plan._qet;
      tree.setOperation(QueryExecutionTree::FILTER,
                        createFilterOperation(filter, parent));
      filtered = plan;
    }
    added.push_back(filtered);
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
    if (pq._orderBy.size() == 1 && !pq._orderBy[0].isDescending_) {
      size_t col =
          parent._qet->getVariableColumn(pq._orderBy[0].variable_.name());
      const std::vector<size_t>& previousSortedOn =
          parent._qet->resultSortedOn();
      if (!previousSortedOn.empty() && col == previousSortedOn[0]) {
        // Already sorted perfectly
        added.push_back(parent);
      } else {
        tree = makeExecutionTree<Sort>(_qec, parent._qet, col);
        added.push_back(plan);
      }
    } else {
      vector<pair<size_t, bool>> sortIndices;
      for (auto& ord : pq._orderBy) {
        sortIndices.emplace_back(
            parent._qet->getVariableColumn(ord.variable_.name()),
            ord.isDescending_);
      }
      const std::vector<size_t>& previousSortedOn =
          parent._qet->resultSortedOn();
      bool alreadySorted = previousSortedOn.size() >= sortIndices.size();
      for (size_t j = 0; alreadySorted && j < sortIndices.size(); j++) {
        alreadySorted = alreadySorted && !sortIndices[j].second &&
                        sortIndices[j].first == previousSortedOn[j];
      }
      if (alreadySorted) {
        // Already sorted perfectly
        added.push_back(parent);
      } else {
        tree = makeExecutionTree<OrderBy>(_qec, parent._qet, sortIndices);
        added.push_back(plan);
      }
    }
  }
  return added;
}

// _____________________________________________________________________________
/*
void QueryPlanner::getVarTripleMap(
    const ParsedQuery& pq,
    ad_utility::HashMap<string, vector<SparqlTriple>>* varToTrip,
    ad_utility::HashSet<string>* contextVars) const {
  for (const SparqlTriple& t : pq._rootGraphPattern._triples) {
    if (isVariable(t._s)) {
      (*varToTrip)[t._s].push_back(t);
    }
    if (isVariable(t._p)) {
      (*varToTrip)[t._p._iri].push_back(t);
    }
    if (isVariable(t._o)) {
      (*varToTrip)[t._o].push_back(t);
    }
    // TODO: Could use more refactoring.
    // In Earlier versions there were no ql:contains... predicates but
    // a symmetric <in-text> predicate. Therefore some parts are still more
    // complex than need be.
    if (t._p._iri == CONTAINS_WORD_PREDICATE ||
        t._p._iri == CONTAINS_ENTITY_PREDICATE) {
      contextVars->insert(t._s);
    }
  }
}
 */

// _____________________________________________________________________________
QueryPlanner::TripleGraph QueryPlanner::createTripleGraph(
    const p::BasicGraphPattern* pattern) const {
  TripleGraph tg;
  if (pattern->_triples.size() > 64) {
    AD_THROW(ad_semsearch::Exception::BAD_QUERY,
             "At most 64 triples allowed at the moment.");
  }
  for (auto& t : pattern->_triples) {
    // Add a node for the triple.
    tg._nodeStorage.emplace_back(TripleGraph::Node(tg._nodeStorage.size(), t));
    auto& addedNode = tg._nodeStorage.back();
    tg._nodeMap[addedNode._id] = &tg._nodeStorage.back();
    tg._adjLists.emplace_back(vector<size_t>());
    assert(tg._adjLists.size() == tg._nodeStorage.size());
    assert(tg._adjLists.size() == addedNode._id + 1);
    // Now add an edge between the added node and every node sharing a var.
    for (auto& addedNodevar : addedNode._variables) {
      for (size_t i = 0; i < addedNode._id; ++i) {
        auto& otherNode = *tg._nodeMap[i];
        if (otherNode._variables.count(addedNodevar) > 0) {
          // There is an edge between *it->second and the node with id "id".
          tg._adjLists[addedNode._id].push_back(otherNode._id);
          tg._adjLists[otherNode._id].push_back(addedNode._id);
        }
      }
    }
  }
  return tg;
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

    auto pushPlan = [&](SubtreePlan plan) {
      plan._idsOfIncludedNodes = (uint64_t(1) << i);
      seeds.push_back(std::move(plan));
    };

    auto addIndexScan = [&](IndexScan::ScanType type) {
      pushPlan(makeSubtreePlan<IndexScan>(_qec, type, node._triple));
    };

    using enum IndexScan::ScanType;

    if (!node._cvar.empty()) {
      seeds.push_back(getTextLeafPlan(node));
      continue;
    }
    if (node._variables.empty()) {
      AD_THROW(ad_semsearch::Exception::BAD_QUERY,
               "Triples should have at least one variable. Not the case in: " +
                   node._triple.asString());
    }

    // If the predicate is a property path, we have to recursively set up the
    // index scans.
    if (node._triple._p._operation != PropertyPath::Operation::IRI) {
      for (SubtreePlan& plan : seedFromPropertyPathTriple(node._triple)) {
        pushPlan(std::move(plan));
      }
      continue;
    }

    // At this point, we know that the predicate is a simple IRI or a variable.

    if (_qec && !_qec->getIndex().hasAllPermutations() &&
        isVariable(node._triple._p._iri)) {
      AD_THROW(ad_semsearch::Exception::BAD_QUERY,
               "The query contains a predicate variable, but only the PSO "
               "and POS permutations were loaded. Rerun the server without "
               "the option --only-pso-and-pos-permutations and if "
               "necessary also rebuild the index.");
    }

    if (node._triple._p._iri == HAS_PREDICATE_PREDICATE) {
      pushPlan(makeSubtreePlan<HasPredicateScan>(_qec, node._triple));
      continue;
    }

    if (node._variables.size() == 1) {
      // There is exactly one variable in the triple (may occur twice).
      if (isVariable(node._triple._s) && isVariable(node._triple._o) &&
          node._triple._s == node._triple._o) {
        if (isVariable(node._triple._p._iri)) {
          AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
                   "Triple with one variable repeated three times");
        }
        LOG(DEBUG) << "Subject variable same as object variable" << std::endl;
        // Need to handle this as IndexScan with a new unique
        // variable + Filter. Works in both directions
        std::string filterVar = generateUniqueVarName();
        auto scanTriple = node._triple;
        scanTriple._o = filterVar;
        auto scanTree =
            makeExecutionTree<IndexScan>(_qec, PSO_FREE_S, scanTriple);
        auto plan = makeSubtreePlan<Filter>(
            _qec, scanTree, SparqlFilter::FilterType::EQ,
            node._triple._s.getString(), filterVar, vector<string>{},
            vector<string>{});
        pushPlan(std::move(plan));
      } else if (isVariable(node._triple._s)) {
        addIndexScan(POS_BOUND_O);
      } else if (isVariable(node._triple._o)) {
        addIndexScan(PSO_BOUND_S);
      } else {
        AD_CHECK(isVariable(node._triple._p));
        addIndexScan(SOP_BOUND_O);
      }
    } else if (node._variables.size() == 2) {
      // Add plans for both possible scan directions.
      if (!isVariable(node._triple._p._iri)) {
        addIndexScan(PSO_FREE_S);
        addIndexScan(POS_FREE_O);
      } else if (!isVariable(node._triple._s)) {
        addIndexScan(SPO_FREE_P);
        addIndexScan(SOP_FREE_O);
      } else if (!isVariable(node._triple._o)) {
        addIndexScan(OSP_FREE_S);
        addIndexScan(OPS_FREE_P);
      }
    } else {
      // The current triple contains three distinct variables.
      if (!_qec || _qec->getIndex().hasAllPermutations()) {
        // Add plans for all six permutations.
        addIndexScan(FULL_INDEX_SCAN_OPS);
        addIndexScan(FULL_INDEX_SCAN_OSP);
        addIndexScan(FULL_INDEX_SCAN_PSO);
        addIndexScan(FULL_INDEX_SCAN_POS);
        addIndexScan(FULL_INDEX_SCAN_SPO);
        addIndexScan(FULL_INDEX_SCAN_SOP);
      } else {
        AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
                 "With only 2 permutations registered (no -a option), "
                 "triples should have at most two variables. "
                 "Not the case in: " +
                     node._triple.asString());
      }
    }
  }
  return seeds;
}

// _____________________________________________________________________________
vector<QueryPlanner::SubtreePlan> QueryPlanner::seedFromPropertyPathTriple(
    const SparqlTriple& triple) {
  if (triple._p._can_be_null) {
    std::stringstream buf;
    buf << "The property path ";
    triple._p.writeToStream(buf);
    buf << " can evaluate to the empty path which is not yet supported.";
    AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
             std::move(buf).str());
  }
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
    case PropertyPath::Operation::TRANSITIVE:
      return seedFromTransitive(left, path, right);
    case PropertyPath::Operation::TRANSITIVE_MAX:
      return seedFromTransitiveMax(left, path, right);
    case PropertyPath::Operation::TRANSITIVE_MIN:
      return seedFromTransitiveMin(left, path, right);
  }
  AD_THROW(
      ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
      "No implementation for creating a seed from a property path of type " +
          std::to_string(static_cast<int>(path._operation)));
}

// _____________________________________________________________________________
std::shared_ptr<ParsedQuery::GraphPattern> QueryPlanner::seedFromSequence(
    const TripleComponent& left, const PropertyPath& path,
    const TripleComponent& right) {
  if (path._children.empty()) {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
             "Tried processing a sequence property path node without any "
             "children.");
  } else if (path._children.size() == 1) {
    LOG(WARN) << "Processing a sequence property path that has only one child."
              << std::endl;
    return seedFromPropertyPath(left, path, right);
  }

  // Split the child paths into groups that can not be null (have at least one
  // node that can not be null). If all children can be null create a single
  // group.
  std::vector<std::array<size_t, 2>> nonNullChunks;
  size_t start = 0;
  while (start < path._children.size()) {
    bool has_non_null = false;
    bool has_null = false;
    size_t end = start;
    for (end = start; end < path._children.size(); end++) {
      if (path._children[end]._can_be_null) {
        has_null = true;
      } else {
        if (has_null && has_non_null) {
          // We already have a non null node and we have the maximum
          // number of null nodes while using the least possible number of
          // non null nodes (for a greedy approach)
          --end;
          break;
        }
        has_non_null = true;
      }
    }
    bool wasAtEnd = false;
    if (end == path._children.size()) {
      --end;
      wasAtEnd = true;
    }
    if (wasAtEnd && nonNullChunks.size() > 0 && !has_non_null) {
      // Avoid creating an empty chunk by expanding the previous one
      nonNullChunks.back()[1] = end + 1;
    } else {
      nonNullChunks.push_back({start, end + 1});
    }
    start = end + 1;
  }

  // Generate unique variable names that connect the nunNullChunks
  std::vector<std::string> connectionVarNames(nonNullChunks.size() + 1);
  // TODO<joka921, kramerfl> Does it always hold that `left` and `right` are
  // variables at this point?
  connectionVarNames[0] = left.toString();
  connectionVarNames.back() = right.toString();
  for (size_t i = 1; i + 1 < connectionVarNames.size(); i++) {
    connectionVarNames[i] = generateUniqueVarName();
  }

  // Stores the pattern for every non null chunk
  std::vector<ParsedQuery::GraphPattern> chunkPatterns;
  chunkPatterns.reserve(nonNullChunks.size());

  for (size_t chunkIdx = 0; chunkIdx < nonNullChunks.size(); ++chunkIdx) {
    std::array<size_t, 2> chunk = nonNullChunks[chunkIdx];
    std::string chunkLeft = connectionVarNames[chunkIdx];
    std::string chunkRight = connectionVarNames[chunkIdx + 1];
    size_t numChildren = chunk[1] - chunk[0];

    // This vector maps the indices of child paths that can be null
    // to a bit in a bitmask. This information is later used
    // on to create a union over every possible combination of including and
    // excluding the child paths that can be null.
    std::vector<size_t> null_child_indices(numChildren, -1);
    size_t num_null_children = 0;

    for (size_t i = chunk[0]; i < chunk[1]; i++) {
      if (path._children[i]._can_be_null) {
        null_child_indices[i] = num_null_children;
        ++num_null_children;
      }
    }
    if (num_null_children > 10) {
      AD_THROW(
          ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
          "More than 10 consecutive children of a sequence path that can be "
          "null are "
          "not yet supported.");
    }

    // We need a union over every combination of joins that exclude any subset
    // the child paths which are marked as can_be_null.
    std::vector<ParsedQuery::GraphPattern> join_patterns;
    join_patterns.reserve((1 << num_null_children));

    std::vector<size_t> included_ids(numChildren);
    for (uint64_t bitmask = 0; bitmask < (size_t(1) << num_null_children);
         ++bitmask) {
      included_ids.clear();
      for (size_t i = chunk[0]; i < chunk[1]; i++) {
        if (!path._children[i]._can_be_null ||
            (bitmask & (1 << null_child_indices[i])) > 0) {
          included_ids.push_back(i);
        }
      }
      // Avoid creating an empty graph pattern
      if (included_ids.empty()) {
        continue;
      }
      TripleComponent l = left;
      TripleComponent r;
      if (included_ids.size() > 1) {
        r = generateUniqueVarName();
      } else {
        r = right;
      }

      // Merge all the child plans into one graph pattern
      // excluding those that can be null and are not marked in the bitmask
      auto p = ParsedQuery::GraphPattern{};
      for (size_t i = 0; i < included_ids.size(); i++) {
        std::shared_ptr<ParsedQuery::GraphPattern> child =
            seedFromPropertyPath(l, path._children[included_ids[i]], r);
        p._graphPatterns.insert(p._graphPatterns.end(),
                                child->_graphPatterns.begin(),
                                child->_graphPatterns.end());
        // Update the variables used on the left and right of the child path
        l = r;
        if (i + 2 < included_ids.size()) {
          r = generateUniqueVarName();
        } else {
          r = right;
        }
      }
      join_patterns.push_back(p);
    }

    if (join_patterns.size() == 1) {
      chunkPatterns.push_back(std::move(join_patterns[0]));
    } else {
      chunkPatterns.push_back(uniteGraphPatterns(std::move(join_patterns)));
    }
  }

  if (chunkPatterns.size() == 1) {
    // todo<joka921> also remove these shared_ptrs
    return std::make_shared<ParsedQuery::GraphPattern>(chunkPatterns[0]);
  }

  // Join the chunk patterns
  std::shared_ptr<ParsedQuery::GraphPattern> fp =
      std::make_shared<ParsedQuery::GraphPattern>();

  for (ParsedQuery::GraphPattern& p : chunkPatterns) {
    fp->_graphPatterns.insert(fp->_graphPatterns.begin(),
                              std::make_move_iterator(p._graphPatterns.begin()),
                              std::make_move_iterator(p._graphPatterns.end()));
  }

  return fp;
}

// _____________________________________________________________________________
std::shared_ptr<ParsedQuery::GraphPattern> QueryPlanner::seedFromAlternative(
    const TripleComponent& left, const PropertyPath& path,
    const TripleComponent& right) {
  if (path._children.empty()) {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
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
    const TripleComponent& right) {
  std::string innerLeft = generateUniqueVarName();
  std::string innerRight = generateUniqueVarName();
  std::shared_ptr<ParsedQuery::GraphPattern> childPlan =
      seedFromPropertyPath(innerLeft, path._children[0], innerRight);
  std::shared_ptr<ParsedQuery::GraphPattern> p =
      std::make_shared<ParsedQuery::GraphPattern>();
  p::TransPath transPath;
  transPath._left = left;
  transPath._right = right;
  transPath._innerLeft = innerLeft;
  transPath._innerRight = innerRight;
  transPath._min = 1;
  transPath._max = std::numeric_limits<size_t>::max();
  transPath._childGraphPattern = *childPlan;
  p->_graphPatterns.emplace_back(std::move(transPath));
  return p;
}

// _____________________________________________________________________________
std::shared_ptr<ParsedQuery::GraphPattern> QueryPlanner::seedFromTransitiveMin(
    const TripleComponent& left, const PropertyPath& path,
    const TripleComponent& right) {
  std::string innerLeft = generateUniqueVarName();
  std::string innerRight = generateUniqueVarName();
  std::shared_ptr<ParsedQuery::GraphPattern> childPlan =
      seedFromPropertyPath(innerLeft, path._children[0], innerRight);
  std::shared_ptr<ParsedQuery::GraphPattern> p =
      std::make_shared<ParsedQuery::GraphPattern>();
  p::TransPath transPath;
  transPath._left = left;
  transPath._right = right;
  transPath._innerLeft = innerLeft;
  transPath._innerRight = innerRight;
  transPath._min = std::max(uint_fast16_t(1), path._limit);
  transPath._max = std::numeric_limits<size_t>::max();
  transPath._childGraphPattern = *childPlan;
  p->_graphPatterns.emplace_back(std::move(transPath));
  return p;
}

// _____________________________________________________________________________
std::shared_ptr<ParsedQuery::GraphPattern> QueryPlanner::seedFromTransitiveMax(
    const TripleComponent& left, const PropertyPath& path,
    const TripleComponent& right) {
  std::string innerLeft = generateUniqueVarName();
  std::string innerRight = generateUniqueVarName();
  std::shared_ptr<ParsedQuery::GraphPattern> childPlan =
      seedFromPropertyPath(innerLeft, path._children[0], innerRight);
  std::shared_ptr<ParsedQuery::GraphPattern> p =
      std::make_shared<ParsedQuery::GraphPattern>();
  p::TransPath transPath;
  transPath._left = left;
  transPath._right = right;
  transPath._innerLeft = innerLeft;
  transPath._innerRight = innerRight;
  transPath._min = 1;
  transPath._max = path._limit;
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
std::string QueryPlanner::generateUniqueVarName() {
  return "?:" + std::to_string(_internalVarCount++);
}

// _____________________________________________________________________________
QueryPlanner::SubtreePlan QueryPlanner::getTextLeafPlan(
    const QueryPlanner::TripleGraph::Node& node) const {
  SubtreePlan plan(_qec);
  plan._idsOfIncludedNodes |= (size_t(1) << node._id);
  auto& tree = *plan._qet;
  AD_CHECK(node._wordPart.size() > 0);
  auto textOp = std::make_shared<TextOperationWithoutFilter>(
      _qec, node._wordPart, node._variables, node._cvar);
  tree.setOperation(QueryExecutionTree::OperationType::TEXT_WITHOUT_FILTER,
                    textOp);
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
      LOG(TRACE) << "Creating join candidates for " << ai._qet->asString()
                 << "\n and " << bj._qet->asString() << '\n';
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
QueryPlanner::SubtreePlan QueryPlanner::optionalJoin(
    const SubtreePlan& a, const SubtreePlan& b) const {
  // Joining two optional patterns is illegal
  // TODO<joka921/kramerfl> : actually the second one must be the optional
  AD_CHECK(a.type != SubtreePlan::OPTIONAL || b.type != SubtreePlan::OPTIONAL);
  SubtreePlan plan(_qec);

  std::vector<std::array<ColumnIndex, 2>> jcs = getJoinColumns(a, b);

  const vector<size_t>& aSortedOn = a._qet->resultSortedOn();
  const vector<size_t>& bSortedOn = b._qet->resultSortedOn();

  bool aSorted = aSortedOn.size() >= jcs.size();
  // TODO: We could probably also accept permutations of the join columns
  // as the order of a here and then permute the join columns to match the
  // sorted columns of a or b (whichever is larger).
  for (size_t i = 0; aSorted && i < jcs.size(); i++) {
    aSorted = aSorted && jcs[i][0] == aSortedOn[i];
  }
  bool bSorted = bSortedOn.size() >= jcs.size();
  for (size_t i = 0; bSorted && i < jcs.size(); i++) {
    bSorted = bSorted && jcs[i][1] == bSortedOn[i];
  }

  // a and b need to be ordered properly first
  vector<pair<size_t, bool>> sortIndicesA;
  vector<pair<size_t, bool>> sortIndicesB;
  for (array<ColumnIndex, 2>& jc : jcs) {
    sortIndicesA.push_back(std::make_pair(jc[0], false));
    sortIndicesB.push_back(std::make_pair(jc[1], false));
  }

  SubtreePlan orderByPlanA(_qec), orderByPlanB(_qec);
  auto orderByA = std::make_shared<OrderBy>(_qec, a._qet, sortIndicesA);
  auto orderByB = std::make_shared<OrderBy>(_qec, b._qet, sortIndicesB);

  if (!aSorted) {
    orderByPlanA._qet->setOperation(QueryExecutionTree::ORDER_BY, orderByA);
  }
  if (!bSorted) {
    orderByPlanB._qet->setOperation(QueryExecutionTree::ORDER_BY, orderByB);
  }

  auto join = std::make_shared<OptionalJoin>(
      _qec, aSorted ? a._qet : orderByPlanA._qet,
      a.type == SubtreePlan::OPTIONAL, bSorted ? b._qet : orderByPlanB._qet,
      b.type == SubtreePlan::OPTIONAL, jcs);
  QueryExecutionTree& tree = *plan._qet;
  tree.setOperation(QueryExecutionTree::OPTIONAL_JOIN, join);

  plan.type = SubtreePlan::BASIC;
  return plan;
}

// _____________________________________________________________________________
QueryPlanner::SubtreePlan QueryPlanner::minus(const SubtreePlan& a,
                                              const SubtreePlan& b) const {
  AD_CHECK(a.type != SubtreePlan::MINUS && b.type == SubtreePlan::MINUS);
  std::vector<std::array<ColumnIndex, 2>> jcs = getJoinColumns(a, b);

  const vector<size_t>& aSortedOn = a._qet->resultSortedOn();
  const vector<size_t>& bSortedOn = b._qet->resultSortedOn();

  bool aSorted = aSortedOn.size() >= jcs.size();
  // TODO: We could probably also accept permutations of the join columns
  // as the order of a here and then permute the join columns to match the
  // sorted columns of a or b (whichever is larger).
  for (size_t i = 0; aSorted && i < jcs.size(); i++) {
    aSorted = aSorted && jcs[i][0] == aSortedOn[i];
  }
  bool bSorted = bSortedOn.size() >= jcs.size();
  for (size_t i = 0; bSorted && i < jcs.size(); i++) {
    bSorted = bSorted && jcs[i][1] == bSortedOn[i];
  }

  // a and b need to be ordered properly first
  vector<pair<size_t, bool>> sortIndicesA;
  vector<pair<size_t, bool>> sortIndicesB;
  for (const auto& [joinColumnA, joinColumnB] : jcs) {
    sortIndicesA.push_back(std::make_pair(joinColumnA, false));
    sortIndicesB.push_back(std::make_pair(joinColumnB, false));
  }

  SubtreePlan orderByPlanA(_qec), orderByPlanB(_qec);
  auto orderByA = std::make_shared<OrderBy>(_qec, a._qet, sortIndicesA);
  auto orderByB = std::make_shared<OrderBy>(_qec, b._qet, sortIndicesB);

  if (!aSorted) {
    orderByPlanA._qet->setOperation(QueryExecutionTree::ORDER_BY, orderByA);
  }
  if (!bSorted) {
    orderByPlanB._qet->setOperation(QueryExecutionTree::ORDER_BY, orderByB);
  }

  SubtreePlan plan(_qec);

  auto join =
      std::make_shared<Minus>(_qec, aSorted ? a._qet : orderByPlanA._qet,
                              bSorted ? b._qet : orderByPlanB._qet, jcs);
  QueryExecutionTree& tree = *plan._qet;
  tree.setOperation(QueryExecutionTree::MINUS, join);

  plan.type = SubtreePlan::BASIC;
  return plan;
}

// _____________________________________________________________________________
QueryPlanner::SubtreePlan QueryPlanner::multiColumnJoin(
    const SubtreePlan& a, const SubtreePlan& b) const {
  SubtreePlan plan(_qec);

  std::vector<std::array<ColumnIndex, 2>> jcs = getJoinColumns(a, b);

  const vector<size_t>& aSortedOn = a._qet->resultSortedOn();
  const vector<size_t>& bSortedOn = b._qet->resultSortedOn();

  bool aSorted = aSortedOn.size() >= jcs.size();
  // TODO: We could probably also accept permutations of the join columns
  // as the order of a here and then permute the join columns to match the
  // sorted columns of a or b (whichever is larger).
  for (size_t i = 0; aSorted && i < jcs.size(); i++) {
    aSorted = aSorted && jcs[i][0] == aSortedOn[i];
  }
  bool bSorted = bSortedOn.size() >= jcs.size();
  for (size_t i = 0; bSorted && i < jcs.size(); i++) {
    bSorted = bSorted && jcs[i][1] == bSortedOn[i];
  }

  // a and b need to be ordered properly first
  vector<pair<size_t, bool>> sortIndicesA;
  vector<pair<size_t, bool>> sortIndicesB;
  for (const auto& [joinColumnA, joinColumnB] : jcs) {
    sortIndicesA.push_back(std::make_pair(joinColumnA, false));
    sortIndicesB.push_back(std::make_pair(joinColumnB, false));
  }

  SubtreePlan orderByPlanA(_qec), orderByPlanB(_qec);
  auto orderByA = std::make_shared<OrderBy>(_qec, a._qet, sortIndicesA);
  auto orderByB = std::make_shared<OrderBy>(_qec, b._qet, sortIndicesB);

  if (!aSorted) {
    orderByPlanA._qet->setOperation(QueryExecutionTree::ORDER_BY, orderByA);
  }
  if (!bSorted) {
    orderByPlanB._qet->setOperation(QueryExecutionTree::ORDER_BY, orderByB);
  }

  const auto& actualA = aSorted ? a._qet : orderByPlanA._qet;
  const auto& actualB = bSorted ? b._qet : orderByPlanB._qet;
  if (Join::isFullScanDummy(a._qet) || Join::isFullScanDummy(b._qet)) {
    throw std::runtime_error(
        "Trying a JOIN between two full scan dummys, this"
        "will be handled by the calling code automatically");
  }

  auto join = std::make_shared<MultiColumnJoin>(_qec, actualA, actualB, jcs);
  QueryExecutionTree& tree = *plan._qet;
  tree.setOperation(QueryExecutionTree::MULTICOLUMN_JOIN, join);

  return plan;
}

// _____________________________________________________________________________
string QueryPlanner::TripleGraph::asString() const {
  std::ostringstream os;
  for (size_t i = 0; i < _adjLists.size(); ++i) {
    if (_nodeMap.find(i)->second->_cvar.size() == 0) {
      os << i << " " << _nodeMap.find(i)->second->_triple.asString() << " : (";
    } else {
      os << i << " {TextOP for " << _nodeMap.find(i)->second->_cvar
         << ", wordPart: \"" << _nodeMap.find(i)->second->_wordPart
         << "\"} : (";
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
vector<array<ColumnIndex, 2>> QueryPlanner::getJoinColumns(
    const QueryPlanner::SubtreePlan& a,
    const QueryPlanner::SubtreePlan& b) const {
  vector<array<ColumnIndex, 2>> jcs;
  const auto& aVarCols = a._qet->getVariableColumns();
  const auto& bVarCols = b._qet->getVariableColumns();
  for (const auto& aVarCol : aVarCols) {
    auto itt = bVarCols.find(aVarCol.first);
    if (itt != bVarCols.end()) {
      jcs.push_back(array<ColumnIndex, 2>{{aVarCol.second, itt->second}});
    }
  }
  return jcs;
}

// _____________________________________________________________________________
string QueryPlanner::getPruningKey(
    const QueryPlanner::SubtreePlan& plan,
    const vector<size_t>& orderedOnColumns) const {
  // Get the ordered var
  std::ostringstream os;
  const auto& varCols = plan._qet->getVariableColumns();
  for (size_t orderedOnCol : orderedOnColumns) {
    for (const auto& varCol : varCols) {
      if (varCol.second == orderedOnCol) {
        os << varCol.first << ", ";
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
  for (size_t n = 0; n < row.size(); ++n) {
    if (row[n]._qet->getType() == QueryExecutionTree::SCAN &&
        row[n]._qet->getResultWidth() == 3) {
      // Do not apply filters to dummies!
      continue;
    }
    for (size_t i = 0; i < filters.size(); ++i) {
      LOG(TRACE) << "filter type: " << filters[i]._type << std::endl;
      if (((row[n]._idsOfIncludedFilters >> i) & 1) != 0) {
        continue;
      }
      if (row[n]._qet.get()->varCovered(filters[i]._lhs) &&
          std::all_of(filters[i]._additionalLhs.begin(),
                      filters[i]._additionalLhs.end(),
                      [&row, &n](const auto& lhs) {
                        return row[n]._qet->varCovered(lhs);
                      }) &&
          (!isVariable(filters[i]._rhs) ||
           row[n]._qet->varCovered(filters[i]._rhs))) {
        // Apply this filter.
        SubtreePlan newPlan =
            makeSubtreePlan(createFilterOperation(filters[i], row[n]));
        newPlan._idsOfIncludedFilters = row[n]._idsOfIncludedFilters;
        newPlan._idsOfIncludedFilters |= (size_t(1) << i);
        newPlan._idsOfIncludedNodes = row[n]._idsOfIncludedNodes;
        newPlan.type = row[n].type;
        if (replace) {
          row[n] = newPlan;
        } else {
          row.push_back(newPlan);
        }
      }
    }
  }
}

// _____________________________________________________________________________
std::shared_ptr<Filter> QueryPlanner::createFilterOperation(
    const SparqlFilter& filter, const SubtreePlan& parent) const {
  std::shared_ptr<Filter> op = std::make_shared<Filter>(
      _qec, parent._qet, filter._type, filter._lhs, filter._rhs,
      filter._additionalLhs, filter._additionalPrefixes);
  op->setLhsAsString(filter._lhsAsString);
  if (filter._type == SparqlFilter::REGEX) {
    op->setRegexIgnoreCase(filter._regexIgnoreCase);
  }
  return op;
}

// _____________________________________________________________________________
vector<vector<QueryPlanner::SubtreePlan>> QueryPlanner::fillDpTab(
    const QueryPlanner::TripleGraph& tg, const vector<SparqlFilter>& filters,
    const vector<vector<QueryPlanner::SubtreePlan>>& children) {
  size_t numSeeds = tg._nodeMap.size() + children.size();

  if (filters.size() > 64) {
    AD_THROW(ad_semsearch::Exception::BAD_QUERY,
             "At most 64 filters allowed at the moment.");
  }
  LOG(TRACE) << "Fill DP table... (there are " << numSeeds
             << " operations to join)" << std::endl;
  vector<vector<SubtreePlan>> dpTab;
  dpTab.emplace_back(seedWithScansAndText(tg, children));
  applyFiltersIfPossible(dpTab.back(), filters, numSeeds == 1);

  for (size_t k = 2; k <= numSeeds; ++k) {
    LOG(TRACE) << "Producing plans that unite " << k << " triples."
               << std::endl;
    dpTab.emplace_back(vector<SubtreePlan>());
    for (size_t i = 1; i * 2 <= k; ++i) {
      auto newPlans = merge(dpTab[i - 1], dpTab[k - i - 1], tg);
      if (newPlans.size() == 0) {
        continue;
      }
      dpTab[k - 1].insert(dpTab[k - 1].end(), newPlans.begin(), newPlans.end());
      applyFiltersIfPossible(dpTab.back(), filters, numSeeds == k);
    }
    if (dpTab[k - 1].size() == 0) {
      AD_THROW(ad_semsearch::Exception::BAD_QUERY,
               "Could not find a suitable execution tree. "
               "Likely cause: Queries that require joins of the full "
               "index with itself are not supported at the moment.");
    }
  }

  LOG(TRACE) << "Fill DP table done." << std::endl;
  return dpTab;
}

// _____________________________________________________________________________
bool QueryPlanner::TripleGraph::isTextNode(size_t i) const {
  return _nodeMap.count(i) > 0 && (_nodeMap.find(i)->second->_triple._p._iri ==
                                       CONTAINS_ENTITY_PREDICATE ||
                                   _nodeMap.find(i)->second->_triple._p._iri ==
                                       CONTAINS_WORD_PREDICATE ||
                                   _nodeMap.find(i)->second->_triple._p._iri ==
                                       INTERNAL_TEXT_MATCH_PREDICATE);
}

// _____________________________________________________________________________
ad_utility::HashMap<string, vector<size_t>>
QueryPlanner::TripleGraph::identifyTextCliques() const {
  ad_utility::HashMap<string, vector<size_t>> contextVarToTextNodesIds;
  // Fill contextVar -> triples map
  for (size_t i = 0; i < _adjLists.size(); ++i) {
    if (isTextNode(i)) {
      auto& triple = _nodeMap.find(i)->second->_triple;
      auto& cvar = triple._s;
      contextVarToTextNodesIds[cvar.getString()].push_back(i);
    }
  }
  return contextVarToTextNodesIds;
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
  ad_utility::HashSet<string> coveredVariables;
  for (auto n : nodes) {
    auto& node = *_nodeMap.find(n)->second;
    coveredVariables.insert(node._variables.begin(), node._variables.end());
  }
  for (auto& f : origFilters) {
    if (coveredVariables.count(f._lhs) > 0 ||
        coveredVariables.count(f._rhs) > 0) {
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
    _nodeMap[p.first._id] = &_nodeStorage.back();
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
      _nodeStorage.back()._id = _nodeMap.size();
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
void QueryPlanner::TripleGraph::collapseTextCliques() {
  // TODO: Could use more refactoring.
  // In Earlier versions there were no ql:contains... predicates but
  // a symmetric <in-text> predicate. Therefore some parts are still more
  // complex than need be.

  // Create a map from context var to triples it occurs in (the cliques).
  ad_utility::HashMap<string, vector<size_t>> cvarsToTextNodes(
      identifyTextCliques());
  if (cvarsToTextNodes.empty()) {
    return;
  }
  // Now turn each such clique into a new node the represents that whole
  // text operation clique.
  size_t id = 0;
  vector<Node> textNodes;
  ad_utility::HashMap<size_t, size_t> removedNodeIds;
  vector<std::set<size_t>> tnAdjSetsToOldIds;
  for (auto& cvarsToTextNode : cvarsToTextNodes) {
    auto& cvar = cvarsToTextNode.first;
    string wordPart;
    vector<SparqlTriple> trips;
    tnAdjSetsToOldIds.push_back(std::set<size_t>());
    auto& adjNodes = tnAdjSetsToOldIds.back();
    for (auto nid : cvarsToTextNode.second) {
      removedNodeIds[nid] = id;
      adjNodes.insert(_adjLists[nid].begin(), _adjLists[nid].end());
      auto& triple = _nodeMap[nid]->_triple;
      trips.push_back(triple);
      if (triple._s == cvar && triple._o.isString() &&
          !triple._o.isVariable()) {
        if (!wordPart.empty()) {
          wordPart += " ";
        }
        wordPart += triple._o.getString();
      }
      // TODO<joka921> Figure out what is going on here... The subject and the
      // object of a triple are being combined into a string as it seems.
      if (triple._o == cvar && !isVariable(triple._s)) {
        if (!wordPart.empty()) {
          wordPart += " ";
        }
        wordPart += triple._s.toString();
      }
    }
    textNodes.emplace_back(Node(id++, cvar, wordPart, trips));
    assert(tnAdjSetsToOldIds.size() == id);
  }

  // Finally update the graph (node ids and adj lists).
  vector<vector<size_t>> oldAdjLists = _adjLists;
  std::list<TripleGraph::Node> oldNodeStorage = _nodeStorage;
  _nodeStorage.clear();
  _nodeMap.clear();
  _adjLists.clear();
  ad_utility::HashMap<size_t, size_t> idMapOldToNew;
  ad_utility::HashMap<size_t, size_t> idMapNewToOld;

  // Storage and ids.
  for (auto& tn : textNodes) {
    _nodeStorage.push_back(tn);
    _nodeMap[tn._id] = &_nodeStorage.back();
  }

  for (auto& n : oldNodeStorage) {
    if (removedNodeIds.count(n._id) == 0) {
      idMapOldToNew[n._id] = id;
      idMapNewToOld[id] = n._id;
      n._id = id++;
      _nodeStorage.push_back(n);
      _nodeMap[n._id] = &_nodeStorage.back();
    }
  }

  // Adj lists
  // First for newly created text nodes.
  for (size_t i = 0; i < tnAdjSetsToOldIds.size(); ++i) {
    const auto& nodes = tnAdjSetsToOldIds[i];
    std::set<size_t> adjNodes;
    for (auto nid : nodes) {
      if (removedNodeIds.count(nid) == 0) {
        adjNodes.insert(idMapOldToNew[nid]);
      } else if (removedNodeIds[nid] != i) {
        adjNodes.insert(removedNodeIds[nid]);
      }
    }
    vector<size_t> adjList;
    adjList.insert(adjList.begin(), adjNodes.begin(), adjNodes.end());
    _adjLists.emplace_back(adjList);
  }
  assert(_adjLists.size() == textNodes.size());
  assert(_adjLists.size() == tnAdjSetsToOldIds.size());
  // Then for remaining (regular) nodes.
  for (size_t i = textNodes.size(); i < _nodeMap.size(); ++i) {
    const Node& node = *_nodeMap[i];
    const auto& oldAdjList = oldAdjLists[idMapNewToOld[node._id]];
    std::set<size_t> adjNodes;
    for (auto nid : oldAdjList) {
      if (removedNodeIds.count(nid) == 0) {
        adjNodes.insert(idMapOldToNew[nid]);
      } else {
        adjNodes.insert(removedNodeIds[nid]);
      }
    }
    vector<size_t> adjList;
    adjList.insert(adjList.begin(), adjNodes.begin(), adjNodes.end());
    _adjLists.emplace_back(adjList);
  }
}

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
        id_map[n._id] = n2._id;
        id_map_reverse[n2._id] = n._id;
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
  AD_CHECK_GT(lastRow.size(), 0);
  auto compare = [this](const auto& a, const auto& b) {
    auto aCost = a.getCostEstimate(), bCost = b.getCostEstimate();
    if (aCost == bCost && isInTestMode()) {
      // Make the tiebreaking deterministic for the unit tests.
      return a._qet->asString() < b._qet->asString();
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
  const auto& a = !isInTestMode() || ain._qet->asString() < bin._qet->asString()
                      ? ain
                      : bin;
  const auto& b = !isInTestMode() || ain._qet->asString() < bin._qet->asString()
                      ? bin
                      : ain;
  std::vector<SubtreePlan> candidates;

  // We often query for the type of an operation, so we shorten these checks.
  using enum QueryExecutionTree::OperationType;

  // TODO<joka921> find out, what is ACTUALLY the use case for the triple
  // graph. Is it only meant for (questionable) performance reasons
  // or does it change the meaning.
  vector<array<ColumnIndex, 2>> jcs;
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
    AD_THROW(ad_semsearch::Exception::BAD_QUERY,
             "MINUS can only appear after"
             " another graph pattern.");
  }

  if (b.type == SubtreePlan::MINUS) {
    // This case shouldn't happen. If the first pattern is OPTIONAL, it
    // is made non optional earlier. If a minus occurs after an optional
    // further into the query that optional should be resolved by now.
    AD_CHECK(a.type != SubtreePlan::OPTIONAL);
    return std::vector{minus(a, b)};
  }

  // TODO<joka921> OPTIONAL JOINS are not symmetric!
  if (a.type == SubtreePlan::OPTIONAL || b.type == SubtreePlan::OPTIONAL) {
    // Join the two optional columns using an optional join
    return std::vector{optionalJoin(a, b)};
  }

  if (jcs.size() >= 2) {
    // If there are two or more join columns and we are not using the
    // TwoColumnJoin (the if part before this comment), use a multiColumnJoin.
    try {
      SubtreePlan plan = multiColumnJoin(a, b);
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
    SubtreePlan a, SubtreePlan b, const vector<array<ColumnIndex, 2>>& jcs)
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

  const size_t otherCol = aIsTransPath ? jcs[0][1] : jcs[0][0];
  const size_t thisCol = aIsTransPath ? jcs[0][0] : jcs[0][1];
  AD_CHECK(thisCol <= 1);
  // Do not bind the side of a path twice
  if (transPathOperation->isBound()) {
    return std::nullopt;
  }
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
    SubtreePlan a, SubtreePlan b, const vector<array<ColumnIndex, 2>>& jcs)
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
    SubtreePlan a, SubtreePlan b, const vector<array<ColumnIndex, 2>>& jcs)
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
