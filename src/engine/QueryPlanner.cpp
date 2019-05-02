// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./QueryPlanner.h"
#include <algorithm>
#include <ctime>
#include "../parser/ParseException.h"
#include "CountAvailablePredicates.h"
#include "Distinct.h"
#include "Filter.h"
#include "GroupBy.h"
#include "HasPredicateScan.h"
#include "IndexScan.h"
#include "Join.h"
#include "MultiColumnJoin.h"
#include "OptionalJoin.h"
#include "OrderBy.h"
#include "Sort.h"
#include "TextOperationWithFilter.h"
#include "TextOperationWithoutFilter.h"
#include "TwoColumnJoin.h"
#include "Union.h"

// _____________________________________________________________________________
QueryPlanner::QueryPlanner(QueryExecutionContext* qec)
    : _qec(qec), _randVarCount(0) {
  _randomSeed = time(NULL);
}

// _____________________________________________________________________________
QueryExecutionTree QueryPlanner::createExecutionTree(ParsedQuery& pq) {
  // Look for ql:has-predicate to determine if the pattern trick should be used.
  // If the pattern trick is used the ql:has-predicate triple will be removed
  // from the list of where clause triples. Otherwise the ql:has-relation triple
  // will be handled using a HasRelationScan.
  SparqlTriple patternTrickTriple("", PropertyPath(), "");
  bool usePatternTrick = checkUsePatternTrick(&pq, &patternTrickTriple);

  bool doGrouping = pq._groupByVariables.size() > 0 || usePatternTrick;
  if (!doGrouping) {
    // if there is no group by statement, but an aggregate alias is used
    // somewhere do grouping anyways.
    for (const ParsedQuery::Alias& a : pq._aliases) {
      if (a._isAggregate) {
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
    plans.emplace_back(getPatternTrickRow(pq, plans, patternTrickTriple));
  }

  // HAVING
  if (pq._havingClauses.size() > 0) {
    plans.emplace_back(getHavingRow(pq, plans));
  }

  // DISTINCT
  if (pq._distinct) {
    plans.emplace_back(getDistinctRow(pq, plans));
  }

  // ORDER BY
  if (pq._orderBy.size() > 0) {
    // If there is an order by clause, add another row to the table and
    // just add an order by / sort to every previous result if needed.
    // If the ordering is perfect already, just copy the plan.
    plans.emplace_back(getOrderByRow(pq, plans));
  }

  // Now find the cheapest execution plan and store that as the optimal
  // plan for this graph pattern.
  vector<SubtreePlan>& lastRow = plans.back();

  AD_CHECK_GT(lastRow.size(), 0);

  size_t minCost = lastRow[0].getCostEstimate();
  size_t minInd = 0;

  for (size_t i = 1; i < lastRow.size(); ++i) {
    size_t thisCost = lastRow[i].getCostEstimate();
    if (thisCost < minCost) {
      minCost = lastRow[i].getCostEstimate();
      minInd = i;
    }
  }
  lastRow[minInd]._isOptional = pq._rootGraphPattern._optional;

  SubtreePlan final = lastRow[minInd];
  final._qet.get()->setTextLimit(getTextLimit(pq._textLimit));
  LOG(DEBUG) << "Done creating execution plan.\n";
  return *final._qet.get();
}

std::vector<QueryPlanner::SubtreePlan> QueryPlanner::optimize(
    const ParsedQuery::GraphPattern* rootPattern) {
  // Create a topological sorting of the trees GraphPatterns where children are
  // in the list after their parents. This allows for ensuring that children are
  // already optimized when their parents need to be optimized
  std::vector<const ParsedQuery::GraphPattern*> patternsToProcess;
  std::vector<const ParsedQuery::GraphPattern*> childrenToAdd;
  childrenToAdd.push_back(rootPattern);
  while (!childrenToAdd.empty()) {
    const ParsedQuery::GraphPattern* pattern = childrenToAdd.back();
    childrenToAdd.pop_back();
    patternsToProcess.push_back(pattern);
    for (const ParsedQuery::GraphPatternOperation* op : pattern->_children) {
      switch (op->_type) {
        case ParsedQuery::GraphPatternOperation::Type::UNION:
        case ParsedQuery::GraphPatternOperation::Type::OPTIONAL:
          childrenToAdd.insert(childrenToAdd.end(),
                               op->_childGraphPatterns.begin(),
                               op->_childGraphPatterns.end());
          break;
        case ParsedQuery::GraphPatternOperation::Type::TRANS_PATH:
          if (op->_pathData._childGraphPattern != nullptr) {
            childrenToAdd.push_back(op->_pathData._childGraphPattern);
          }
          break;
        case ParsedQuery::GraphPatternOperation::Type::SUBQUERY:
        default:
          break;
      }
      if (op->_type != ParsedQuery::GraphPatternOperation::Type::SUBQUERY) {
        childrenToAdd.insert(childrenToAdd.end(),
                             op->_childGraphPatterns.begin(),
                             op->_childGraphPatterns.end());
      }
    }
  }

  // This vector holds the SubtreePlan for every GraphPattern that was already
  // processed. As the plans ids are dense we can use a vector, but because
  // the ids may be processed in any order the vector needs to be resized here.
  std::vector<SubtreePlan> patternPlans;
  patternPlans.reserve(patternsToProcess.size());
  for (size_t i = 0; i < patternsToProcess.size(); i++) {
    // Using a loop instead of resize as there is no default constructor, and
    // distinct _qet values are needed.
    patternPlans.emplace_back(_qec);
  }
  LOG(DEBUG) << "Got " << patternPlans.size() << " subplans to create."
             << std::endl;

  // Optimize every GraphPattern starting with the leaves of the GraphPattern
  // tree.
  while (!patternsToProcess.empty()) {
    const ParsedQuery::GraphPattern* pattern = patternsToProcess.back();
    patternsToProcess.pop_back();
    std::vector<std::vector<SubtreePlan>> plans;

    // Add all the OPTIONALs and UNIONs of the GraphPattern into the childPlans
    // vector. These will be treated the same as a node in the triple graph
    // later on (so the same as a simple triple).
    vector<const SubtreePlan*> childPlans;
    std::vector<SubtreePlan> unionPlans;
    std::vector<SubtreePlan> subqueryPlans;
    // Ensure the vectors won't reallocate (this is an upper bound on the actual
    // size)
    unionPlans.reserve(pattern->_children.size());
    subqueryPlans.reserve(pattern->_children.size());
    for (ParsedQuery::GraphPatternOperation* child : pattern->_children) {
      switch (child->_type) {
        case ParsedQuery::GraphPatternOperation::Type::OPTIONAL:
          for (const ParsedQuery::GraphPattern* p :
               child->_childGraphPatterns) {
            childPlans.push_back(&patternPlans[p->_id]);
          }
          break;
        case ParsedQuery::GraphPatternOperation::Type::UNION: {
          // the efficiency of the union operation is not dependent on the
          // sorting of the inputs and its position is fixed so it does not
          // need to be part of the optimization of the child.
          const SubtreePlan* left =
              &patternPlans[child->_childGraphPatterns[0]->_id];
          const SubtreePlan* right =
              &patternPlans[child->_childGraphPatterns[1]->_id];

          // create a new subtree plan
          unionPlans.emplace_back(_qec);
          std::shared_ptr<Operation> unionOp(
              new Union(_qec, left->_qet, right->_qet));
          QueryExecutionTree& tree = *unionPlans.back()._qet.get();
          tree.setVariableColumns(
              static_cast<Union*>(unionOp.get())->getVariableColumns());
          tree.setOperation(QueryExecutionTree::UNION, unionOp);
          childPlans.push_back(&unionPlans.back());
        } break;
        case ParsedQuery::GraphPatternOperation::Type::SUBQUERY: {
          subqueryPlans.emplace_back(_qec);
          QueryExecutionTree tree = createExecutionTree(*child->_subquery);
          *subqueryPlans.back()._qet.get() = tree;
          childPlans.push_back(&subqueryPlans.back());
        } break;
        case ParsedQuery::GraphPatternOperation::Type::TRANS_PATH: {
          AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
                   "The * family of operators is not yet implemented");
        } break;
      }
    }

    // Strategy:
    // Create a graph.
    // Each triple corresponds to a node, there is an edge between two nodes iff
    // they share a variable.

    TripleGraph tg = createTripleGraph(pattern);

    // Each node/triple corresponds to a scan (more than one way possible),
    // each edge corresponds to a possible join.

    // Enumerate and judge possible query plans using a DP table.
    // Each ExecutionTree for a sub-problem gives an estimate:
    // There are estimates for cost and size ( and multiplicity per column).
    // Start bottom up, i.e. with the scans for triples.
    // Always merge two solutions from the table by picking one possible join.
    // A join is possible, if there is an edge between the results.
    // Therefore we keep track of all edges that touch a sub-result.
    // When joining two sub-results, the results edges are those that belong
    // to exactly one of the two input sub-trees.
    // If two of them have the same target, only one out edge is created.
    // All edges that are shared by both subtrees, are checked if they are
    // covered by the join or if an extra filter/select is needed.

    // The algorithm then creates all possible plans for 1 to n triples.
    // To generate a plan for k triples, all subsets between i and k-i are
    // joined.

    // Filters are now added to the mix when building execution plans.
    // Without them, a plan has an execution tree and a set of
    // covered triple nodes.
    // With them, it also has a set of covered filters.
    // A filter can be applied as soon as all variables that occur in the filter
    // Are covered by the query. This is also always the place where this is
    // done.

    // Text operations form cliques (all triples connected via the context
    // cvar). Detect them and turn them into nodes with stored word part and
    // edges to connected variables.

    LOG(TRACE) << "Collapse text cliques..." << std::endl;
    tg.collapseTextCliques();
    LOG(TRACE) << "Collapse text cliques done." << std::endl;
    vector<vector<SubtreePlan>> finalTab;

    // Each text operation has two ways how it can be used.
    // 1) As leave in the bottom row of the tab.
    // According to the number of connected variables, the operation creates
    // a cross product with n entities that can be used in subsequent joins.
    // 2) as intermediate unary (downwards) nodes in the execution tree.
    // This is a bit similar to sorts: they can be applied after each step
    // and will filter on one variable.
    // Cycles have to be avoided (by previously removing a triple and using it
    // as a filter later on).
    std::vector<SubtreePlan> lastRow =
        fillDpTab(tg, pattern->_filters, childPlans).back();

    if (pattern == rootPattern) {
      return lastRow;
    } else {
      AD_CHECK_GT(lastRow.size(), 0);
      size_t minCost = lastRow[0].getCostEstimate();
      size_t minInd = 0;
      for (size_t i = 1; i < lastRow.size(); ++i) {
        size_t thisCost = lastRow[i].getCostEstimate();
        if (thisCost < minCost) {
          minCost = lastRow[i].getCostEstimate();
          minInd = i;
        }
      }
      lastRow[minInd]._isOptional = pattern->_optional;
      patternPlans[pattern->_id] = lastRow[minInd];
    }
  }
  std::ostringstream error;
  error << "The optimizer found no way to optimize the graph pattern: ";
  rootPattern->toString(error, 0);
  AD_THROW(ad_semsearch::Exception::BAD_INPUT, error.str());
}

bool QueryPlanner::checkUsePatternTrick(
    ParsedQuery* pq, SparqlTriple* patternTrickTriple) const {
  bool usePatternTrick = false;
  // Check if the query has the right number of variables for aliases and
  // group by.
  if (pq->_groupByVariables.size() == 1 && pq->_aliases.size() == 1) {
    const ParsedQuery::Alias& alias = pq->_aliases.back();
    // Create a lower case version of the aliases function string to allow
    // for case insensitive keyword detection.
    std::string aliasFunctionLower =
        ad_utility::getLowercaseUtf8(alias._function);
    // Check if the alias is a non distinct count alias
    if (alias._isAggregate &&
        aliasFunctionLower.find("distinct") == std::string::npos &&
        ad_utility::startsWith(aliasFunctionLower, "count")) {
      // look for a HAS_RELATION_PREDICATE triple
      for (size_t i = 0; i < pq->_rootGraphPattern._whereClauseTriples.size();
           i++) {
        const SparqlTriple& t = pq->_rootGraphPattern._whereClauseTriples[i];
        // Check that the triples predicates is the HAS_PREDICATE_PREDICATE.
        // Also check that the triples object matches the aliases input
        // variable and the group by variable.
        if (t._p._iri == HAS_PREDICATE_PREDICATE && alias._inVarName == t._o &&
            pq->_groupByVariables[0] == t._o) {
          // Assume we will use the pattern trick for now but run several more
          // checks before actually modifying the query.
          usePatternTrick = true;
          // check that all selected variables are outputs of
          // CountAvailablePredicates
          for (const std::string& s : pq->_selectedVariables) {
            if (s != t._o && s != alias._outVarName) {
              usePatternTrick = false;
              break;
            }
          }
          // Check for triples containing the ql:has-predicate triple's
          // object.
          for (size_t j = 0;
               usePatternTrick &&
               j < pq->_rootGraphPattern._whereClauseTriples.size();
               j++) {
            const SparqlTriple& other =
                pq->_rootGraphPattern._whereClauseTriples[j];
            if (j != i && (other._s == t._o || other._p._iri == t._o ||
                           other._o == t._o)) {
              usePatternTrick = false;
            }
          }
          // Don't run any more checks if we already determined that the
          // pattern trick is not going to be used.
          if (usePatternTrick) {
            // Check for filters on the ql:has-predicate triple's subject or
            // object.
            // Filters that filter on the triple's object but have a static
            // rhs will be transformed to a having clause later on.
            for (const SparqlFilter& filter : pq->_rootGraphPattern._filters) {
              if (!(filter._lhs == t._o && filter._rhs[0] != '?') &&
                  (filter._lhs == t._s || filter._rhs == t._o ||
                   filter._rhs == t._s)) {
                usePatternTrick = false;
                break;
              }
            }
          }
          // Don't run any more checks if we already determined that the
          // pattern trick is not going to be used.
          if (usePatternTrick) {
            // Check for optional parts containing the ql:has-predicate
            // triple's object
            std::vector<const ParsedQuery::GraphPattern*> graphsToProcess;
            for (const ParsedQuery::GraphPatternOperation* op :
                 pq->_rootGraphPattern._children) {
              graphsToProcess.insert(graphsToProcess.end(),
                                     op->_childGraphPatterns.begin(),
                                     op->_childGraphPatterns.end());
            }
            while (!graphsToProcess.empty()) {
              const ParsedQuery::GraphPattern* pattern = graphsToProcess.back();
              graphsToProcess.pop_back();

              for (const ParsedQuery::GraphPatternOperation* op :
                   pattern->_children) {
                graphsToProcess.insert(graphsToProcess.end(),
                                       op->_childGraphPatterns.begin(),
                                       op->_childGraphPatterns.end());
              }

              for (const SparqlTriple& other : pattern->_whereClauseTriples) {
                if (other._s == t._o || other._p._iri == t._o ||
                    other._o == t._o) {
                  usePatternTrick = false;
                  break;
                }
              }
              if (!usePatternTrick) {
                break;
              }
            }
          }
          if (usePatternTrick) {
            LOG(DEBUG) << "Using the pattern trick to answer the query."
                       << endl;
            *patternTrickTriple = t;
            // remove the triple from the graph
            pq->_rootGraphPattern._whereClauseTriples.erase(
                pq->_rootGraphPattern._whereClauseTriples.begin() + i);
            // Transform filters on the ql:has-relation triple's object that
            // have a static rhs to having clauses
            for (size_t i = 0; i < pq->_rootGraphPattern._filters.size(); i++) {
              const SparqlFilter& filter = pq->_rootGraphPattern._filters[i];
              if (filter._lhs == t._o && filter._rhs[0] != '?') {
                pq->_havingClauses.push_back(filter);
                pq->_rootGraphPattern._filters.erase(
                    pq->_rootGraphPattern._filters.begin() + i);
                i--;
              }
            }
          }
        }
      }
    }
  }
  return usePatternTrick;
}

// _____________________________________________________________________________
vector<QueryPlanner::SubtreePlan> QueryPlanner::getDistinctRow(
    const ParsedQuery& pq, const vector<vector<SubtreePlan>>& dpTab) const {
  const vector<SubtreePlan>& previous = dpTab[dpTab.size() - 1];
  vector<SubtreePlan> added;
  added.reserve(previous.size());
  for (size_t i = 0; i < previous.size(); ++i) {
    const SubtreePlan& parent = previous[i];
    SubtreePlan distinctPlan(_qec);
    vector<size_t> keepIndices;
    ad_utility::HashSet<size_t> indDone;
    const auto& colMap = parent._qet->getVariableColumns();
    for (const auto& var : pq._selectedVariables) {
      const auto it = colMap.find(var);
      if (it != colMap.end()) {
        auto ind = it->second;
        if (indDone.count(ind) == 0) {
          keepIndices.push_back(ind);
          indDone.insert(ind);
        }
      } else if (ad_utility::startsWith(var, "SCORE(") ||
                 ad_utility::startsWith(var, "TEXT(")) {
        auto varInd = var.find('?');
        auto cVar = var.substr(varInd, var.rfind(')') - varInd);
        const auto it = colMap.find(cVar);
        if (it != colMap.end()) {
          auto ind = it->second;
          if (indDone.count(ind) == 0) {
            keepIndices.push_back(ind);
            indDone.insert(ind);
          }
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
      std::shared_ptr<Operation> distinct(
          new Distinct(_qec, parent._qet, keepIndices));
      distinctPlan._qet->setOperation(QueryExecutionTree::DISTINCT, distinct);
      distinctPlan._qet->setVariableColumns(
          static_cast<Distinct*>(distinct.get())->getVariableColumns());
      distinctPlan._qet->setContextVars(parent._qet.get()->getContextVars());
    } else {
      if (keepIndices.size() == 1) {
        std::shared_ptr<QueryExecutionTree> tree(new QueryExecutionTree(_qec));
        std::shared_ptr<Operation> sort(
            new Sort(_qec, parent._qet, keepIndices[0]));
        tree->setVariableColumns(parent._qet->getVariableColumns());
        tree->setOperation(QueryExecutionTree::SORT, sort);
        tree->setContextVars(parent._qet.get()->getContextVars());
        std::shared_ptr<Operation> distinct(
            new Distinct(_qec, tree, keepIndices));
        distinctPlan._qet->setOperation(QueryExecutionTree::DISTINCT, distinct);
        distinctPlan._qet->setVariableColumns(
            static_cast<Distinct*>(distinct.get())->getVariableColumns());
        distinctPlan._qet->setContextVars(parent._qet.get()->getContextVars());
      } else {
        std::shared_ptr<QueryExecutionTree> tree(new QueryExecutionTree(_qec));
        vector<pair<size_t, bool>> obCols;
        for (auto& i : keepIndices) {
          obCols.emplace_back(std::make_pair(i, false));
        }
        std::shared_ptr<Operation> ob(new OrderBy(_qec, parent._qet, obCols));
        tree->setVariableColumns(parent._qet->getVariableColumns());
        tree->setOperation(QueryExecutionTree::ORDER_BY, ob);
        tree->setContextVars(parent._qet.get()->getContextVars());
        std::shared_ptr<Operation> distinct(
            new Distinct(_qec, tree, keepIndices));
        distinctPlan._qet->setOperation(QueryExecutionTree::DISTINCT, distinct);
        distinctPlan._qet->setVariableColumns(
            static_cast<Distinct*>(distinct.get())->getVariableColumns());
        distinctPlan._qet->setContextVars(parent._qet.get()->getContextVars());
      }
    }
    added.push_back(distinctPlan);
  }
  return added;
}

// _____________________________________________________________________________
vector<QueryPlanner::SubtreePlan> QueryPlanner::getPatternTrickRow(
    const ParsedQuery& pq, const vector<vector<SubtreePlan>>& dpTab,
    const SparqlTriple& patternTrickTriple) const {
  const vector<SubtreePlan>* previous = nullptr;
  if (!dpTab.empty()) {
    previous = &dpTab.back();
  }
  vector<SubtreePlan> added;
  if (previous != nullptr && !previous->empty()) {
    added.reserve(previous->size());
    for (size_t i = 0; i < previous->size(); ++i) {
      const SubtreePlan& parent = (*previous)[i];
      // Determine the column containing the subjects for which we are
      // interested in their predicates.
      auto it =
          parent._qet.get()->getVariableColumns().find(patternTrickTriple._s);
      if (it == parent._qet.get()->getVariableColumns().end()) {
        AD_THROW(ad_semsearch::Exception::BAD_QUERY,
                 "The root operation of the "
                 "query excecution tree does "
                 "not contain a column for "
                 "variable " +
                     patternTrickTriple._s +
                     " required by the pattern "
                     "trick.");
      }
      size_t subjectColumn = it->second;
      const std::vector<size_t>& resultSortedOn =
          parent._qet->getRootOperation()->getResultSortedOn();
      bool isSorted =
          resultSortedOn.size() > 0 && resultSortedOn[0] == subjectColumn;
      // a and b need to be ordered properly first
      vector<pair<size_t, bool>> sortIndices = {
          std::make_pair(subjectColumn, false)};

      SubtreePlan orderByPlan(_qec);
      if (!isSorted) {
        std::shared_ptr<Operation> orderByOp =
            std::make_shared<OrderBy>(_qec, parent._qet, sortIndices);
        orderByPlan._qet->setVariableColumns(parent._qet->getVariableColumns());
        orderByPlan._qet->setOperation(QueryExecutionTree::ORDER_BY, orderByOp);
      }
      SubtreePlan patternTrickPlan(_qec);
      std::shared_ptr<Operation> countPred(new CountAvailablePredicates(
          _qec, isSorted ? parent._qet : orderByPlan._qet, subjectColumn));

      static_cast<CountAvailablePredicates*>(countPred.get())
          ->setVarNames(patternTrickTriple._o, pq._aliases[0]._outVarName);
      QueryExecutionTree& tree = *patternTrickPlan._qet.get();
      tree.setVariableColumns(
          static_cast<CountAvailablePredicates*>(countPred.get())
              ->getVariableColumns());
      tree.setOperation(QueryExecutionTree::COUNT_AVAILABLE_PREDICATES,
                        countPred);
      added.push_back(patternTrickPlan);
    }
  } else if (patternTrickTriple._s[0] != '?') {
    // The subject of the pattern trick is not a variable
    SubtreePlan patternTrickPlan(_qec);
    std::shared_ptr<Operation> countPred(
        new CountAvailablePredicates(_qec, patternTrickTriple._s));

    static_cast<CountAvailablePredicates*>(countPred.get())
        ->setVarNames(patternTrickTriple._o, pq._aliases[0]._outVarName);
    QueryExecutionTree& tree = *patternTrickPlan._qet.get();
    tree.setVariableColumns(
        static_cast<CountAvailablePredicates*>(countPred.get())
            ->getVariableColumns());
    tree.setOperation(QueryExecutionTree::COUNT_AVAILABLE_PREDICATES,
                      countPred);
    added.push_back(patternTrickPlan);
  } else {
    // Use the pattern trick without a subtree
    SubtreePlan patternTrickPlan(_qec);
    std::shared_ptr<Operation> countPred(new CountAvailablePredicates(_qec));

    static_cast<CountAvailablePredicates*>(countPred.get())
        ->setVarNames(patternTrickTriple._o, pq._aliases[0]._outVarName);
    QueryExecutionTree& tree = *patternTrickPlan._qet.get();
    tree.setVariableColumns(
        static_cast<CountAvailablePredicates*>(countPred.get())
            ->getVariableColumns());
    tree.setOperation(QueryExecutionTree::COUNT_AVAILABLE_PREDICATES,
                      countPred);
    added.push_back(patternTrickPlan);
  }
  return added;
}

// _____________________________________________________________________________
vector<QueryPlanner::SubtreePlan> QueryPlanner::getHavingRow(
    const ParsedQuery& pq, const vector<vector<SubtreePlan>>& dpTab) const {
  const vector<SubtreePlan>& previous = dpTab[dpTab.size() - 1];
  vector<SubtreePlan> added;
  added.reserve(previous.size());
  for (size_t i = 0; i < previous.size(); ++i) {
    const SubtreePlan& parent = previous[i];
    SubtreePlan filtered = parent;
    for (const SparqlFilter& filter : pq._havingClauses) {
      SubtreePlan plan(_qec);
      auto& tree = *plan._qet.get();
      tree.setVariableColumns(parent._qet.get()->getVariableColumns());
      tree.setOperation(QueryExecutionTree::FILTER,
                        createFilterOperation(filter, parent));
      tree.setContextVars(parent._qet.get()->getContextVars());
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
  for (size_t i = 0; i < previous.size(); ++i) {
    const SubtreePlan* parent = &previous[i];
    // Create a group by operation to determine on which columns the input
    // needs to be sorted
    SubtreePlan groupByPlan(_qec);
    groupByPlan._idsOfIncludedNodes = parent->_idsOfIncludedNodes;
    groupByPlan._idsOfIncludedFilters = parent->_idsOfIncludedFilters;
    std::shared_ptr<Operation> groupBy =
        std::make_shared<GroupBy>(_qec, pq._groupByVariables, pq._aliases);
    QueryExecutionTree& groupByTree = *groupByPlan._qet.get();

    // Then compute the sort columns
    std::vector<std::pair<size_t, bool>> sortColumns =
        static_cast<GroupBy*>(groupBy.get())->computeSortColumns(parent->_qet);

    const std::vector<size_t>& inputSortedOn =
        parent->_qet->getRootOperation()->getResultSortedOn();

    bool inputSorted = sortColumns.size() <= inputSortedOn.size();
    for (size_t i = 0; inputSorted && i < sortColumns.size(); i++) {
      inputSorted = sortColumns[i].first == inputSortedOn[i];
    }
    // Create the plan here to avoid it falling out of context early
    SubtreePlan orderByPlan(_qec);
    if (!sortColumns.empty() && !inputSorted) {
      // Create an order by operation as required by the group by
      std::shared_ptr<Operation> orderBy =
          std::make_shared<OrderBy>(_qec, parent->_qet, sortColumns);
      QueryExecutionTree& orderByTree = *orderByPlan._qet.get();
      orderByTree.setVariableColumns(parent->_qet->getVariableColumns());
      orderByTree.setOperation(QueryExecutionTree::ORDER_BY, orderBy);
      parent = &orderByPlan;
    }

    static_cast<GroupBy*>(groupBy.get())->setSubtree(parent->_qet);
    groupByTree.setVariableColumns(
        static_cast<GroupBy*>(groupBy.get())->getVariableColumns());
    groupByTree.setOperation(QueryExecutionTree::GROUP_BY, groupBy);
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
  for (size_t i = 0; i < previous.size(); ++i) {
    SubtreePlan plan(_qec);
    auto& tree = *plan._qet.get();
    plan._idsOfIncludedNodes = previous[i]._idsOfIncludedNodes;
    plan._idsOfIncludedFilters = previous[i]._idsOfIncludedFilters;
    if (pq._orderBy.size() == 1 && !pq._orderBy[0]._desc) {
      size_t col =
          previous[i]._qet.get()->getVariableColumn(pq._orderBy[0]._key);
      const std::vector<size_t>& previousSortedOn =
          previous[i]._qet.get()->resultSortedOn();
      if (previousSortedOn.size() > 0 && col == previousSortedOn[0]) {
        // Already sorted perfectly
        added.push_back(previous[i]);
      } else {
        std::shared_ptr<Operation> sort(new Sort(_qec, previous[i]._qet, col));
        tree.setVariableColumns(previous[i]._qet.get()->getVariableColumns());
        tree.setOperation(QueryExecutionTree::SORT, sort);
        tree.setContextVars(previous[i]._qet.get()->getContextVars());
        added.push_back(plan);
      }
    } else {
      vector<pair<size_t, bool>> sortIndices;
      for (auto& ord : pq._orderBy) {
        sortIndices.emplace_back(pair<size_t, bool>{
            previous[i]._qet.get()->getVariableColumn(ord._key), ord._desc});
      }
      const std::vector<size_t>& previousSortedOn =
          previous[i]._qet.get()->resultSortedOn();
      bool alreadySorted = previousSortedOn.size() >= sortIndices.size();
      for (size_t i = 0; alreadySorted && i < sortIndices.size(); i++) {
        alreadySorted = alreadySorted && !sortIndices[i].second &&
                        sortIndices[i].first == previousSortedOn[i];
      }
      if (alreadySorted) {
        // Already sorted perfectly
        added.push_back(previous[i]);
      } else {
        std::shared_ptr<Operation> ob(
            new OrderBy(_qec, previous[i]._qet, sortIndices));
        tree.setVariableColumns(previous[i]._qet.get()->getVariableColumns());
        tree.setOperation(QueryExecutionTree::ORDER_BY, ob);
        tree.setContextVars(previous[i]._qet.get()->getContextVars());

        added.push_back(plan);
      }
    }
  }
  return added;
}

// _____________________________________________________________________________
void QueryPlanner::getVarTripleMap(
    const ParsedQuery& pq,
    ad_utility::HashMap<string, vector<SparqlTriple>>* varToTrip,
    ad_utility::HashSet<string>* contextVars) const {
  for (auto& t : pq._rootGraphPattern._whereClauseTriples) {
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

// _____________________________________________________________________________
bool QueryPlanner::isVariable(const string& elem) {
  return ad_utility::startsWith(elem, "?");
}

bool QueryPlanner::isVariable(const PropertyPath& elem) {
  return elem._operation == PropertyPath::Operation::IRI &&
         isVariable(elem._iri);
}
// _____________________________________________________________________________
QueryPlanner::TripleGraph QueryPlanner::createTripleGraph(
    const ParsedQuery::GraphPattern* pattern) const {
  TripleGraph tg;
  if (pattern->_whereClauseTriples.size() > 64) {
    AD_THROW(ad_semsearch::Exception::BAD_QUERY,
             "At most 64 triples allowed at the moment.");
  }
  if (pattern->_filters.size() > 64) {
    AD_THROW(ad_semsearch::Exception::BAD_QUERY,
             "At most 64 filters allowed at the moment.");
  }
  for (auto& t : pattern->_whereClauseTriples) {
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
    const vector<const QueryPlanner::SubtreePlan*>& children) {
  vector<SubtreePlan> seeds;
  // add all child plans as seeds
  uint32_t idShift = tg._nodeMap.size();
  for (const SubtreePlan* plan : children) {
    SubtreePlan newIdPlan = *plan;
    // give the plan a unique id bit
    newIdPlan._idsOfIncludedNodes = size_t(1) << idShift;
    newIdPlan._idsOfIncludedFilters = 0;
    seeds.push_back(newIdPlan);
    idShift++;
  }
  for (size_t i = 0; i < tg._nodeMap.size(); ++i) {
    const TripleGraph::Node& node = *tg._nodeMap.find(i)->second;
    if (node._cvar.size() > 0) {
      seeds.push_back(getTextLeafPlan(node));
    } else {
      if (node._variables.size() == 0) {
        AD_THROW(
            ad_semsearch::Exception::BAD_QUERY,
            "Triples should have at least one variable. Not the case in: " +
                node._triple.asString());
      }
      // Simple iris can be resolved directly.
      if (node._triple._p._operation == PropertyPath::Operation::IRI) {
        if (node._variables.size() == 1) {
          // Just pick one direction, they should be equivalent.
          SubtreePlan plan(_qec);
          plan._idsOfIncludedNodes |= (uint64_t(1) << i);
          auto& tree = *plan._qet.get();
          if (node._triple._p._iri == HAS_PREDICATE_PREDICATE) {
            // Add a has relation scan instead of a normal IndexScan
            HasPredicateScan::ScanType scanType;
            if (isVariable(node._triple._s)) {
              scanType = HasPredicateScan::ScanType::FREE_S;
            } else if (isVariable(node._triple._o)) {
              scanType = HasPredicateScan::ScanType::FREE_O;
            }
            std::shared_ptr<Operation> scan =
                std::make_shared<HasPredicateScan>(_qec, scanType);
            static_cast<HasPredicateScan*>(scan.get())
                ->setSubject(node._triple._s);
            static_cast<HasPredicateScan*>(scan.get())
                ->setObject(node._triple._o);
            tree.setOperation(
                QueryExecutionTree::OperationType::HAS_RELATION_SCAN, scan);
            tree.setVariableColumns(static_cast<HasPredicateScan*>(scan.get())
                                        ->getVariableColumns());
          } else if (isVariable(node._triple._s)) {
            std::shared_ptr<Operation> scan(
                new IndexScan(_qec, IndexScan::ScanType::POS_BOUND_O));
            static_cast<IndexScan*>(scan.get())->setSubject(node._triple._s);
            static_cast<IndexScan*>(scan.get())
                ->setPredicate(node._triple._p._iri);
            static_cast<IndexScan*>(scan.get())->setObject(node._triple._o);
            tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
            tree.setVariableColumn(node._triple._s, 0);
          } else if (isVariable(node._triple._o)) {
            std::shared_ptr<Operation> scan(
                new IndexScan(_qec, IndexScan::ScanType::PSO_BOUND_S));
            static_cast<IndexScan*>(scan.get())->setSubject(node._triple._s);
            static_cast<IndexScan*>(scan.get())
                ->setPredicate(node._triple._p._iri);
            static_cast<IndexScan*>(scan.get())->setObject(node._triple._o);
            tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
            tree.setVariableColumn(node._triple._o, 0);
          } else {
            std::shared_ptr<Operation> scan(
                new IndexScan(_qec, IndexScan::ScanType::SOP_BOUND_O));
            static_cast<IndexScan*>(scan.get())->setSubject(node._triple._s);
            static_cast<IndexScan*>(scan.get())
                ->setPredicate(node._triple._p._iri);
            static_cast<IndexScan*>(scan.get())->setObject(node._triple._o);
            tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
            tree.setVariableColumn(node._triple._p._iri, 0);
          }
          seeds.push_back(plan);
        } else if (node._variables.size() == 2) {
          // Add plans for both possible scan directions.
          if (node._triple._p._iri == HAS_PREDICATE_PREDICATE) {
            // Add a has relation scan instead of a normal IndexScan
            SubtreePlan plan(_qec);
            plan._idsOfIncludedNodes |= (uint64_t(1) << i);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation> scan(new HasPredicateScan(
                _qec, HasPredicateScan::ScanType::FULL_SCAN));
            static_cast<HasPredicateScan*>(scan.get())
                ->setSubject(node._triple._s);
            static_cast<HasPredicateScan*>(scan.get())
                ->setObject(node._triple._o);
            tree.setOperation(
                QueryExecutionTree::OperationType::HAS_RELATION_SCAN, scan);
            tree.setVariableColumns(static_cast<HasPredicateScan*>(scan.get())
                                        ->getVariableColumns());
            seeds.push_back(plan);
          } else if (!isVariable(node._triple._p._iri)) {
            {
              SubtreePlan plan(_qec);
              plan._idsOfIncludedNodes |= (uint64_t(1) << i);
              auto& tree = *plan._qet.get();
              std::shared_ptr<Operation> scan(
                  new IndexScan(_qec, IndexScan::ScanType::PSO_FREE_S));
              static_cast<IndexScan*>(scan.get())->setSubject(node._triple._s);
              static_cast<IndexScan*>(scan.get())
                  ->setPredicate(node._triple._p._iri);
              static_cast<IndexScan*>(scan.get())->setObject(node._triple._o);
              static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
              tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
              tree.setVariableColumn(node._triple._s, 0);
              tree.setVariableColumn(node._triple._o, 1);
              seeds.push_back(plan);
            }
            {
              SubtreePlan plan(_qec);
              plan._idsOfIncludedNodes |= (uint64_t(1) << i);
              auto& tree = *plan._qet.get();
              std::shared_ptr<Operation> scan(
                  new IndexScan(_qec, IndexScan::ScanType::POS_FREE_O));
              static_cast<IndexScan*>(scan.get())->setSubject(node._triple._s);
              static_cast<IndexScan*>(scan.get())
                  ->setPredicate(node._triple._p._iri);
              static_cast<IndexScan*>(scan.get())->setObject(node._triple._o);
              static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
              tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
              tree.setVariableColumn(node._triple._o, 0);
              tree.setVariableColumn(node._triple._s, 1);
              seeds.push_back(plan);
            }
          } else if (!isVariable(node._triple._s)) {
            {
              SubtreePlan plan(_qec);
              plan._idsOfIncludedNodes |= (uint64_t(1) << i);
              auto& tree = *plan._qet.get();
              std::shared_ptr<Operation> scan(
                  new IndexScan(_qec, IndexScan::ScanType::SPO_FREE_P));
              static_cast<IndexScan*>(scan.get())->setSubject(node._triple._s);
              static_cast<IndexScan*>(scan.get())
                  ->setPredicate(node._triple._p._iri);
              static_cast<IndexScan*>(scan.get())->setObject(node._triple._o);
              static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
              tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
              tree.setVariableColumn(node._triple._p._iri, 0);
              tree.setVariableColumn(node._triple._o, 1);
              seeds.push_back(plan);
            }
            {
              SubtreePlan plan(_qec);
              plan._idsOfIncludedNodes |= (uint64_t(1) << i);
              auto& tree = *plan._qet.get();
              std::shared_ptr<Operation> scan(
                  new IndexScan(_qec, IndexScan::ScanType::SOP_FREE_O));
              static_cast<IndexScan*>(scan.get())->setSubject(node._triple._s);
              static_cast<IndexScan*>(scan.get())
                  ->setPredicate(node._triple._p._iri);
              static_cast<IndexScan*>(scan.get())->setObject(node._triple._o);
              static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
              tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
              tree.setVariableColumn(node._triple._o, 0);
              tree.setVariableColumn(node._triple._p._iri, 1);
              seeds.push_back(plan);
            }
          } else if (!isVariable(node._triple._o)) {
            {
              SubtreePlan plan(_qec);
              plan._idsOfIncludedNodes |= (uint64_t(1) << i);
              auto& tree = *plan._qet.get();
              std::shared_ptr<Operation> scan(
                  new IndexScan(_qec, IndexScan::ScanType::OSP_FREE_S));
              static_cast<IndexScan*>(scan.get())->setSubject(node._triple._s);
              static_cast<IndexScan*>(scan.get())
                  ->setPredicate(node._triple._p._iri);
              static_cast<IndexScan*>(scan.get())->setObject(node._triple._o);
              static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
              tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
              tree.setVariableColumn(node._triple._s, 0);
              tree.setVariableColumn(node._triple._p._iri, 1);
              seeds.push_back(plan);
            }
            {
              SubtreePlan plan(_qec);
              plan._idsOfIncludedNodes |= (uint64_t(1) << i);
              auto& tree = *plan._qet.get();
              std::shared_ptr<Operation> scan(
                  new IndexScan(_qec, IndexScan::ScanType::OPS_FREE_P));
              static_cast<IndexScan*>(scan.get())->setSubject(node._triple._s);
              static_cast<IndexScan*>(scan.get())
                  ->setPredicate(node._triple._p._iri);
              static_cast<IndexScan*>(scan.get())->setObject(node._triple._o);
              static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
              tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
              tree.setVariableColumn(node._triple._p._iri, 0);
              tree.setVariableColumn(node._triple._s, 1);
              seeds.push_back(plan);
            }
          }
        } else {
          if (!_qec || _qec->getIndex().hasAllPermutations()) {
            // Add plans for all six permutations.
            // SPO
            {
              SubtreePlan plan(_qec);
              plan._idsOfIncludedNodes |= (uint64_t(1) << i);
              auto& tree = *plan._qet.get();
              std::shared_ptr<Operation> scan(new IndexScan(
                  _qec, IndexScan::ScanType::FULL_INDEX_SCAN_SPO));
              static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
              tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
              tree.setVariableColumn(node._triple._s, 0);
              tree.setVariableColumn(node._triple._p._iri, 1);
              tree.setVariableColumn(node._triple._o, 2);
              seeds.push_back(plan);
            }
            // SOP
            {
              SubtreePlan plan(_qec);
              plan._idsOfIncludedNodes |= (uint64_t(1) << i);
              auto& tree = *plan._qet.get();
              std::shared_ptr<Operation> scan(new IndexScan(
                  _qec, IndexScan::ScanType::FULL_INDEX_SCAN_SOP));
              static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
              tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
              tree.setVariableColumn(node._triple._s, 0);
              tree.setVariableColumn(node._triple._o, 1);
              tree.setVariableColumn(node._triple._p._iri, 2);
              seeds.push_back(plan);
            }
            // PSO
            {
              SubtreePlan plan(_qec);
              plan._idsOfIncludedNodes |= (uint64_t(1) << i);
              auto& tree = *plan._qet.get();
              std::shared_ptr<Operation> scan(new IndexScan(
                  _qec, IndexScan::ScanType::FULL_INDEX_SCAN_PSO));
              static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
              tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
              tree.setVariableColumn(node._triple._p._iri, 0);
              tree.setVariableColumn(node._triple._s, 1);
              tree.setVariableColumn(node._triple._o, 2);
              seeds.push_back(plan);
            }
            // POS
            {
              SubtreePlan plan(_qec);
              plan._idsOfIncludedNodes |= (uint64_t(1) << i);
              auto& tree = *plan._qet.get();
              std::shared_ptr<Operation> scan(new IndexScan(
                  _qec, IndexScan::ScanType::FULL_INDEX_SCAN_POS));
              static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
              tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
              tree.setVariableColumn(node._triple._p._iri, 0);
              tree.setVariableColumn(node._triple._o, 1);
              tree.setVariableColumn(node._triple._s, 2);
              seeds.push_back(plan);
            }
            // OSP
            {
              SubtreePlan plan(_qec);
              plan._idsOfIncludedNodes |= (uint64_t(1) << i);
              auto& tree = *plan._qet.get();
              std::shared_ptr<Operation> scan(new IndexScan(
                  _qec, IndexScan::ScanType::FULL_INDEX_SCAN_OSP));
              static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
              tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
              tree.setVariableColumn(node._triple._o, 0);
              tree.setVariableColumn(node._triple._s, 1);
              tree.setVariableColumn(node._triple._p._iri, 2);
              seeds.push_back(plan);
            }
            // OPS
            {
              SubtreePlan plan(_qec);
              plan._idsOfIncludedNodes |= (uint64_t(1) << i);
              auto& tree = *plan._qet.get();
              std::shared_ptr<Operation> scan(new IndexScan(
                  _qec, IndexScan::ScanType::FULL_INDEX_SCAN_OPS));
              static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
              tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
              tree.setVariableColumn(node._triple._o, 0);
              tree.setVariableColumn(node._triple._p._iri, 1);
              tree.setVariableColumn(node._triple._s, 2);
              seeds.push_back(plan);
            }
          } else {
            AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
                     "With only 2 permutations registered (no -a option), "
                     "triples should have at most two variables. "
                     "Not the case in: " +
                         node._triple.asString());
          }
        }
      } else {
        // The Property path is complex
        std::vector<SubtreePlan> plans =
            seedFromPropertyPathTriple(node._triple);
        for (SubtreePlan& plan : plans) {
          plan._idsOfIncludedNodes = (uint64_t(1) << i);
        }
        seeds.insert(seeds.end(), plans.begin(), plans.end());
      }
    }
  }
  return seeds;
}

// _____________________________________________________________________________
vector<QueryPlanner::SubtreePlan> QueryPlanner::seedFromPropertyPathTriple(
    const SparqlTriple& triple) {
  ParsedQuery::GraphPattern pattern =
      seedFromPropertyPath(triple._s, triple._p, triple._o);
  std::ostringstream out;
  pattern.toString(out, 0);
  std::cout << "Turned " << triple.asString() << " into " << std::endl;
  std::cout << out.str() << std::endl << std::endl;
  pattern.recomputeIds();
  return optimize(&pattern);
}

ParsedQuery::GraphPattern QueryPlanner::seedFromPropertyPath(
    const std::string& left, const PropertyPath& path,
    const std::string& right) {
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
          ((int)path._operation));
}

// _____________________________________________________________________________
ParsedQuery::GraphPattern QueryPlanner::seedFromSequence(
    const std::string& left, const PropertyPath& path,
    const std::string& right) {
  if (path._children.size() == 0) {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
             "Tried processing a sequence property path node without any "
             "children.");
  } else if (path._children.size() == 1) {
    LOG(WARN) << "Processing a sequence property path that has only one child."
              << std::endl;
    return seedFromPropertyPath(left, path, right);
  }
  std::vector<ParsedQuery::GraphPattern> childPlans;
  childPlans.reserve(path._children.size());

  std::vector<std::string> tmpVarNames;
  for (size_t i = 0; i < path._children.size(); i++) {
    if (i == 0) {
      tmpVarNames.push_back(generateRandomVarName());
      childPlans.push_back(
          seedFromPropertyPath(left, path._children[i], tmpVarNames.back()));
    } else if (i + 1 == path._children.size()) {
      childPlans.push_back(
          seedFromPropertyPath(tmpVarNames.back(), path._children[i], right));
    } else {
      tmpVarNames.push_back(generateRandomVarName());
      childPlans.push_back(
          seedFromPropertyPath(tmpVarNames[tmpVarNames.size() - 2],
                               path._children[i], tmpVarNames.back()));
    }
  }

  // Merge all the child plans into one graph pattern
  ParsedQuery::GraphPattern p;
  for (ParsedQuery::GraphPattern& child : childPlans) {
    p._children.insert(p._children.end(), child._children.begin(),
                       child._children.end());
    // Ensure the child does not try to delete the pointers
    child._children.clear();
    p._filters.insert(p._filters.end(), child._filters.begin(),
                      child._filters.end());
    p._whereClauseTriples.insert(p._whereClauseTriples.begin(),
                                 child._whereClauseTriples.begin(),
                                 child._whereClauseTriples.end());
  }
  return p;
}

// _____________________________________________________________________________
ParsedQuery::GraphPattern QueryPlanner::seedFromAlternative(
    const std::string& left, const PropertyPath& path,
    const std::string& right) {
  using GraphPattern = ParsedQuery::GraphPattern;
  if (path._children.size() == 0) {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
             "Tried processing an alternative property path node without any "
             "children.");
  } else if (path._children.size() == 1) {
    LOG(WARN)
        << "Processing an alternative property path that has only one child."
        << std::endl;
    return seedFromPropertyPath(left, path, right);
  }

  std::vector<GraphPattern> childPlans;
  childPlans.reserve(path._children.size());
  for (size_t i = 0; i < path._children.size(); i++) {
    childPlans.push_back(seedFromPropertyPath(left, path._children[i], right));
  }

  // Build a tree of union operations
  GraphPattern p;
  p._children.push_back(new ParsedQuery::GraphPatternOperation(
      ParsedQuery::GraphPatternOperation::Type::UNION,
      {new GraphPattern(std::move(childPlans[0])), new GraphPattern(std::move(childPlans[1]))}));
  for (size_t i = 2; i < childPlans.size(); i++) {
    GraphPattern next;
    next._children.push_back(new ParsedQuery::GraphPatternOperation(
        ParsedQuery::GraphPatternOperation::Type::UNION,
        {new GraphPattern(std::move(p)), new GraphPattern(std::move(childPlans[i]))}));
    p = next;
  }
  return p;
}

// _____________________________________________________________________________
ParsedQuery::GraphPattern QueryPlanner::seedFromTransitive(
    const std::string& left, const PropertyPath& path,
    const std::string& right) {
  ParsedQuery::GraphPattern childPlan = seedFromPropertyPath(left, path._children[0], right);
  ParsedQuery::GraphPattern p;
  ParsedQuery::GraphPatternOperation* op = new ParsedQuery::GraphPatternOperation(ParsedQuery::GraphPatternOperation::Type::TRANS_PATH);
  op->_pathData._left = left;
  op->_pathData._right = right;
  op->_pathData._childGraphPattern = new ParsedQuery::GraphPattern(std::move(childPlan));
  p._children.push_back(op);
  return p;
}

// _____________________________________________________________________________
ParsedQuery::GraphPattern QueryPlanner::seedFromTransitiveMin(
    const std::string& left, const PropertyPath& path,
    const std::string& right) {
  ParsedQuery::GraphPattern childPlan = seedFromPropertyPath(left, path._children[0], right);
  ParsedQuery::GraphPattern p;
  ParsedQuery::GraphPatternOperation* op = new ParsedQuery::GraphPatternOperation(ParsedQuery::GraphPatternOperation::Type::TRANS_PATH);
  op->_pathData._left = left;
  op->_pathData._right = right;
  op->_pathData._min = path._limit;
  op->_pathData._childGraphPattern = new ParsedQuery::GraphPattern(std::move(childPlan));
  p._children.push_back(op);
  return p;
}

// _____________________________________________________________________________
ParsedQuery::GraphPattern QueryPlanner::seedFromTransitiveMax(
    const std::string& left, const PropertyPath& path,
    const std::string& right) {
  ParsedQuery::GraphPattern childPlan = seedFromPropertyPath(left, path._children[0], right);
  ParsedQuery::GraphPattern p;
  ParsedQuery::GraphPatternOperation* op = new ParsedQuery::GraphPatternOperation(ParsedQuery::GraphPatternOperation::Type::TRANS_PATH);
  op->_pathData._left = left;
  op->_pathData._right = right;
  op->_pathData._max = path._limit;
  op->_pathData._childGraphPattern = new ParsedQuery::GraphPattern(std::move(childPlan));
  p._children.push_back(op);
  return p;
}

// _____________________________________________________________________________
ParsedQuery::GraphPattern QueryPlanner::seedFromInverse(
    const std::string& left, const PropertyPath& path,
    const std::string& right) {
  return seedFromPropertyPath(right, path, left);
}

// _____________________________________________________________________________
ParsedQuery::GraphPattern QueryPlanner::seedFromIri(const std::string& left,
                                                    const PropertyPath& path,
                                                    const std::string& right) {
  ParsedQuery::GraphPattern p;
  p._whereClauseTriples.push_back(SparqlTriple(left, path, right));
  return p;
}

// _____________________________________________________________________________
std::string QueryPlanner::generateRandomVarName() {
  constexpr int VAR_NAME_LENGTH = 32;
  std::string name(VAR_NAME_LENGTH, ' ');
  // pick random characters in the range [65-90] and [97-122]

  // How many random characters we can extract from a single call to rand
  int rand_bytes = (unsigned int)(std::log(RAND_MAX) / std::log(52));
  int current_rand;
  int bytes_left = 0;
  for (int i = 0; i < VAR_NAME_LENGTH; i++) {
    if (bytes_left <= 0) {
      current_rand = rand_r(&_randomSeed);
      bytes_left = rand_bytes;
    }
    uint8_t v = current_rand % 52;
    if (v >= 26) {
      // Characters a-z
      v += 'a' - 26;
    } else {
      // Characters A-Z
      v += 'A';
    }
    name[i] = v;
    current_rand /= 52;
  }
  _randVarCount++;
  return "?" + name + std::to_string(_randVarCount);
}

// _____________________________________________________________________________
QueryPlanner::SubtreePlan QueryPlanner::getTextLeafPlan(
    const QueryPlanner::TripleGraph::Node& node) const {
  SubtreePlan plan(_qec);
  plan._idsOfIncludedNodes |= (size_t(1) << node._id);
  auto& tree = *plan._qet.get();
  AD_CHECK(node._wordPart.size() > 0);
  std::shared_ptr<Operation> textOp(new TextOperationWithoutFilter(
      _qec, node._wordPart, node._variables, node._cvar));
  tree.setOperation(QueryExecutionTree::OperationType::TEXT_WITHOUT_FILTER,
                    textOp);
  tree.setVariableColumns(textOp->getVariableColumns());
  tree.addContextVar(node._cvar);
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
  for (size_t i = 0; i < a.size(); ++i) {
    for (size_t j = 0; j < b.size(); ++j) {
      if (connected(a[i], b[j], tg)) {
        // Find join variable(s) / columns.
        auto jcs = getJoinColumns(a[i], b[j]);
        if (jcs.size() == 2 &&
            (a[i]._qet.get()->getType() ==
                 QueryExecutionTree::OperationType::TEXT_WITHOUT_FILTER ||
             b[j]._qet.get()->getType() ==
                 QueryExecutionTree::OperationType::TEXT_WITHOUT_FILTER)) {
          LOG(WARN) << "Not considering possible join on "
                    << "two columns, if they involve text operations.\n";
          continue;
        }

        if (a[i]._isOptional || b[j]._isOptional) {
          // Join the two optional columns using an optional join
          SubtreePlan plan = optionalJoin(a[i], b[j]);
          plan._idsOfIncludedNodes = a[i]._idsOfIncludedNodes;
          plan.addAllNodes(b[j]._idsOfIncludedNodes);
          plan._idsOfIncludedFilters = a[i]._idsOfIncludedFilters;
          plan._idsOfIncludedFilters |= b[j]._idsOfIncludedFilters;
          candidates[getPruningKey(plan, plan._qet->resultSortedOn())]
              .emplace_back(plan);
          continue;
        }

        if (jcs.size() == 2) {
          // SPECIAL CASE: Cyclic queries -> join on exactly two columns

          // Forbid a join between two dummies.
          if ((a[i]._qet.get()->getType() == QueryExecutionTree::SCAN &&
               a[i]._qet.get()->getRootOperation()->getResultWidth() == 3) &&
              (b[j]._qet.get()->getType() == QueryExecutionTree::SCAN &&
               b[j]._qet.get()->getRootOperation()->getResultWidth() == 3)) {
            continue;
          }

          // Check if a sub-result has to be re-sorted
          // Consider both ways to order join columns as primary / secondary
          // Make four iterations instead of two so that first and second
          // column can be swapped and more trees are constructed (of which
          // the optimal solution will be picked).
          // The order plays a role for the efficient implementation for
          // filtering directly with a scan's result.
          for (size_t n = 0; n < 4; ++n) {
            size_t c = n / 2;
            size_t swap = n % 2;
            std::shared_ptr<QueryExecutionTree> left(
                new QueryExecutionTree(_qec));
            std::shared_ptr<QueryExecutionTree> right(
                new QueryExecutionTree(_qec));
            const vector<size_t>& aSortedOn = a[i]._qet.get()->resultSortedOn();
            if (aSortedOn.size() > 0 &&
                aSortedOn[0] == jcs[c][(0 + swap) % 2] &&
                (a[i]._qet.get()->getResultWidth() == 2 ||
                 a[i]._qet.get()->getType() == QueryExecutionTree::SCAN)) {
              left = a[i]._qet;
            } else {
              // Create an order by operation.
              vector<pair<size_t, bool>> sortIndices;
              sortIndices.emplace_back(
                  std::make_pair(jcs[c][(0 + swap) % 2], false));
              sortIndices.emplace_back(
                  std::make_pair(jcs[(c + 1) % 2][(0 + swap) % 2], false));
              std::shared_ptr<Operation> orderBy(
                  new OrderBy(_qec, a[i]._qet, sortIndices));
              left->setVariableColumns(a[i]._qet->getVariableColumns());
              left->setOperation(QueryExecutionTree::ORDER_BY, orderBy);
            }
            const vector<size_t>& bSortedOn = b[j]._qet.get()->resultSortedOn();
            if (bSortedOn.size() > 0 &&
                bSortedOn[0] == jcs[c][(1 + swap) % 2] &&
                b[j]._qet.get()->getResultWidth() == 2) {
              right = b[j]._qet;
            } else {
              // Create a sort operation.
              // Create an order by operation.
              vector<pair<size_t, bool>> sortIndices;
              sortIndices.emplace_back(
                  std::make_pair(jcs[c][(1 + swap) % 2], false));
              sortIndices.emplace_back(
                  std::make_pair(jcs[(c + 1) % 2][(1 + swap) % 2], false));
              std::shared_ptr<Operation> orderBy(
                  new OrderBy(_qec, b[j]._qet, sortIndices));
              right->setVariableColumns(b[j]._qet->getVariableColumns());
              right->setOperation(QueryExecutionTree::ORDER_BY, orderBy);
            }
            // TODO(florian): consider replacing this with a multicolumn join.
            // Create the join operation.
            SubtreePlan plan(_qec);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation> join(
                new TwoColumnJoin(_qec, left, right, jcs));
            tree.setVariableColumns(
                static_cast<TwoColumnJoin*>(join.get())->getVariableColumns());
            tree.setOperation(QueryExecutionTree::TWO_COL_JOIN, join);
            plan._idsOfIncludedFilters = a[i]._idsOfIncludedFilters;
            plan._idsOfIncludedNodes = a[i]._idsOfIncludedNodes;
            plan.addAllNodes(b[j]._idsOfIncludedNodes);
            candidates[getPruningKey(plan, {jcs[c][(0 + swap) % 2]})]
                .emplace_back(plan);
          }
          continue;
        } else if (jcs.size() > 2) {
          // this can happen when e.g. subqueries are used
          SubtreePlan plan = multiColumnJoin(a[i], b[j]);
          plan._idsOfIncludedNodes = a[i]._idsOfIncludedNodes;
          plan.addAllNodes(b[j]._idsOfIncludedNodes);
          plan._idsOfIncludedFilters = a[i]._idsOfIncludedFilters;
          plan._idsOfIncludedFilters |= b[j]._idsOfIncludedFilters;
          candidates[getPruningKey(plan, plan._qet->resultSortedOn())]
              .emplace_back(plan);
          continue;
        }

        // CASE: JOIN ON ONE COLUMN ONLY.
        if ((a[i]._qet.get()->getType() ==
                 QueryExecutionTree::OperationType::TEXT_WITHOUT_FILTER ||
             b[j]._qet.get()->getType() ==
                 QueryExecutionTree::OperationType::TEXT_WITHOUT_FILTER)) {
          // If one of the join results is a text operation without filter
          // also consider using the other one as filter and thus
          // turning this join into a text operation with filter, instead,
          bool aTextOp = true;
          // If both are TextOps, the smaller one will be used as filter.
          if (a[i]._qet.get()->getType() !=
                  QueryExecutionTree::OperationType::TEXT_WITHOUT_FILTER ||
              (b[j]._qet.get()->getType() ==
                   QueryExecutionTree::OperationType::TEXT_WITHOUT_FILTER &&
               b[j]._qet.get()->getSizeEstimate() >
                   a[i]._qet.get()->getSizeEstimate())) {
            aTextOp = false;
          }
          const SubtreePlan& textPlan = aTextOp ? a[i] : b[j];
          const SubtreePlan& filterPlan = aTextOp ? b[j] : a[i];
          size_t otherPlanJc = aTextOp ? jcs[0][1] : jcs[0][0];
          SubtreePlan plan(_qec);
          plan._idsOfIncludedNodes = filterPlan._idsOfIncludedNodes;
          plan._idsOfIncludedNodes |= textPlan._idsOfIncludedNodes;
          plan._idsOfIncludedFilters = filterPlan._idsOfIncludedFilters;
          auto& tree = *plan._qet.get();
          const TextOperationWithoutFilter& noFilter =
              *static_cast<const TextOperationWithoutFilter*>(
                  textPlan._qet->getRootOperation().get());
          std::shared_ptr<Operation> textOp(new TextOperationWithFilter(
              _qec, noFilter.getWordPart(), noFilter.getVars(),
              noFilter.getCVar(), filterPlan._qet, otherPlanJc));
          tree.setOperation(QueryExecutionTree::OperationType::TEXT_WITH_FILTER,
                            textOp);
          tree.setVariableColumns(textOp->getVariableColumns());
          tree.setContextVars(filterPlan._qet.get()->getContextVars());
          tree.addContextVar(noFilter.getCVar());
          candidates[getPruningKey(plan, {jcs[0][0]})].emplace_back(plan);
        }
        // Skip if we have two dummies
        if (a[i]._qet.get()->getType() ==
                QueryExecutionTree::OperationType::SCAN &&
            a[i]._qet->getResultWidth() == 3 &&
            b[j]._qet.get()->getType() ==
                QueryExecutionTree::OperationType::SCAN &&
            b[j]._qet->getResultWidth() == 3) {
          continue;
        }

        // Check if one of the two operations is a HAS_RELATION_SCAN.
        // If the join column corresponds to the has-predicate scan's
        // subject column we can use a specialized join that avoids
        // loading the full has-predicate predicate.
        if (a[i]._qet.get()->getType() ==
                QueryExecutionTree::OperationType::HAS_RELATION_SCAN ||
            b[j]._qet.get()->getType() ==
                QueryExecutionTree::OperationType::HAS_RELATION_SCAN) {
          bool replaceJoin = false;

          std::shared_ptr<QueryExecutionTree> hasPredicateScan =
              std::make_shared<QueryExecutionTree>(_qec);
          std::shared_ptr<QueryExecutionTree> other =
              std::make_shared<QueryExecutionTree>(_qec);
          if (a[i]._qet.get()->getType() ==
                  QueryExecutionTree::OperationType::HAS_RELATION_SCAN &&
              jcs[0][0] == 0) {
            const HasPredicateScan* op = static_cast<HasPredicateScan*>(
                a[i]._qet->getRootOperation().get());
            if (op->getType() == HasPredicateScan::ScanType::FULL_SCAN) {
              hasPredicateScan = a[i]._qet;
              other = b[j]._qet;
              replaceJoin = true;
            }
          } else if (b[j]._qet.get()->getType() ==
                         QueryExecutionTree::OperationType::HAS_RELATION_SCAN &&
                     jcs[0][1] == 0) {
            const HasPredicateScan* op = static_cast<HasPredicateScan*>(
                b[j]._qet->getRootOperation().get());
            if (op->getType() == HasPredicateScan::ScanType::FULL_SCAN) {
              other = a[i]._qet;
              hasPredicateScan = b[j]._qet;
              replaceJoin = true;
            }
          }
          if (replaceJoin) {
            SubtreePlan plan(_qec);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation> scan =
                std::make_shared<HasPredicateScan>(
                    _qec, HasPredicateScan::ScanType::SUBQUERY_S);
            static_cast<HasPredicateScan*>(scan.get())->setSubtree(other);
            static_cast<HasPredicateScan*>(scan.get())
                ->setSubtreeSubjectColumn(jcs[0][1]);
            static_cast<HasPredicateScan*>(scan.get())
                ->setObject(static_cast<HasPredicateScan*>(
                                hasPredicateScan->getRootOperation().get())
                                ->getObject());
            tree.setVariableColumns(static_cast<HasPredicateScan*>(scan.get())
                                        ->getVariableColumns());
            tree.setOperation(QueryExecutionTree::HAS_RELATION_SCAN, scan);
            plan._idsOfIncludedNodes = a[i]._idsOfIncludedNodes;
            plan.addAllNodes(b[j]._idsOfIncludedNodes);
            plan._idsOfIncludedFilters = a[i]._idsOfIncludedFilters;
            plan._idsOfIncludedFilters |= b[j]._idsOfIncludedFilters;
            candidates[getPruningKey(plan,
                                     static_cast<HasPredicateScan*>(scan.get())
                                         ->resultSortedOn())]
                .emplace_back(plan);
            continue;
          }
        }

        // "NORMAL" CASE:
        // Check if a sub-result has to be re-sorted
        // TODO: replace with HashJoin maybe (or add variant to possible
        // plans).
        std::shared_ptr<QueryExecutionTree> left(new QueryExecutionTree(_qec));
        std::shared_ptr<QueryExecutionTree> right(new QueryExecutionTree(_qec));
        const vector<size_t>& aSortedOn = a[i]._qet.get()->resultSortedOn();
        if (aSortedOn.size() > 0 && aSortedOn[0] == jcs[0][0]) {
          left = a[i]._qet;
        } else {
          // Create a sort operation.
          // But never sort scans, there we could have just scanned
          // differently.
          if (a[i]._qet.get()->getType() == QueryExecutionTree::SCAN) {
            continue;
          }
          std::shared_ptr<Operation> sort(new Sort(_qec, a[i]._qet, jcs[0][0]));
          left.get()->setVariableColumns(a[i]._qet.get()->getVariableColumns());
          left.get()->setContextVars(a[i]._qet.get()->getContextVars());
          left.get()->setOperation(QueryExecutionTree::SORT, sort);
        }
        const vector<size_t>& bSortedOn = b[j]._qet.get()->resultSortedOn();
        if (bSortedOn.size() > 0 && bSortedOn[0] == jcs[0][1]) {
          right = b[j]._qet;
        } else {
          // Create a sort operation.
          // But never sort scans, there we could have just scanned
          // differently.
          if (b[j]._qet.get()->getType() == QueryExecutionTree::SCAN) {
            continue;
          }
          std::shared_ptr<Operation> sort(new Sort(_qec, b[j]._qet, jcs[0][1]));
          right.get()->setVariableColumns(
              b[j]._qet.get()->getVariableColumns());
          right.get()->setContextVars(b[j]._qet.get()->getContextVars());
          right.get()->setOperation(QueryExecutionTree::SORT, sort);
        }

        // TODO(florian): consider replacing this with a multicolumn join.
        // Create the join operation.
        SubtreePlan plan(_qec);
        auto& tree = *plan._qet.get();
        std::shared_ptr<Operation> join(
            new Join(_qec, left, right, jcs[0][0], jcs[0][1]));
        tree.setVariableColumns(
            static_cast<Join*>(join.get())->getVariableColumns());
        tree.setContextVars(static_cast<Join*>(join.get())->getContextVars());
        tree.setOperation(QueryExecutionTree::JOIN, join);
        plan._idsOfIncludedNodes = a[i]._idsOfIncludedNodes;
        plan.addAllNodes(b[j]._idsOfIncludedNodes);
        plan._idsOfIncludedFilters = a[i]._idsOfIncludedFilters;
        plan._idsOfIncludedFilters |= b[j]._idsOfIncludedFilters;
        candidates[getPruningKey(plan, {jcs[0][0]})].emplace_back(plan);
      }
    }
  }

  // Duplicates are removed if the same triples are touched,
  // the ordering is the same. Only the best is kept then.

  // Therefore we mapped plans and use contained triples + ordering var
  // as key.
  LOG(TRACE) << "Pruning...\n";
  vector<SubtreePlan> prunedPlans;
  size_t nofCandidates = 0;
  for (auto it = candidates.begin(); it != candidates.end(); ++it) {
    size_t minCost = std::numeric_limits<size_t>::max();
    size_t minIndex = 0;
    for (size_t i = 0; i < it->second.size(); ++i) {
      ++nofCandidates;
      if (it->second[i].getCostEstimate() < minCost) {
        minCost = it->second[i].getCostEstimate();
        minIndex = i;
      }
    }
    prunedPlans.push_back(it->second[minIndex]);
  }
  LOG(TRACE) << "Got " << prunedPlans.size() << " pruned plans from "
             << nofCandidates << " candidates.\n";
  return prunedPlans;
}

// _____________________________________________________________________________
QueryPlanner::SubtreePlan QueryPlanner::optionalJoin(
    const SubtreePlan& a, const SubtreePlan& b) const {
  SubtreePlan plan(_qec);

  std::vector<std::array<Id, 2>> jcs = getJoinColumns(a, b);

  const vector<size_t>& aSortedOn = a._qet.get()->resultSortedOn();
  const vector<size_t>& bSortedOn = b._qet.get()->resultSortedOn();

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
  for (array<Id, 2>& jc : jcs) {
    sortIndicesA.push_back(std::make_pair(jc[0], false));
    sortIndicesB.push_back(std::make_pair(jc[1], false));
  }

  SubtreePlan orderByPlanA(_qec), orderByPlanB(_qec);
  std::shared_ptr<Operation> orderByA(new OrderBy(_qec, a._qet, sortIndicesA));
  std::shared_ptr<Operation> orderByB(new OrderBy(_qec, b._qet, sortIndicesB));

  if (!aSorted) {
    orderByPlanA._qet->setVariableColumns(a._qet->getVariableColumns());
    orderByPlanA._qet->setOperation(QueryExecutionTree::ORDER_BY, orderByA);
  }
  if (!bSorted) {
    orderByPlanB._qet->setVariableColumns(b._qet->getVariableColumns());
    orderByPlanB._qet->setOperation(QueryExecutionTree::ORDER_BY, orderByB);
  }

  std::shared_ptr<Operation> join(new OptionalJoin(
      _qec, aSorted ? a._qet : orderByPlanA._qet, a._isOptional,
      bSorted ? b._qet : orderByPlanB._qet, b._isOptional, jcs));
  QueryExecutionTree& tree = *plan._qet.get();
  tree.setVariableColumns(
      static_cast<OptionalJoin*>(join.get())->getVariableColumns());
  tree.setOperation(QueryExecutionTree::OPTIONAL_JOIN, join);

  return plan;
}

// _____________________________________________________________________________
QueryPlanner::SubtreePlan QueryPlanner::multiColumnJoin(
    const SubtreePlan& a, const SubtreePlan& b) const {
  SubtreePlan plan(_qec);

  std::vector<std::array<Id, 2>> jcs = getJoinColumns(a, b);

  const vector<size_t>& aSortedOn = a._qet.get()->resultSortedOn();
  const vector<size_t>& bSortedOn = b._qet.get()->resultSortedOn();

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
  for (array<Id, 2>& jc : jcs) {
    sortIndicesA.push_back(std::make_pair(jc[0], false));
    sortIndicesB.push_back(std::make_pair(jc[1], false));
  }

  SubtreePlan orderByPlanA(_qec), orderByPlanB(_qec);
  std::shared_ptr<Operation> orderByA(new OrderBy(_qec, a._qet, sortIndicesA));
  std::shared_ptr<Operation> orderByB(new OrderBy(_qec, b._qet, sortIndicesB));

  if (!aSorted) {
    orderByPlanA._qet->setVariableColumns(a._qet->getVariableColumns());
    orderByPlanA._qet->setOperation(QueryExecutionTree::ORDER_BY, orderByA);
  }
  if (!bSorted) {
    orderByPlanB._qet->setVariableColumns(b._qet->getVariableColumns());
    orderByPlanB._qet->setOperation(QueryExecutionTree::ORDER_BY, orderByB);
  }

  std::shared_ptr<Operation> join(
      new MultiColumnJoin(_qec, aSorted ? a._qet : orderByPlanA._qet,
                          bSorted ? b._qet : orderByPlanB._qet, jcs));
  QueryExecutionTree& tree = *plan._qet.get();
  tree.setVariableColumns(
      static_cast<OptionalJoin*>(join.get())->getVariableColumns());
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
  return os.str();
}

// _____________________________________________________________________________
size_t QueryPlanner::SubtreePlan::getCostEstimate() const {
  return _qet.get()->getCostEstimate();
}

// _____________________________________________________________________________
size_t QueryPlanner::SubtreePlan::getSizeEstimate() const {
  return _qet.get()->getSizeEstimate();
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
vector<array<Id, 2>> QueryPlanner::getJoinColumns(
    const QueryPlanner::SubtreePlan& a,
    const QueryPlanner::SubtreePlan& b) const {
  vector<array<Id, 2>> jcs;
  const auto& aVarCols = a._qet->getVariableColumns();
  const auto& bVarCols = b._qet->getVariableColumns();
  for (const auto& aVarCol : aVarCols) {
    auto itt = bVarCols.find(aVarCol.first);
    if (itt != bVarCols.end()) {
      jcs.push_back(array<Id, 2>{{aVarCol.second, itt->second}});
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

  return os.str();
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
          (!isVariable(filters[i]._rhs) ||
           row[n]._qet.get()->varCovered(filters[i]._rhs))) {
        // Apply this filter.
        SubtreePlan newPlan(_qec);
        newPlan._idsOfIncludedFilters = row[n]._idsOfIncludedFilters;
        newPlan._idsOfIncludedFilters |= (size_t(1) << i);
        newPlan._idsOfIncludedNodes = row[n]._idsOfIncludedNodes;
        newPlan._isOptional = row[n]._isOptional;
        auto& tree = *newPlan._qet.get();
        tree.setOperation(QueryExecutionTree::FILTER,
                          createFilterOperation(filters[i], row[n]));
        tree.setVariableColumns(row[n]._qet.get()->getVariableColumns());
        tree.setContextVars(row[n]._qet.get()->getContextVars());
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
std::shared_ptr<Operation> QueryPlanner::createFilterOperation(
    const SparqlFilter& filter, const SubtreePlan& parent) const {
  std::shared_ptr<Filter> op = std::make_shared<Filter>(
      _qec, parent._qet, filter._type, filter._lhs, filter._rhs);
  if (filter._type == SparqlFilter::REGEX) {
    op->setRegexIgnoreCase(filter._regexIgnoreCase);
  }
  return op;
}

// _____________________________________________________________________________
vector<vector<QueryPlanner::SubtreePlan>> QueryPlanner::fillDpTab(
    const QueryPlanner::TripleGraph& tg, const vector<SparqlFilter>& filters,
    const vector<const QueryPlanner::SubtreePlan*>& children) {
  LOG(TRACE) << "Fill DP table... (there are "
             << tg._nodeMap.size() + children.size() << " triples to join)"
             << std::endl;
  vector<vector<SubtreePlan>> dpTab;
  dpTab.emplace_back(seedWithScansAndText(tg, children));
  applyFiltersIfPossible(dpTab.back(), filters,
                         tg._nodeMap.size() + children.size() == 1);

  for (size_t k = 2; k <= tg._nodeMap.size() + children.size(); ++k) {
    LOG(TRACE) << "Producing plans that unite " << k << " triples."
               << std::endl;
    dpTab.emplace_back(vector<SubtreePlan>());
    for (size_t i = 1; i * 2 <= k; ++i) {
      auto newPlans = merge(dpTab[i - 1], dpTab[k - i - 1], tg);
      if (newPlans.size() == 0) {
        continue;
      }
      dpTab[k - 1].insert(dpTab[k - 1].end(), newPlans.begin(), newPlans.end());
      applyFiltersIfPossible(dpTab.back(), filters,
                             tg._nodeMap.size() + children.size() == k);
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
size_t QueryPlanner::getTextLimit(const string& textLimitString) const {
  if (textLimitString.size() == 0) {
    return 1;
  } else {
    return static_cast<size_t>(atol(textLimitString.c_str()));
  }
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
      contextVarToTextNodesIds[cvar].push_back(i);
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
  for (auto it = other._nodeMap.begin(); it != other._nodeMap.end(); ++it) {
    _nodeStorage.push_back(*it->second);
    _nodeMap[it->first] = &_nodeStorage.back();
  }
}

// _____________________________________________________________________________
QueryPlanner::TripleGraph& QueryPlanner::TripleGraph::operator=(
    const TripleGraph& other) {
  _adjLists = other._adjLists;
  for (auto it = other._nodeMap.begin(); it != other._nodeMap.end(); ++it) {
    _nodeStorage.push_back(*it->second);
    _nodeMap[it->first] = &_nodeStorage.back();
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
  if (cvarsToTextNodes.size() == 0) {
    return;
  }
  // Now turn each such clique into a new node the represents that whole
  // text operation clique.
  size_t id = 0;
  vector<Node> textNodes;
  ad_utility::HashMap<size_t, size_t> removedNodeIds;
  vector<std::set<size_t>> tnAdjSetsToOldIds;
  for (auto it = cvarsToTextNodes.begin(); it != cvarsToTextNodes.end(); ++it) {
    auto& cvar = it->first;
    string wordPart;
    vector<SparqlTriple> trips;
    tnAdjSetsToOldIds.push_back(std::set<size_t>());
    auto& adjNodes = tnAdjSetsToOldIds.back();
    for (auto nid : it->second) {
      removedNodeIds[nid] = id;
      adjNodes.insert(_adjLists[nid].begin(), _adjLists[nid].end());
      auto& triple = _nodeMap[nid]->_triple;
      trips.push_back(triple);
      if (triple._s == cvar && !isVariable(triple._o)) {
        if (wordPart.size() > 0) {
          wordPart += " ";
        }
        wordPart += triple._o;
      }
      if (triple._o == cvar && !isVariable(triple._s)) {
        if (wordPart.size() > 0) {
          wordPart += " ";
        }
        wordPart += triple._s;
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
bool QueryPlanner::TripleGraph::isPureTextQuery() {
  return _nodeStorage.size() == 1 && _nodeStorage.begin()->_cvar.size() > 0;
}

// _____________________________________________________________________________
ad_utility::HashMap<string, size_t>
QueryPlanner::createVariableColumnsMapForTextOperation(
    const string& contextVar, const string& entityVar,
    const ad_utility::HashSet<string>& freeVars,
    const vector<pair<QueryExecutionTree, size_t>>& subtrees) {
  AD_CHECK(contextVar.size() > 0);
  ad_utility::HashMap<string, size_t> map;
  size_t n = 0;
  if (entityVar.size() > 0) {
    map[entityVar] = n++;
    map[string("SCORE(") + contextVar + ")"] = n++;
    map[contextVar] = n++;
  } else {
    map[contextVar] = n++;
    map[string("SCORE(") + contextVar + ")"] = n++;
  }

  for (const auto& v : freeVars) {
    map[v] = n++;
  }

  for (size_t i = 0; i < subtrees.size(); ++i) {
    size_t offset = n;
    for (auto it = subtrees[i].first.getVariableColumns().begin();
         it != subtrees[i].first.getVariableColumns().end(); ++it) {
      map[it->first] = offset + it->second;
      ++n;
    }
  }
  return map;
}
