// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./QueryPlanner.h"
#include <algorithm>
#include "../parser/ParseException.h"
#include "CountAvailablePredicates.h"
#include "Distinct.h"
#include "Filter.h"
#include "GroupBy.h"
#include "HasPredicateScan.h"
#include "IndexScan.h"
#include "Join.h"
#include "OptionalJoin.h"
#include "OrderBy.h"
#include "Sort.h"
#include "TextOperationWithFilter.h"
#include "TextOperationWithoutFilter.h"
#include "TwoColumnJoin.h"

// _____________________________________________________________________________
QueryPlanner::QueryPlanner(QueryExecutionContext* qec, bool optimizeOptionals)
    : _qec(qec), _optimizeOptionals(optimizeOptionals) {}

// _____________________________________________________________________________
QueryExecutionTree QueryPlanner::createExecutionTree(ParsedQuery& pq) const {
  // Create a topological sorting of the tree where children are in the list
  // after their parents.
  std::vector<const ParsedQuery::GraphPattern*> patternsToProcess;
  std::vector<const ParsedQuery::GraphPattern*> childrenToAdd;
  childrenToAdd.push_back(&pq._rootGraphPattern);
  while (!childrenToAdd.empty()) {
    const ParsedQuery::GraphPattern* pattern = childrenToAdd.back();
    childrenToAdd.pop_back();
    patternsToProcess.push_back(pattern);
    childrenToAdd.insert(childrenToAdd.end(), pattern->_children.begin(),
                         pattern->_children.end());
  }

  std::vector<SubtreePlan> patternPlans;
  for (size_t i = 0; i < pq._numGraphPatterns; i++) {
    // Using a loop instead of resize as there is no default constructor, and
    // distinct _qet values are needed.
    patternPlans.emplace_back(_qec);
  }
  LOG(DEBUG) << "Got " << patternPlans.size() << " subplans to create."
             << std::endl;

  // Look for ql:has-predicate to determine if the pattern trick should be used.
  // If the pattern trick is used the ql:has-predicate triple will be removed
  // from the list of where clause triples. Otherwise the ql:has-relation triple
  // will be handled using a HasRelationScan.
  SparqlTriple patternTrickTriple("", "", "");
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

  vector<SubtreePlan*> childPlans;
  while (!patternsToProcess.empty()) {
    const ParsedQuery::GraphPattern* pattern = patternsToProcess.back();
    patternsToProcess.pop_back();

    LOG(DEBUG) << "Creating execution plan.\n";
    childPlans.clear();
    if (_optimizeOptionals) {
      for (const ParsedQuery::GraphPattern* child : pattern->_children) {
        childPlans.push_back(&patternPlans[child->_id]);
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
    finalTab = fillDpTab(tg, pattern->_filters, childPlans);

    // If any form of grouping is used (e.g. the pattern trick) sorting
    // has to be done after the grouping.
    if (pattern == &pq._rootGraphPattern && pq._orderBy.size() > 0 &&
        !doGrouping) {
      // If there is an order by clause, add another row to the table and
      // just add an order by / sort to every previous result if needed.
      // If the ordering is perfect already, just copy the plan.
      finalTab.emplace_back(getOrderByRow(pq, finalTab));
    }

    vector<SubtreePlan>& lastRow = finalTab.back();
    if (!usePatternTrick) {
      // when the pattern trick is in use there is one triple that is not
      // part of the triple graph, so the lastRow can be empty
      AD_CHECK_GT(lastRow.size(), 0);
    }
    if (lastRow.size() > 0) {
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

  if (!_optimizeOptionals) {
    // join the created trees using optional joins on all of their common
    // variables
    // Create an inverse topological ordering of all nodes with children
    std::vector<const ParsedQuery::GraphPattern*> inverseTopo;
    patternsToProcess.push_back(&pq._rootGraphPattern);
    while (!patternsToProcess.empty()) {
      const ParsedQuery::GraphPattern* pattern = patternsToProcess.back();
      patternsToProcess.pop_back();
      if (pattern->_children.size() > 0) {
        // queue all children for processing
        patternsToProcess.insert(patternsToProcess.end(),
                                 pattern->_children.begin(),
                                 pattern->_children.end());
        inverseTopo.push_back(pattern);
      }
    }

    LOG(DEBUG) << inverseTopo.size() << " of the nodes have children"
               << std::endl;

    if (!inverseTopo.empty()) {
      std::vector<ParsedQuery::GraphPattern*> sortedChildren;
      for (int i = inverseTopo.size() - 1; i >= 0; i--) {
        const ParsedQuery::GraphPattern* pattern = inverseTopo[i];
        sortedChildren.clear();
        sortedChildren.insert(sortedChildren.end(), pattern->_children.begin(),
                              pattern->_children.end());
        // Init the joins by taking the parent and its first child, then
        // succesively join with the next child.
        // ensure the children are sorted in ascending order
        std::sort(sortedChildren.begin(), sortedChildren.end(),
                  [&patternPlans](const ParsedQuery::GraphPattern* p1,
                                  const ParsedQuery::GraphPattern* p2) -> bool {
                    return patternPlans[p1->_id].getSizeEstimate() <
                           patternPlans[p2->_id].getSizeEstimate();
                  });

        std::vector<SubtreePlan> plans;

        plans.push_back(optionalJoin(patternPlans[pattern->_id],
                                     patternPlans[sortedChildren[0]->_id]));

        for (size_t j = 1; j < sortedChildren.size(); j++) {
          SubtreePlan& plan1 = plans.back();
          SubtreePlan& plan2 = patternPlans[sortedChildren[j]->_id];
          plans.push_back(optionalJoin(plan1, plan2));
        }
        // Replace the old pattern with the new one that merges all children.
        patternPlans[pattern->_id] = plans.back();
      }
    }
  }

  SubtreePlan final = patternPlans[0];

  if (usePatternTrick) {
    if (final._qet->getRootOperation() != nullptr) {
      // Determine the column containing the subjects for which we are
      // interested in their predicates.
      auto it =
          final._qet.get()->getVariableColumnMap().find(patternTrickTriple._s);
      if (it == final._qet.get()->getVariableColumnMap().end()) {
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
      bool isSorted =
          final._qet->getRootOperation()->resultSortedOn() == subjectColumn;
      // a and b need to be ordered properly first
      vector<pair<size_t, bool>> sortIndices = {
          std::make_pair(subjectColumn, false)};

      SubtreePlan orderByPlan(_qec);
      std::shared_ptr<Operation> orderByOp(
          new OrderBy(_qec, final._qet, sortIndices));

      if (!isSorted) {
        orderByPlan._qet->setVariableColumns(
            final._qet->getVariableColumnMap());
        orderByPlan._qet->setOperation(QueryExecutionTree::ORDER_BY, orderByOp);
      }
      SubtreePlan patternTrickPlan(_qec);
      std::shared_ptr<Operation> countPred(new CountAvailablePredicates(
          _qec, isSorted ? final._qet : orderByPlan._qet, subjectColumn));

      static_cast<CountAvailablePredicates*>(countPred.get())
          ->setVarNames(patternTrickTriple._o, pq._aliases[0]._outVarName);
      QueryExecutionTree& tree = *patternTrickPlan._qet.get();
      tree.setVariableColumns(
          static_cast<CountAvailablePredicates*>(countPred.get())
              ->getVariableColumns());
      tree.setOperation(QueryExecutionTree::COUNT_AVAILABLE_PREDICATES,
                        countPred);

      final = patternTrickPlan;
      LOG(DEBUG) << "Plan after pattern trick: " << endl
                 << final._qet->asString() << endl;
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

      final = patternTrickPlan;
    }
  } else if (doGrouping) {
    // Create a group by operation to determine on which columns the input
    // needs to be sorted
    SubtreePlan groupByPlan(_qec);
    std::shared_ptr<Operation> groupBy =
        std::make_shared<GroupBy>(_qec, pq._groupByVariables, pq._aliases);
    QueryExecutionTree& groupByTree = *groupByPlan._qet.get();

    // Then compute the sort columns
    std::vector<std::pair<size_t, bool>> sortColumns =
        static_cast<GroupBy*>(groupBy.get())->computeSortColumns(final._qet);

    if (!sortColumns.empty() &&
        !(sortColumns.size() == 1 &&
          final._qet->resultSortedOn() == sortColumns[0].first)) {
      // Create an order by operation as required by the group by
      std::shared_ptr<Operation> orderBy =
          std::make_shared<OrderBy>(_qec, final._qet, sortColumns);
      SubtreePlan orderByPlan(_qec);
      QueryExecutionTree& orderByTree = *orderByPlan._qet.get();
      orderByTree.setVariableColumns(final._qet->getVariableColumnMap());
      orderByTree.setOperation(QueryExecutionTree::ORDER_BY, orderBy);
      final = orderByPlan;
    }

    static_cast<GroupBy*>(groupBy.get())->setSubtree(final._qet);
    groupByTree.setVariableColumns(
        static_cast<GroupBy*>(groupBy.get())->getVariableColumns());
    groupByTree.setOperation(QueryExecutionTree::GROUP_BY, groupBy);
    final = groupByPlan;
  }

  if (doGrouping) {
    // Add the order by operation
    if (pq._orderBy.size() > 0) {
      SubtreePlan plan(_qec);
      auto& tree = *plan._qet.get();
      vector<pair<size_t, bool>> sortIndices;
      for (auto& ord : pq._orderBy) {
        sortIndices.emplace_back(pair<size_t, bool>{
            final._qet.get()->getVariableColumn(ord._key), ord._desc});
      }
      std::shared_ptr<Operation> ob(new OrderBy(_qec, final._qet, sortIndices));
      tree.setVariableColumns(final._qet.get()->getVariableColumnMap());
      tree.setOperation(QueryExecutionTree::ORDER_BY, ob);
      tree.setContextVars(final._qet.get()->getContextVars());
      final = plan;
    }
  }

  // A distinct modifier is applied in the end. This is very easy
  // but not necessarily optimal.
  // TODO: Adjust so that the optimal place for the operation is found.
  if (pq._distinct) {
    QueryExecutionTree distinctTree(*final._qet.get());
    vector<size_t> keepIndices;
    ad_utility::HashSet<size_t> indDone;
    for (const auto& var : pq._selectedVariables) {
      if (final._qet.get()->getVariableColumnMap().find(var) !=
          final._qet.get()->getVariableColumnMap().end()) {
        auto ind = final._qet.get()->getVariableColumnMap().find(var)->second;
        if (indDone.count(ind) == 0) {
          keepIndices.push_back(ind);
          indDone.insert(ind);
        }
      } else if (ad_utility::startsWith(var, "SCORE(") ||
                 ad_utility::startsWith(var, "TEXT(")) {
        auto varInd = var.find('?');
        auto cVar = var.substr(varInd, var.rfind(')') - varInd);
        if (final._qet.get()->getVariableColumnMap().find(cVar) !=
            final._qet.get()->getVariableColumnMap().end()) {
          auto ind =
              final._qet.get()->getVariableColumnMap().find(cVar)->second;
          if (indDone.count(ind) == 0) {
            keepIndices.push_back(ind);
            indDone.insert(ind);
          }
        }
      }
    }
    if (final._qet.get()->getType() == QueryExecutionTree::SORT ||
        final._qet.get()->getType() == QueryExecutionTree::ORDER_BY ||
        std::find(keepIndices.begin(), keepIndices.end(),
                  final._qet.get()->resultSortedOn()) != keepIndices.end()) {
      std::shared_ptr<Operation> distinct(
          new Distinct(_qec, final._qet, keepIndices));
      distinctTree.setOperation(QueryExecutionTree::DISTINCT, distinct);
    } else {
      if (keepIndices.size() == 1) {
        std::shared_ptr<QueryExecutionTree> tree(new QueryExecutionTree(_qec));
        std::shared_ptr<Operation> sort(
            new Sort(_qec, final._qet, keepIndices[0]));
        tree->setVariableColumns(final._qet.get()->getVariableColumnMap());
        tree->setOperation(QueryExecutionTree::SORT, sort);
        tree->setContextVars(final._qet.get()->getContextVars());
        std::shared_ptr<Operation> distinct(
            new Distinct(_qec, tree, keepIndices));
        distinctTree.setOperation(QueryExecutionTree::DISTINCT, distinct);
      } else {
        std::shared_ptr<QueryExecutionTree> tree(new QueryExecutionTree(_qec));
        vector<pair<size_t, bool>> obCols;
        for (auto& i : keepIndices) {
          obCols.emplace_back(std::make_pair(i, false));
        }
        std::shared_ptr<Operation> ob(new OrderBy(_qec, final._qet, obCols));
        tree->setVariableColumns(final._qet.get()->getVariableColumnMap());
        tree->setOperation(QueryExecutionTree::ORDER_BY, ob);
        tree->setContextVars(final._qet.get()->getContextVars());
        std::shared_ptr<Operation> distinct(
            new Distinct(_qec, tree, keepIndices));
        distinctTree.setOperation(QueryExecutionTree::DISTINCT, distinct);
      }
    }
    distinctTree.setTextLimit(getTextLimit(pq._textLimit));
    return distinctTree;
  }

  final._qet.get()->setTextLimit(getTextLimit(pq._textLimit));
  LOG(DEBUG) << "Done creating execution plan.\n";
  return *final._qet.get();
}

bool QueryPlanner::checkUsePatternTrick(
    ParsedQuery* pq, SparqlTriple* patternTrickTriple) const {
  bool usePatternTrick = false;
  // Check if the query has the right number of variables for aliases and group
  // by.
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
        // Also check that the triples object matches the aliases input variable
        // and the group by variable.
        if (t._p == HAS_PREDICATE_PREDICATE && alias._inVarName == t._o &&
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
          // Check for triples containing the ql:has-predicate triple's object.
          for (size_t j = 0;
               usePatternTrick &&
               j < pq->_rootGraphPattern._whereClauseTriples.size();
               j++) {
            const SparqlTriple& other =
                pq->_rootGraphPattern._whereClauseTriples[j];
            if (j != i &&
                (other._s == t._o || other._p == t._o || other._o == t._o)) {
              usePatternTrick = false;
            }
          }
          // Don't run any more checks if we already determined that the pattern
          // trick is not going to be used.
          if (usePatternTrick) {
            // Check for filters on the ql:has-predicate triple's subject or
            // object
            for (const SparqlFilter& filter : pq->_rootGraphPattern._filters) {
              if (filter._lhs == t._o || filter._lhs == t._s ||
                  filter._rhs == t._o || filter._rhs == t._s) {
                usePatternTrick = false;
                break;
              }
            }
          }
          // Don't run any more checks if we already determined that the pattern
          // trick is not going to be used.
          if (usePatternTrick) {
            // Check for optional parts containing the ql:has-predicate triple's
            // object
            std::vector<const ParsedQuery::GraphPattern*> graphsToProcess;
            graphsToProcess.insert(graphsToProcess.end(),
                                   pq->_rootGraphPattern._children.begin(),
                                   pq->_rootGraphPattern._children.end());
            while (!graphsToProcess.empty()) {
              const ParsedQuery::GraphPattern* pattern = graphsToProcess.back();
              graphsToProcess.pop_back();
              graphsToProcess.insert(graphsToProcess.end(),
                                     pattern->_children.begin(),
                                     pattern->_children.end());
              for (const SparqlTriple& other : pattern->_whereClauseTriples) {
                if (other._s == t._o || other._p == t._o || other._o == t._o) {
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
          }
        }
      }
    }
  }
  return usePatternTrick;
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
      if (col == previous[i]._qet.get()->resultSortedOn()) {
        // Already sorted perfectly
        added.push_back(previous[i]);
      } else {
        std::shared_ptr<Operation> sort(new Sort(_qec, previous[i]._qet, col));
        tree.setVariableColumns(previous[i]._qet.get()->getVariableColumnMap());
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
      std::shared_ptr<Operation> ob(
          new OrderBy(_qec, previous[i]._qet, sortIndices));
      tree.setVariableColumns(previous[i]._qet.get()->getVariableColumnMap());
      tree.setOperation(QueryExecutionTree::ORDER_BY, ob);
      tree.setContextVars(previous[i]._qet.get()->getContextVars());

      added.push_back(plan);
    }
  }
  return added;
}

// _____________________________________________________________________________
void QueryPlanner::getVarTripleMap(
    const ParsedQuery& pq,
    ad_utility::HashMap<string, vector<SparqlTriple>>& varToTrip,
    ad_utility::HashSet<string>& contextVars) const {
  for (auto& t : pq._rootGraphPattern._whereClauseTriples) {
    if (isVariable(t._s)) {
      varToTrip[t._s].push_back(t);
    }
    if (isVariable(t._p)) {
      varToTrip[t._p].push_back(t);
    }
    if (isVariable(t._o)) {
      varToTrip[t._o].push_back(t);
    }
    // TODO: Could use more refactoring.
    // In Earlier versions there were no ql:contains... predicates but
    // a symmetric <in-text> predicate. Therefore some parts are still more
    // complex than need be.
    if (t._p == CONTAINS_WORD_PREDICATE || t._p == CONTAINS_ENTITY_PREDICATE) {
      contextVars.insert(t._s);
    }
  }
}

// _____________________________________________________________________________
bool QueryPlanner::isVariable(const string& elem) {
  return ad_utility::startsWith(elem, "?");
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
    const vector<QueryPlanner::SubtreePlan*>& children) const {
  vector<SubtreePlan> seeds;
  // add all child plans as seeds
  uint32_t idShift = tg._nodeMap.size();
  for (SubtreePlan* plan : children) {
    SubtreePlan newIdPlan = *plan;
    // give the plan a unique id bit
    newIdPlan._idsOfIncludedNodes = 1 << idShift;
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
      } else if (node._variables.size() == 1) {
        // Just pick one direction, they should be equivalent.
        SubtreePlan plan(_qec);
        plan._idsOfIncludedNodes |= (1 << i);
        auto& tree = *plan._qet.get();
        if (node._triple._p == HAS_PREDICATE_PREDICATE) {
          // Add a has relation scan instead of a normal IndexScan
          if (isVariable(node._triple._s)) {
            std::shared_ptr<Operation> scan =
                std::make_shared<HasPredicateScan>(
                    _qec, HasPredicateScan::ScanType::FREE_S);
            static_cast<HasPredicateScan*>(scan.get())
                ->setSubject(node._triple._s);
            static_cast<HasPredicateScan*>(scan.get())
                ->setObject(node._triple._o);
            tree.setOperation(
                QueryExecutionTree::OperationType::HAS_RELATION_SCAN, scan);
            tree.setVariableColumns(static_cast<HasPredicateScan*>(scan.get())
                                        ->getVariableColumns());
          } else if (isVariable(node._triple._o)) {
            std::shared_ptr<Operation> scan =
                std::make_shared<HasPredicateScan>(
                    _qec, HasPredicateScan::ScanType::FREE_O);
            static_cast<HasPredicateScan*>(scan.get())
                ->setSubject(node._triple._s);
            static_cast<HasPredicateScan*>(scan.get())
                ->setObject(node._triple._o);
            tree.setOperation(
                QueryExecutionTree::OperationType::HAS_RELATION_SCAN, scan);
            tree.setVariableColumns(static_cast<HasPredicateScan*>(scan.get())
                                        ->getVariableColumns());
          }
        } else if (isVariable(node._triple._s)) {
          std::shared_ptr<Operation> scan(
              new IndexScan(_qec, IndexScan::ScanType::POS_BOUND_O));
          static_cast<IndexScan*>(scan.get())->setPredicate(node._triple._p);
          static_cast<IndexScan*>(scan.get())->setObject(node._triple._o);
          tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
          tree.setVariableColumn(node._triple._s, 0);
        } else if (isVariable(node._triple._o)) {
          std::shared_ptr<Operation> scan(
              new IndexScan(_qec, IndexScan::ScanType::PSO_BOUND_S));
          static_cast<IndexScan*>(scan.get())->setPredicate(node._triple._p);
          static_cast<IndexScan*>(scan.get())->setSubject(node._triple._s);
          tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
          tree.setVariableColumn(node._triple._o, 0);
        } else {
          std::shared_ptr<Operation> scan(
              new IndexScan(_qec, IndexScan::ScanType::SOP_BOUND_O));
          static_cast<IndexScan*>(scan.get())->setSubject(node._triple._s);
          static_cast<IndexScan*>(scan.get())->setObject(node._triple._o);
          tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
          tree.setVariableColumn(node._triple._p, 0);
        }
        seeds.push_back(plan);
      } else if (node._variables.size() == 2) {
        // Add plans for both possible scan directions.
        if (node._triple._p == HAS_PREDICATE_PREDICATE) {
          // Add a has relation scan instead of a normal IndexScan
          SubtreePlan plan(_qec);
          plan._idsOfIncludedNodes |= (1 << i);
          auto& tree = *plan._qet.get();
          std::shared_ptr<Operation> scan(new HasPredicateScan(
              _qec, HasPredicateScan::ScanType::FULL_SCAN));
          static_cast<HasPredicateScan*>(scan.get())
              ->setSubject(node._triple._s);
          static_cast<HasPredicateScan*>(scan.get())
              ->setObject(node._triple._o);
          tree.setOperation(
              QueryExecutionTree::OperationType::HAS_RELATION_SCAN, scan);
          tree.setVariableColumns(
              static_cast<HasPredicateScan*>(scan.get())->getVariableColumns());
          seeds.push_back(plan);
        } else if (!isVariable(node._triple._p)) {
          {
            SubtreePlan plan(_qec);
            plan._idsOfIncludedNodes |= (1 << i);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation> scan(
                new IndexScan(_qec, IndexScan::ScanType::PSO_FREE_S));
            static_cast<IndexScan*>(scan.get())->setPredicate(node._triple._p);
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
            tree.setVariableColumn(node._triple._s, 0);
            tree.setVariableColumn(node._triple._o, 1);
            seeds.push_back(plan);
          }
          {
            SubtreePlan plan(_qec);
            plan._idsOfIncludedNodes |= (1 << i);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation> scan(
                new IndexScan(_qec, IndexScan::ScanType::POS_FREE_O));
            static_cast<IndexScan*>(scan.get())->setPredicate(node._triple._p);
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
            tree.setVariableColumn(node._triple._o, 0);
            tree.setVariableColumn(node._triple._s, 1);
            seeds.push_back(plan);
          }
        } else if (!isVariable(node._triple._s)) {
          {
            SubtreePlan plan(_qec);
            plan._idsOfIncludedNodes |= (1 << i);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation> scan(
                new IndexScan(_qec, IndexScan::ScanType::SPO_FREE_P));
            static_cast<IndexScan*>(scan.get())->setSubject(node._triple._s);
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
            tree.setVariableColumn(node._triple._p, 0);
            tree.setVariableColumn(node._triple._o, 1);
            seeds.push_back(plan);
          }
          {
            SubtreePlan plan(_qec);
            plan._idsOfIncludedNodes |= (1 << i);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation> scan(
                new IndexScan(_qec, IndexScan::ScanType::SOP_FREE_O));
            static_cast<IndexScan*>(scan.get())->setSubject(node._triple._s);
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
            tree.setVariableColumn(node._triple._o, 0);
            tree.setVariableColumn(node._triple._p, 1);
            seeds.push_back(plan);
          }
        } else if (!isVariable(node._triple._o)) {
          {
            SubtreePlan plan(_qec);
            plan._idsOfIncludedNodes |= (1 << i);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation> scan(
                new IndexScan(_qec, IndexScan::ScanType::OSP_FREE_S));
            static_cast<IndexScan*>(scan.get())->setObject(node._triple._o);
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
            tree.setVariableColumn(node._triple._s, 0);
            tree.setVariableColumn(node._triple._p, 1);
            seeds.push_back(plan);
          }
          {
            SubtreePlan plan(_qec);
            plan._idsOfIncludedNodes |= (1 << i);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation> scan(
                new IndexScan(_qec, IndexScan::ScanType::OPS_FREE_P));
            static_cast<IndexScan*>(scan.get())->setObject(node._triple._o);
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
            tree.setVariableColumn(node._triple._p, 0);
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
            plan._idsOfIncludedNodes |= (1 << i);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation> scan(
                new IndexScan(_qec, IndexScan::ScanType::FULL_INDEX_SCAN_SPO));
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
            tree.setVariableColumn(node._triple._s, 0);
            tree.setVariableColumn(node._triple._p, 1);
            tree.setVariableColumn(node._triple._o, 2);
            seeds.push_back(plan);
          }
          // SOP
          {
            SubtreePlan plan(_qec);
            plan._idsOfIncludedNodes |= (1 << i);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation> scan(
                new IndexScan(_qec, IndexScan::ScanType::FULL_INDEX_SCAN_SOP));
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
            tree.setVariableColumn(node._triple._s, 0);
            tree.setVariableColumn(node._triple._o, 1);
            tree.setVariableColumn(node._triple._p, 2);
            seeds.push_back(plan);
          }
          // PSO
          {
            SubtreePlan plan(_qec);
            plan._idsOfIncludedNodes |= (1 << i);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation> scan(
                new IndexScan(_qec, IndexScan::ScanType::FULL_INDEX_SCAN_PSO));
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
            tree.setVariableColumn(node._triple._p, 0);
            tree.setVariableColumn(node._triple._s, 1);
            tree.setVariableColumn(node._triple._o, 2);
            seeds.push_back(plan);
          }
          // POS
          {
            SubtreePlan plan(_qec);
            plan._idsOfIncludedNodes |= (1 << i);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation> scan(
                new IndexScan(_qec, IndexScan::ScanType::FULL_INDEX_SCAN_POS));
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
            tree.setVariableColumn(node._triple._p, 0);
            tree.setVariableColumn(node._triple._o, 1);
            tree.setVariableColumn(node._triple._s, 2);
            seeds.push_back(plan);
          }
          // OSP
          {
            SubtreePlan plan(_qec);
            plan._idsOfIncludedNodes |= (1 << i);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation> scan(
                new IndexScan(_qec, IndexScan::ScanType::FULL_INDEX_SCAN_OSP));
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
            tree.setVariableColumn(node._triple._o, 0);
            tree.setVariableColumn(node._triple._s, 1);
            tree.setVariableColumn(node._triple._p, 2);
            seeds.push_back(plan);
          }
          // OPS
          {
            SubtreePlan plan(_qec);
            plan._idsOfIncludedNodes |= (1 << i);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation> scan(
                new IndexScan(_qec, IndexScan::ScanType::FULL_INDEX_SCAN_OPS));
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
            tree.setVariableColumn(node._triple._o, 0);
            tree.setVariableColumn(node._triple._p, 1);
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
    }
  }
  return seeds;
}

// _____________________________________________________________________________
QueryPlanner::SubtreePlan QueryPlanner::getTextLeafPlan(
    const QueryPlanner::TripleGraph::Node& node) const {
  SubtreePlan plan(_qec);
  plan._idsOfIncludedNodes |= (1 << node._id);
  auto& tree = *plan._qet.get();
  AD_CHECK(node._wordPart.size() > 0);
  // Subtract 1 for variables.size() for the context var.
  std::shared_ptr<Operation> textOp(new TextOperationWithoutFilter(
      _qec, node._wordPart, node._variables.size() - 1));
  tree.setOperation(QueryExecutionTree::OperationType::TEXT_WITHOUT_FILTER,
                    textOp);
  std::unordered_map<string, size_t> vcmap;
  size_t index = 0;
  vcmap[node._cvar] = index++;
  vcmap[string("SCORE(") + node._cvar + ")"] = index++;
  for (const auto& var : node._variables) {
    if (var != node._cvar) {
      vcmap[var] = index++;
    }
  }
  tree.setVariableColumns(vcmap);
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
        if (jcs.size() > 2 && !a[i]._isOptional && !b[j]._isOptional) {
          LOG(WARN) << "Not considering possible join on "
                    << "three or more columns at once.\n";
          continue;
        }
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
            if (a[i]._qet.get()->resultSortedOn() == jcs[c][(0 + swap) % 2] &&
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
              left->setVariableColumns(a[i]._qet->getVariableColumnMap());
              left->setOperation(QueryExecutionTree::ORDER_BY, orderBy);
            }
            if (b[j]._qet.get()->resultSortedOn() == jcs[c][(1 + swap) % 2] &&
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
              right->setVariableColumns(b[j]._qet->getVariableColumnMap());
              right->setOperation(QueryExecutionTree::ORDER_BY, orderBy);
            }
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
            candidates[getPruningKey(plan, jcs[c][(0 + swap) % 2])]
                .emplace_back(plan);
          }
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
          // Subtract 1 for variables.size() for the context var.
          const TextOperationWithoutFilter& noFilter =
              *static_cast<const TextOperationWithoutFilter*>(
                  textPlan._qet->getRootOperation().get());
          std::shared_ptr<Operation> textOp(new TextOperationWithFilter(
              _qec, noFilter.getWordPart(), noFilter.getNofVars(),
              filterPlan._qet, otherPlanJc));
          tree.setOperation(QueryExecutionTree::OperationType::TEXT_WITH_FILTER,
                            textOp);
          std::unordered_map<string, size_t> vcmap;
          // Subtract one because the entity that we filtered on
          // is provided by the filter table and still has the same place there.
          size_t colN = 2;
          string cvar = *textPlan._qet.get()->getContextVars().begin();
          for (auto it = textPlan._qet.get()->getVariableColumnMap().begin();
               it != textPlan._qet.get()->getVariableColumnMap().end(); ++it) {
            if (it->first == cvar ||
                it->first == string("SCORE(") + cvar + ")") {
              vcmap[it->first] = it->second;
            } else if (filterPlan._qet.get()->getVariableColumnMap().count(
                           it->first) == 0) {
              vcmap[it->first] = colN++;
            }
          }
          for (auto it = filterPlan._qet.get()->getVariableColumnMap().begin();
               it != filterPlan._qet.get()->getVariableColumnMap().end();
               ++it) {
            vcmap[it->first] = colN + it->second;
          }
          tree.setVariableColumns(vcmap);
          tree.setContextVars(filterPlan._qet.get()->getContextVars());
          tree.addContextVar(cvar);
          candidates[getPruningKey(plan, jcs[0][0])].emplace_back(plan);
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
        // TODO: replace with HashJoin maybe (or add variant to possible plans).
        std::shared_ptr<QueryExecutionTree> left(new QueryExecutionTree(_qec));
        std::shared_ptr<QueryExecutionTree> right(new QueryExecutionTree(_qec));
        if (a[i]._qet.get()->resultSortedOn() == jcs[0][0]) {
          left = a[i]._qet;
        } else {
          // Create a sort operation.
          // But never sort scans, there we could have just scanned differently.
          if (a[i]._qet.get()->getType() == QueryExecutionTree::SCAN) {
            continue;
          }
          std::shared_ptr<Operation> sort(new Sort(_qec, a[i]._qet, jcs[0][0]));
          left.get()->setVariableColumns(
              a[i]._qet.get()->getVariableColumnMap());
          left.get()->setContextVars(a[i]._qet.get()->getContextVars());
          left.get()->setOperation(QueryExecutionTree::SORT, sort);
        }
        if (b[j]._qet.get()->resultSortedOn() == jcs[0][1]) {
          right = b[j]._qet;
        } else {
          // Create a sort operation.
          // But never sort scans, there we could have just scanned differently.
          if (b[j]._qet.get()->getType() == QueryExecutionTree::SCAN) {
            continue;
          }
          std::shared_ptr<Operation> sort(new Sort(_qec, b[j]._qet, jcs[0][1]));
          right.get()->setVariableColumns(
              b[j]._qet.get()->getVariableColumnMap());
          right.get()->setContextVars(b[j]._qet.get()->getContextVars());
          right.get()->setOperation(QueryExecutionTree::SORT, sort);
        }

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
        candidates[getPruningKey(plan, jcs[0][0])].emplace_back(plan);
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

  bool aSorted = jcs.size() == 1 &&
                 a._qet->getRootOperation()->resultSortedOn() == jcs[0][0];
  bool bSorted = jcs.size() == 1 &&
                 b._qet->getRootOperation()->resultSortedOn() == jcs[0][1];

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
    orderByPlanA._qet->setVariableColumns(a._qet->getVariableColumnMap());
    orderByPlanA._qet->setOperation(QueryExecutionTree::ORDER_BY, orderByA);
  }
  if (!bSorted) {
    orderByPlanB._qet->setVariableColumns(b._qet->getVariableColumnMap());
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

  if (a._isOptional || b._isOptional) {
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
  for (auto it = a._qet.get()->getVariableColumnMap().begin();
       it != a._qet.get()->getVariableColumnMap().end(); ++it) {
    auto itt = b._qet.get()->getVariableColumnMap().find(it->first);
    if (itt != b._qet.get()->getVariableColumnMap().end()) {
      jcs.push_back(array<Id, 2>{{it->second, itt->second}});
    }
  }
  return jcs;
}

// _____________________________________________________________________________
string QueryPlanner::getPruningKey(const QueryPlanner::SubtreePlan& plan,
                                   size_t orderedOnCol) const {
  // Get the ordered var
  std::ostringstream os;
  for (auto it = plan._qet.get()->getVariableColumnMap().begin();
       it != plan._qet.get()->getVariableColumnMap().end(); ++it) {
    if (it->second == orderedOnCol) {
      os << it->first;
      break;
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
  // filters May be duplicated. To prevent this, calling code has to ensure That
  // the method is only called once on each row. Similarly this affects the
  // (albeit rare) fact that a filter is directly applicable after a scan of a
  // huge relation where a subsequent join with a small result could be
  // translated into one or more scans directly. This also helps with cases
  // where applying the filter later is better. Finally, the replace flag can be
  // set to enforce that all filters are applied. This should be done for the
  // last row in the DPTab so that no filters are missed.
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
        newPlan._idsOfIncludedFilters |= (1 << i);
        newPlan._idsOfIncludedNodes = row[n]._idsOfIncludedNodes;
        newPlan._isOptional = row[n]._isOptional;
        auto& tree = *newPlan._qet.get();
        if (isVariable(filters[i]._rhs)) {
          std::shared_ptr<Operation> filter = std::make_shared<Filter>(
              _qec, row[n]._qet, filters[i]._type,
              row[n]._qet.get()->getVariableColumn(filters[i]._lhs),
              row[n]._qet.get()->getVariableColumn(filters[i]._rhs));
          tree.setOperation(QueryExecutionTree::FILTER, filter);
        } else {
          string compWith = filters[i]._rhs;
          Id entityId = 0;
          if (_qec) {
            // TODO(schnelle): A proper SPARQL parser should have
            // tagged/converted numeric values already. However our parser is
            // currently far too crude for that
            if (ad_utility::isXsdValue(compWith)) {
              compWith = ad_utility::convertValueLiteralToIndexWord(compWith);
            } else if (ad_utility::isNumeric(compWith)) {
              compWith = ad_utility::convertNumericToIndexWord(compWith);
            }
            if (filters[i]._type == SparqlFilter::EQ ||
                filters[i]._type == SparqlFilter::NE) {
              if (!_qec->getIndex().getVocab().getId(compWith, &entityId)) {
                entityId = std::numeric_limits<size_t>::max() - 1;
              }
            } else if (filters[i]._type == SparqlFilter::GE) {
              entityId = _qec->getIndex().getVocab().getValueIdForGE(compWith);
            } else if (filters[i]._type == SparqlFilter::GT) {
              entityId = _qec->getIndex().getVocab().getValueIdForGT(compWith);
            } else if (filters[i]._type == SparqlFilter::LT) {
              entityId = _qec->getIndex().getVocab().getValueIdForLT(compWith);
            } else if (filters[i]._type == SparqlFilter::LE) {
              entityId = _qec->getIndex().getVocab().getValueIdForLE(compWith);
            } else if (filters[i]._type == SparqlFilter::LANG_MATCHES ||
                       filters[i]._type == SparqlFilter::REGEX) {
              entityId = std::numeric_limits<size_t>::max() - 1;
            }
          }
          std::shared_ptr<Operation> filter(
              new Filter(_qec, row[n]._qet, filters[i]._type,
                         row[n]._qet.get()->getVariableColumn(filters[i]._lhs),
                         std::numeric_limits<size_t>::max(), entityId));
          if (_qec && (filters[i]._type == SparqlFilter::LANG_MATCHES ||
                       filters[i]._type == SparqlFilter::REGEX)) {
            static_cast<Filter*>(filter.get())
                ->setRightHandSideString(filters[i]._rhs);
            if (filters[i]._type == SparqlFilter::REGEX) {
              static_cast<Filter*>(filter.get())
                  ->setRegexIgnoreCase(filters[i]._regexIgnoreCase);
            }
          }
          tree.setOperation(QueryExecutionTree::FILTER, filter);
        }

        tree.setVariableColumns(row[n]._qet.get()->getVariableColumnMap());
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
vector<vector<QueryPlanner::SubtreePlan>> QueryPlanner::fillDpTab(
    const QueryPlanner::TripleGraph& tg, const vector<SparqlFilter>& filters,
    const vector<QueryPlanner::SubtreePlan*>& children) const {
  LOG(TRACE) << "Fill DP table... (there are " << tg._nodeMap.size()
             << " triples to join)" << std::endl;
  vector<vector<SubtreePlan>> dpTab;
  dpTab.emplace_back(seedWithScansAndText(tg, children));
  applyFiltersIfPossible(dpTab.back(), filters, tg._nodeMap.size() == 1);

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
      applyFiltersIfPossible(dpTab.back(), filters, k == tg._nodeMap.size());
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
  return _nodeMap.count(i) > 0 &&
         (_nodeMap.find(i)->second->_triple._p == CONTAINS_ENTITY_PREDICATE ||
          _nodeMap.find(i)->second->_triple._p == CONTAINS_WORD_PREDICATE ||
          _nodeMap.find(i)->second->_triple._p ==
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
        // Recursively split each part because there may be other context vars.
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
    for (auto it = subtrees[i].first.getVariableColumnMap().begin();
         it != subtrees[i].first.getVariableColumnMap().end(); ++it) {
      map[it->first] = offset + it->second;
      ++n;
    }
  }
  return map;
}
