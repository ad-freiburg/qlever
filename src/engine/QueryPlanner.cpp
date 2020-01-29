// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./QueryPlanner.h"

#include <algorithm>
#include <ctime>

#include "../parser/ParseException.h"
#include "../parser/ParsedQuery.h"
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
#include "TransitivePath.h"
#include "TwoColumnJoin.h"
#include "Union.h"
#include "Values.h"

// _____________________________________________________________________________
QueryPlanner::QueryPlanner(QueryExecutionContext* qec)
    : _qec(qec), _internalVarCount(0), _enablePatternTrick(true) {}

// _____________________________________________________________________________
QueryExecutionTree QueryPlanner::createExecutionTree(ParsedQuery& pq) {
  // Look for ql:has-predicate to determine if the pattern trick should be used.
  // If the pattern trick is used the ql:has-predicate triple will be removed
  // from the list of where clause triples. Otherwise the ql:has-relation triple
  // will be handled using a HasRelationScan.
  SparqlTriple patternTrickTriple("", PropertyPath(), "");
  bool usePatternTrick =
      _enablePatternTrick && checkUsePatternTrick(&pq, &patternTrickTriple);

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
  plans.push_back(optimize(pq._rootGraphPattern));

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
  auto minInd = findCheapestExecutionTree(lastRow);

  lastRow[minInd]._isOptional = pq._rootGraphPattern->_optional;

  SubtreePlan final = lastRow[minInd];
  final._qet.get()->setTextLimit(getTextLimit(pq._textLimit));
  LOG(DEBUG) << "Done creating execution plan.\n";
  return *final._qet.get();
}

std::vector<QueryPlanner::SubtreePlan> QueryPlanner::optimize(
    std::shared_ptr<const ParsedQuery::GraphPattern> rootPattern) {
  // Create a topological sorting of the trees GraphPatterns where children are
  // in the list after their parents. This allows for ensuring that children are
  // already optimized when their parents need to be optimized
  std::vector<std::shared_ptr<const ParsedQuery::GraphPattern>>
      patternsToProcess;
  std::vector<std::shared_ptr<const ParsedQuery::GraphPattern>> childrenToAdd;
  childrenToAdd.push_back(rootPattern);
  while (!childrenToAdd.empty()) {
    std::shared_ptr<const ParsedQuery::GraphPattern> pattern =
        childrenToAdd.back();
    childrenToAdd.pop_back();
    patternsToProcess.push_back(pattern);
    for (const std::shared_ptr<const ParsedQuery::GraphPatternOperation>& op :
         pattern->_children) {
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
    std::shared_ptr<const ParsedQuery::GraphPattern> pattern =
        patternsToProcess.back();
    patternsToProcess.pop_back();
    std::vector<std::vector<SubtreePlan>> plans;

    // Add all the OPTIONALs and UNIONs of the GraphPattern into the childPlans
    // vector. These will be treated the same as a node in the triple graph
    // later on (so the same as a simple triple).
    vector<const SubtreePlan*> childPlans;
    std::vector<SubtreePlan> childPlanStorage;
    std::vector<SubtreePlan> subqueryPlans;
    // Ensure the vectors won't reallocate (this is an upper bound on the actual
    // size)
    childPlanStorage.reserve(pattern->_children.size());
    subqueryPlans.reserve(pattern->_children.size());
    for (const std::shared_ptr<ParsedQuery::GraphPatternOperation>& child :
         pattern->_children) {
      switch (child->_type) {
        case ParsedQuery::GraphPatternOperation::Type::OPTIONAL:
          for (const std::shared_ptr<const ParsedQuery::GraphPattern>& p :
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
          childPlanStorage.emplace_back(_qec);
          auto unionOp = std::make_shared<Union>(_qec, left->_qet, right->_qet);
          QueryExecutionTree& tree = *childPlanStorage.back()._qet.get();
          tree.setVariableColumns(unionOp->getVariableColumns());
          tree.setOperation(QueryExecutionTree::UNION, unionOp);
          childPlans.push_back(&childPlanStorage.back());
        } break;
        case ParsedQuery::GraphPatternOperation::Type::SUBQUERY: {
          subqueryPlans.emplace_back(_qec);
          QueryExecutionTree tree = createExecutionTree(*child->_subquery);
          *subqueryPlans.back()._qet.get() = tree;
          childPlans.push_back(&subqueryPlans.back());
        } break;
        case ParsedQuery::GraphPatternOperation::Type::TRANS_PATH: {
          const SubtreePlan* sub =
              &patternPlans[child->_pathData._childGraphPattern->_id];
          childPlanStorage.emplace_back(_qec);
          size_t leftCol, rightCol;
          Id leftValue, rightValue;
          std::string leftColName, rightColName;
          size_t min, max;
          bool leftVar, rightVar;
          if (isVariable(child->_pathData._left)) {
            leftVar = true;
            leftCol = sub->_qet->getVariableColumn(child->_pathData._innerLeft);
            leftColName = child->_pathData._left;
          } else {
            leftVar = false;
            leftColName = generateUniqueVarName();
            leftCol = sub->_qet->getVariableColumn(child->_pathData._innerLeft);
            if (!_qec->getIndex().getVocab().getId(child->_pathData._left,
                                                   &leftValue)) {
              AD_THROW(ad_semsearch::Exception::BAD_QUERY,
                       "No vocabulary entry for " + child->_pathData._left);
            }
          }
          if (isVariable(child->_pathData._right)) {
            rightVar = true;
            rightCol =
                sub->_qet->getVariableColumn(child->_pathData._innerRight);
            rightColName = child->_pathData._right;
          } else {
            rightVar = false;
            rightCol =
                sub->_qet->getVariableColumn(child->_pathData._innerRight);
            rightColName = generateUniqueVarName();
            if (!_qec->getIndex().getVocab().getId(child->_pathData._right,
                                                   &rightValue)) {
              AD_THROW(ad_semsearch::Exception::BAD_QUERY,
                       "No vocabulary entry for " + child->_pathData._right);
            }
          }
          min = child->_pathData._min;
          max = child->_pathData._max;
          auto transOp = std::make_shared<TransitivePath>(
              _qec, sub->_qet, leftVar, rightVar, leftCol, rightCol, leftValue,
              rightValue, leftColName, rightColName, min, max);

          QueryExecutionTree& tree = *childPlanStorage.back()._qet.get();
          tree.setVariableColumns(static_cast<TransitivePath*>(transOp.get())
                                      ->getVariableColumns());
          tree.setOperation(QueryExecutionTree::TRANSITIVE_PATH, transOp);
          childPlans.push_back(&childPlanStorage.back());
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
        fillDpTab(tg, pattern->_filters, childPlans, pattern->_inlineValues)
            .back();

    if (pattern == rootPattern) {
      return lastRow;
    } else {
      auto minInd = findCheapestExecutionTree(lastRow);
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
  // Check if the query has the right number of variables for aliases and
  // group by.
  if (pq->_groupByVariables.size() != 1 || pq->_aliases.size() > 1) {
    return false;
  }

  bool returns_counts = pq->_aliases.size() == 1;

  // These will only be set if the query returns the count of predicates
  // The varialbe the COUNT alias counts
  std::string counted_var_name;
  // The variable holding the counts
  std::string count_var_name;

  if (returns_counts) {
    // There has to be a single count alias
    const ParsedQuery::Alias& alias = pq->_aliases.back();
    // Create a lower case version of the aliases function string to allow
    // for case insensitive keyword detection.
    std::string aliasFunctionLower =
        ad_utility::getLowercaseUtf8(alias._function);
    // Check if the alias is a non distinct count alias
    if (!(alias._isAggregate &&
          aliasFunctionLower.find("distinct") == std::string::npos &&
          ad_utility::startsWith(aliasFunctionLower, "count"))) {
      return false;
    }
    counted_var_name = alias._inVarName;
    count_var_name = alias._outVarName;
  }

  // The first possibility for using the pattern trick is having a
  // ql:has-predicate predicate in the query

  // look for a HAS_RELATION_PREDICATE triple which satisfies all constraints
  for (size_t i = 0; i < pq->_rootGraphPattern->_whereClauseTriples.size();
       i++) {
    bool usePatternTrick = true;
    const SparqlTriple& t = pq->_rootGraphPattern->_whereClauseTriples[i];
    // Check that the triples predicates is the HAS_PREDICATE_PREDICATE.
    // Also check that the triples object or subject matches the aliases input
    // variable and the group by variable.
    if (t._p._iri != HAS_PREDICATE_PREDICATE ||
        (returns_counts &&
         !(counted_var_name == t._o || counted_var_name == t._s)) ||
        pq->_groupByVariables[0] != t._o) {
      usePatternTrick = false;
      continue;
    }

    // check that all selected variables are outputs of
    // CountAvailablePredicates
    for (const std::string& s : pq->_selectedVariables) {
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
    for (size_t j = 0; usePatternTrick &&
                       j < pq->_rootGraphPattern->_whereClauseTriples.size();
         j++) {
      const SparqlTriple& other = pq->_rootGraphPattern->_whereClauseTriples[j];
      if (j != i &&
          (other._s == t._o || other._p._iri == t._o || other._o == t._o)) {
        usePatternTrick = false;
      }
    }
    if (!usePatternTrick) {
      continue;
    }

    // Check for filters on the ql:has-predicate triple's subject or
    // object.
    // Filters that filter on the triple's object but have a static
    // rhs will be transformed to a having clause later on.
    for (const SparqlFilter& filter : pq->_rootGraphPattern->_filters) {
      if (!(filter._lhs == t._o && filter._rhs[0] != '?') &&
          (filter._lhs == t._s || filter._lhs == t._o || filter._rhs == t._o ||
           filter._rhs == t._s)) {
        usePatternTrick = false;
        break;
      }
    }
    if (!usePatternTrick) {
      continue;
    }

    // Check for sub graph patterns containing the ql:has-predicate
    // triple's object
    std::vector<std::shared_ptr<const ParsedQuery::GraphPattern>>
        graphsToProcess;
    for (const std::shared_ptr<const ParsedQuery::GraphPatternOperation>& op :
         pq->_rootGraphPattern->_children) {
      switch (op->_type) {
        case ParsedQuery::GraphPatternOperation::Type::OPTIONAL:
        case ParsedQuery::GraphPatternOperation::Type::UNION:
          graphsToProcess.insert(graphsToProcess.end(),
                                 op->_childGraphPatterns.begin(),
                                 op->_childGraphPatterns.end());
          break;
        case ParsedQuery::GraphPatternOperation::Type::SUBQUERY:
          for (const std::string& v : op->_subquery->_selectedVariables) {
            if (v == t._o) {
              usePatternTrick = false;
              break;
            }
          }
          break;
        case ParsedQuery::GraphPatternOperation::Type::TRANS_PATH:
          // Transitive paths cannot yet exist in the query. They could also not
          // contain the variables we are interested in.
          break;
      }
    }
    while (!graphsToProcess.empty() && usePatternTrick) {
      std::shared_ptr<const ParsedQuery::GraphPattern> pattern =
          graphsToProcess.back();
      graphsToProcess.pop_back();

      for (const std::shared_ptr<const ParsedQuery::GraphPatternOperation>& op :
           pattern->_children) {
        switch (op->_type) {
          case ParsedQuery::GraphPatternOperation::Type::OPTIONAL:
          case ParsedQuery::GraphPatternOperation::Type::UNION:
            graphsToProcess.insert(graphsToProcess.end(),
                                   op->_childGraphPatterns.begin(),
                                   op->_childGraphPatterns.end());
            break;
          case ParsedQuery::GraphPatternOperation::Type::SUBQUERY:
            for (const std::string& v : op->_subquery->_selectedVariables) {
              if (v == t._o) {
                usePatternTrick = false;
                break;
              }
            }
            break;
          case ParsedQuery::GraphPatternOperation::Type::TRANS_PATH:
            // Transitive paths cannot yet exist in the query. They could also
            // not contain the variables we are interested in.
            break;
        }
      }

      for (const SparqlTriple& other : pattern->_whereClauseTriples) {
        if (other._s == t._o || other._p._iri == t._o || other._o == t._o) {
          usePatternTrick = false;
          break;
        }
      }
      if (!usePatternTrick) {
        break;
      }
    }
    if (!usePatternTrick) {
      continue;
    }

    LOG(DEBUG) << "Using the pattern trick to answer the query." << endl;
    *patternTrickTriple = t;
    // remove the triple from the graph
    pq->_rootGraphPattern->_whereClauseTriples.erase(
        pq->_rootGraphPattern->_whereClauseTriples.begin() + i);
    // Transform filters on the ql:has-relation triple's object that
    // have a static rhs to having clauses
    for (size_t i = 0; i < pq->_rootGraphPattern->_filters.size(); i++) {
      const SparqlFilter& filter = pq->_rootGraphPattern->_filters[i];
      if (filter._lhs == t._o && filter._rhs[0] != '?') {
        pq->_havingClauses.push_back(filter);
        pq->_rootGraphPattern->_filters.erase(
            pq->_rootGraphPattern->_filters.begin() + i);
        i--;
      }
    }
    return true;
  }

  // The second possibility for using the pattern trick is a subquery.

  LOG(TRACE) << "Considering a subquery as a patterntrick candidate"
             << std::endl;

  // Check if the queries single child is a subquery that contains a triple
  // of the form ?s ?p ?o with constraints solely on ?s and a select on
  // distinct ?s and ?p
  std::string predVar = pq->_selectedVariables[0];
  std::string subjVar = counted_var_name;
  LOG(TRACE) << "The subject is " << subjVar << " the predicate " << predVar
             << std::endl;
  std::string objVar;
  std::shared_ptr<ParsedQuery::GraphPattern> root = pq->_rootGraphPattern;

  // Check that pq does not have where clause triples or filters, but
  // contains a single subquery child
  if (!root->_filters.empty() || !root->_whereClauseTriples.empty() ||
      root->_children.size() != 1 ||
      root->_children[0]->_type !=
          ParsedQuery::GraphPatternOperation::Type::SUBQUERY) {
    return false;
  }

  LOG(TRACE) << "Query has a single subquery of the right type" << std::endl;

  // Check that the query is distinct and does not do any grouping and returns 2
  // variables.
  std::shared_ptr<ParsedQuery> sub = root->_children[0]->_subquery;
  if (!sub->_distinct || sub->_groupByVariables.size() > 0 ||
      sub->_aliases.size() > 0 || sub->_selectedVariables.size() != 2) {
    return false;
  }
  // Also check that it returns the correct variables
  for (std::string& v : sub->_selectedVariables) {
    if (v != predVar && v != subjVar) {
      return false;
    }
  }

  LOG(TRACE) << "The subquery has the correct variables" << std::endl;

  // Look for a triple in the subquery of the form 'predVar subjVar ?o'
  std::shared_ptr<ParsedQuery::GraphPattern> subroot = sub->_rootGraphPattern;
  for (size_t i = 0; i < subroot->_whereClauseTriples.size(); i++) {
    const SparqlTriple& t = subroot->_whereClauseTriples[i];
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
    for (size_t j = 0; j < subroot->_whereClauseTriples.size(); j++) {
      if (j != i) {
        const SparqlTriple& t2 = subroot->_whereClauseTriples[j];
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
    for (size_t j = 0; j < subroot->_filters.size(); j++) {
      const SparqlFilter& f = subroot->_filters[j];
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
    subroot->_whereClauseTriples.erase(subroot->_whereClauseTriples.begin() +
                                       i);
    root->_children.clear();
    pq->merge(*sub);
    break;
  }
  return true;
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
      auto distinct =
          std::make_shared<Distinct>(_qec, parent._qet, keepIndices);
      distinctPlan._qet->setOperation(QueryExecutionTree::DISTINCT, distinct);
      distinctPlan._qet->setVariableColumns(distinct->getVariableColumns());
      distinctPlan._qet->setContextVars(parent._qet.get()->getContextVars());
    } else {
      if (keepIndices.size() == 1) {
        auto tree = std::make_shared<QueryExecutionTree>(_qec);
        auto sort = std::make_shared<Sort>(_qec, parent._qet, keepIndices[0]);
        tree->setVariableColumns(parent._qet->getVariableColumns());
        tree->setOperation(QueryExecutionTree::SORT, sort);
        tree->setContextVars(parent._qet.get()->getContextVars());
        auto distinct = std::make_shared<Distinct>(_qec, tree, keepIndices);
        distinctPlan._qet->setOperation(QueryExecutionTree::DISTINCT, distinct);
        distinctPlan._qet->setVariableColumns(distinct->getVariableColumns());
        distinctPlan._qet->setContextVars(parent._qet.get()->getContextVars());
      } else {
        auto tree = std::make_shared<QueryExecutionTree>(_qec);
        vector<pair<size_t, bool>> obCols;
        for (auto& i : keepIndices) {
          obCols.emplace_back(std::make_pair(i, false));
        }
        auto ob = std::make_shared<OrderBy>(_qec, parent._qet, obCols);
        tree->setVariableColumns(parent._qet->getVariableColumns());
        tree->setOperation(QueryExecutionTree::ORDER_BY, ob);
        tree->setContextVars(parent._qet.get()->getContextVars());
        auto distinct = std::make_shared<Distinct>(_qec, tree, keepIndices);
        distinctPlan._qet->setOperation(QueryExecutionTree::DISTINCT, distinct);
        distinctPlan._qet->setVariableColumns(distinct->getVariableColumns());
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
    const SparqlTriple& patternTrickTriple) {
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
        auto orderByOp =
            std::make_shared<OrderBy>(_qec, parent._qet, sortIndices);
        orderByPlan._qet->setVariableColumns(parent._qet->getVariableColumns());
        orderByPlan._qet->setOperation(QueryExecutionTree::ORDER_BY, orderByOp);
      }
      SubtreePlan patternTrickPlan(_qec);
      auto countPred = std::make_shared<CountAvailablePredicates>(
          _qec, isSorted ? parent._qet : orderByPlan._qet, subjectColumn);

      countPred->setVarNames(patternTrickTriple._o, pq._aliases[0]._outVarName);
      QueryExecutionTree& tree = *patternTrickPlan._qet.get();
      tree.setVariableColumns(countPred->getVariableColumns());
      tree.setOperation(QueryExecutionTree::COUNT_AVAILABLE_PREDICATES,
                        countPred);
      added.push_back(patternTrickPlan);
    }
  } else if (patternTrickTriple._s[0] != '?') {
    // The subject of the pattern trick is not a variable
    SubtreePlan patternTrickPlan(_qec);
    auto countPred =
        std::make_shared<CountAvailablePredicates>(_qec, patternTrickTriple._s);

    countPred->setVarNames(patternTrickTriple._o, pq._aliases[0]._outVarName);
    QueryExecutionTree& tree = *patternTrickPlan._qet.get();
    tree.setVariableColumns(countPred->getVariableColumns());
    tree.setOperation(QueryExecutionTree::COUNT_AVAILABLE_PREDICATES,
                      countPred);
    added.push_back(patternTrickPlan);
  } else {
    // Use the pattern trick without a subtree
    SubtreePlan patternTrickPlan(_qec);
    auto countPred = std::make_shared<CountAvailablePredicates>(_qec);

    if (pq._aliases.size() > 0) {
      countPred->setVarNames(patternTrickTriple._o, pq._aliases[0]._outVarName);
    } else {
      countPred->setVarNames(patternTrickTriple._o, generateUniqueVarName());
    }
    QueryExecutionTree& tree = *patternTrickPlan._qet.get();
    tree.setVariableColumns(countPred->getVariableColumns());
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
    auto groupBy =
        std::make_shared<GroupBy>(_qec, pq._groupByVariables, pq._aliases);
    QueryExecutionTree& groupByTree = *groupByPlan._qet.get();

    // Then compute the sort columns
    std::vector<std::pair<size_t, bool>> sortColumns =
        groupBy->computeSortColumns(parent->_qet);

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
      auto orderBy = std::make_shared<OrderBy>(_qec, parent->_qet, sortColumns);
      QueryExecutionTree& orderByTree = *orderByPlan._qet.get();
      orderByTree.setVariableColumns(parent->_qet->getVariableColumns());
      orderByTree.setOperation(QueryExecutionTree::ORDER_BY, orderBy);
      parent = &orderByPlan;
    }

    groupBy->setSubtree(parent->_qet);
    groupByTree.setVariableColumns(groupBy->getVariableColumns());
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
        auto sort = std::make_shared<Sort>(_qec, previous[i]._qet, col);
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
        auto ob =
            std::make_shared<OrderBy>(_qec, previous[i]._qet, sortIndices);
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
  for (SparqlTriple& t : pq._rootGraphPattern->_whereClauseTriples) {
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
QueryPlanner::TripleGraph QueryPlanner::createTripleGraph(
    std::shared_ptr<const ParsedQuery::GraphPattern> pattern) const {
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
    const vector<const QueryPlanner::SubtreePlan*>& children,
    const vector<SparqlValues>& values) {
  vector<SubtreePlan> seeds;
  // add all child plans as seeds
  uint64_t idShift = tg._nodeMap.size();
  for (const SubtreePlan* plan : children) {
    SubtreePlan newIdPlan = *plan;
    // give the plan a unique id bit
    newIdPlan._idsOfIncludedNodes = uint64_t(1) << idShift;
    newIdPlan._idsOfIncludedFilters = 0;
    seeds.emplace_back(newIdPlan);
    idShift++;
  }
  for (const SparqlValues& val : values) {
    SubtreePlan valuesPlan(_qec);
    std::shared_ptr<Values> op = std::make_shared<Values>(_qec, val);
    valuesPlan._qet->setOperation(QueryExecutionTree::OperationType::VALUES,
                                  op);
    valuesPlan._qet->setVariableColumns(op->getVariableColumns());
    valuesPlan._idsOfIncludedNodes = uint64_t(1) << idShift;
    valuesPlan._idsOfIncludedFilters = 0;
    seeds.emplace_back(valuesPlan);
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
            // TODO(schnelle): Handle ?p ql:has-prediacte ?p
            // Add a has relation scan instead of a normal IndexScan
            HasPredicateScan::ScanType scanType;
            if (isVariable(node._triple._s)) {
              scanType = HasPredicateScan::ScanType::FREE_S;
            } else if (isVariable(node._triple._o)) {
              scanType = HasPredicateScan::ScanType::FREE_O;
            }
            auto scan = std::make_shared<HasPredicateScan>(_qec, scanType);
            scan->setSubject(node._triple._s);
            scan->setObject(node._triple._o);
            tree.setOperation(
                QueryExecutionTree::OperationType::HAS_RELATION_SCAN, scan);
            tree.setVariableColumns(scan->getVariableColumns());
          } else if (isVariable(node._triple._s) &&
                     isVariable(node._triple._o) &&
                     node._triple._s == node._triple._o) {
            if (isVariable(node._triple._p._iri)) {
              AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
                       "Triple with one variable repated 3 times");
            }
            LOG(DEBUG) << "Subject variable same as object variable"
                       << std::endl;
            // Need to handle this as IndexScan with a new unique
            // variable + Filter. Works in both directions
            std::string filterVar = generateUniqueVarName();

            auto scanTree = std::make_shared<QueryExecutionTree>(_qec);
            auto scan = std::make_shared<IndexScan>(
                _qec, IndexScan::ScanType::PSO_FREE_S);
            scan->setSubject(node._triple._s);
            scan->setPredicate(node._triple._p._iri);
            scan->setObject(filterVar);
            scan->precomputeSizeEstimate();
            scanTree->setOperation(QueryExecutionTree::OperationType::SCAN,
                                   scan);
            scanTree->setVariableColumn(node._triple._s, 0);
            scanTree->setVariableColumn(filterVar, 1);
            auto filter = std::make_shared<Filter>(_qec, scanTree,
                                                   SparqlFilter::FilterType::EQ,
                                                   node._triple._s, filterVar);
            tree.setOperation(QueryExecutionTree::OperationType::FILTER,
                              filter);
            tree.setVariableColumns(filter->getVariableColumns());
          } else if (isVariable(node._triple._s)) {
            auto scan = std::make_shared<IndexScan>(
                _qec, IndexScan::ScanType::POS_BOUND_O);
            scan->setSubject(node._triple._s);
            scan->setPredicate(node._triple._p._iri);
            scan->setObject(node._triple._o);
            tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
            tree.setVariableColumn(node._triple._s, 0);
          } else if (isVariable(node._triple._o)) {
            auto scan = std::make_shared<IndexScan>(
                _qec, IndexScan::ScanType::PSO_BOUND_S);
            scan->setSubject(node._triple._s);
            scan->setPredicate(node._triple._p._iri);
            scan->setObject(node._triple._o);
            tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
            tree.setVariableColumn(node._triple._o, 0);
          } else {
            assert(isVariable(node._triple._p));
            auto scan = std::make_shared<IndexScan>(
                _qec, IndexScan::ScanType::SOP_BOUND_O);
            scan->setSubject(node._triple._s);
            scan->setPredicate(node._triple._p._iri);
            scan->setObject(node._triple._o);
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
            auto scan = std::make_shared<HasPredicateScan>(
                _qec, HasPredicateScan::ScanType::FULL_SCAN);
            scan->setSubject(node._triple._s);
            scan->setObject(node._triple._o);
            tree.setOperation(
                QueryExecutionTree::OperationType::HAS_RELATION_SCAN, scan);
            tree.setVariableColumns(scan->getVariableColumns());
            seeds.push_back(plan);
          } else if (!isVariable(node._triple._p._iri)) {
            {
              SubtreePlan plan(_qec);
              plan._idsOfIncludedNodes |= (uint64_t(1) << i);
              auto& tree = *plan._qet.get();
              auto scan = std::make_shared<IndexScan>(
                  _qec, IndexScan::ScanType::PSO_FREE_S);
              scan->setSubject(node._triple._s);
              scan->setPredicate(node._triple._p._iri);
              scan->setObject(node._triple._o);
              scan->precomputeSizeEstimate();
              tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
              tree.setVariableColumn(node._triple._s, 0);
              tree.setVariableColumn(node._triple._o, 1);
              seeds.push_back(plan);
            }
            {
              SubtreePlan plan(_qec);
              plan._idsOfIncludedNodes |= (uint64_t(1) << i);
              auto& tree = *plan._qet.get();
              auto scan = std::make_shared<IndexScan>(
                  _qec, IndexScan::ScanType::POS_FREE_O);
              scan->setSubject(node._triple._s);
              scan->setPredicate(node._triple._p._iri);
              scan->setObject(node._triple._o);
              scan->precomputeSizeEstimate();
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
              auto scan = std::make_shared<IndexScan>(
                  _qec, IndexScan::ScanType::SPO_FREE_P);
              scan->setSubject(node._triple._s);
              scan->setPredicate(node._triple._p._iri);
              scan->setObject(node._triple._o);
              scan->precomputeSizeEstimate();
              tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
              tree.setVariableColumn(node._triple._p._iri, 0);
              tree.setVariableColumn(node._triple._o, 1);
              seeds.push_back(plan);
            }
            {
              SubtreePlan plan(_qec);
              plan._idsOfIncludedNodes |= (uint64_t(1) << i);
              auto& tree = *plan._qet.get();
              auto scan = std::make_shared<IndexScan>(
                  _qec, IndexScan::ScanType::SOP_FREE_O);
              scan->setSubject(node._triple._s);
              scan->setPredicate(node._triple._p._iri);
              scan->setObject(node._triple._o);
              scan->precomputeSizeEstimate();
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
              auto scan = std::make_shared<IndexScan>(
                  _qec, IndexScan::ScanType::OSP_FREE_S);
              scan->setSubject(node._triple._s);
              scan->setPredicate(node._triple._p._iri);
              scan->setObject(node._triple._o);
              scan->precomputeSizeEstimate();
              tree.setOperation(QueryExecutionTree::OperationType::SCAN, scan);
              tree.setVariableColumn(node._triple._s, 0);
              tree.setVariableColumn(node._triple._p._iri, 1);
              seeds.push_back(plan);
            }
            {
              SubtreePlan plan(_qec);
              plan._idsOfIncludedNodes |= (uint64_t(1) << i);
              auto& tree = *plan._qet.get();
              auto scan = std::make_shared<IndexScan>(
                  _qec, IndexScan::ScanType::OPS_FREE_P);
              scan->setSubject(node._triple._s);
              scan->setPredicate(node._triple._p._iri);
              scan->setObject(node._triple._o);
              scan->precomputeSizeEstimate();
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
              auto scan = std::make_shared<IndexScan>(
                  _qec, IndexScan::ScanType::FULL_INDEX_SCAN_SPO);
              scan->precomputeSizeEstimate();
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
              auto scan = std::make_shared<IndexScan>(
                  _qec, IndexScan::ScanType::FULL_INDEX_SCAN_SOP);
              scan->precomputeSizeEstimate();
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
              auto scan = std::make_shared<IndexScan>(
                  _qec, IndexScan::ScanType::FULL_INDEX_SCAN_PSO);
              scan->precomputeSizeEstimate();
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
              auto scan = std::make_shared<IndexScan>(
                  _qec, IndexScan::ScanType::FULL_INDEX_SCAN_POS);
              scan->precomputeSizeEstimate();
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
              auto scan = std::make_shared<IndexScan>(
                  _qec, IndexScan::ScanType::FULL_INDEX_SCAN_OSP);
              scan->precomputeSizeEstimate();
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
              auto scan = std::make_shared<IndexScan>(
                  _qec, IndexScan::ScanType::FULL_INDEX_SCAN_OPS);
              scan->precomputeSizeEstimate();
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
  if (triple._p._can_be_null) {
    std::stringstream buf;
    buf << "The property path ";
    triple._p.writeToStream(buf);
    buf << " can evaluate to the empty path which is not yet supported.";
    AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, buf.str());
  }
  std::shared_ptr<ParsedQuery::GraphPattern> pattern =
      seedFromPropertyPath(triple._s, triple._p, triple._o);
#if LOGLEVEL >= TRACE
  std::ostringstream out;
  pattern->toString(out, 0);
  LOG(TRACE) << "Turned " << triple.asString() << " into " << std::endl;
  LOG(TRACE) << out.str() << std::endl << std::endl;
#endif
  pattern->recomputeIds();
  return optimize(pattern);
}

std::shared_ptr<ParsedQuery::GraphPattern> QueryPlanner::seedFromPropertyPath(
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
std::shared_ptr<ParsedQuery::GraphPattern> QueryPlanner::seedFromSequence(
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
  connectionVarNames[0] = left;
  connectionVarNames.back() = right;
  for (size_t i = 1; i + 1 < connectionVarNames.size(); i++) {
    connectionVarNames[i] = generateUniqueVarName();
  }

  // Stores the pattern for every non null chunk
  std::vector<std::shared_ptr<ParsedQuery::GraphPattern>> chunkPatterns;
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
    std::vector<std::shared_ptr<ParsedQuery::GraphPattern>> join_patterns;
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
      std::string l = left;
      std::string r;
      if (included_ids.size() > 1) {
        r = generateUniqueVarName();
      } else {
        r = right;
      }

      // Merge all the child plans into one graph pattern
      // excluding those that can be null and are not marked in the bitmask
      auto p = std::make_shared<ParsedQuery::GraphPattern>();
      for (size_t i = 0; i < included_ids.size(); i++) {
        std::shared_ptr<ParsedQuery::GraphPattern> child =
            seedFromPropertyPath(l, path._children[included_ids[i]], r);
        p->_children.insert(p->_children.end(), child->_children.begin(),
                            child->_children.end());
        p->_filters.insert(p->_filters.end(), child->_filters.begin(),
                           child->_filters.end());
        p->_whereClauseTriples.insert(p->_whereClauseTriples.begin(),
                                      child->_whereClauseTriples.begin(),
                                      child->_whereClauseTriples.end());
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
      chunkPatterns.push_back(join_patterns[0]);
    } else {
      chunkPatterns.push_back(uniteGraphPatterns(join_patterns));
    }
  }

  if (chunkPatterns.size() == 1) {
    return chunkPatterns[0];
  }

  // Join the chunk patterns
  std::shared_ptr<ParsedQuery::GraphPattern> fp =
      std::make_shared<ParsedQuery::GraphPattern>();

  for (const std::shared_ptr<ParsedQuery::GraphPattern>& p : chunkPatterns) {
    fp->_children.insert(fp->_children.begin(), p->_children.begin(),
                         p->_children.end());
    fp->_whereClauseTriples.insert(fp->_whereClauseTriples.begin(),
                                   p->_whereClauseTriples.begin(),
                                   p->_whereClauseTriples.end());
    fp->_filters.insert(fp->_filters.begin(), p->_filters.begin(),
                        p->_filters.end());
  }

  return fp;
}

// _____________________________________________________________________________
std::shared_ptr<ParsedQuery::GraphPattern> QueryPlanner::seedFromAlternative(
    const std::string& left, const PropertyPath& path,
    const std::string& right) {
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

  std::vector<std::shared_ptr<ParsedQuery::GraphPattern>> childPlans;
  childPlans.reserve(path._children.size());
  for (size_t i = 0; i < path._children.size(); i++) {
    childPlans.push_back(seedFromPropertyPath(left, path._children[i], right));
  }

  return uniteGraphPatterns(childPlans);
}

// _____________________________________________________________________________
std::shared_ptr<ParsedQuery::GraphPattern> QueryPlanner::seedFromTransitive(
    const std::string& left, const PropertyPath& path,
    const std::string& right) {
  std::string innerLeft = generateUniqueVarName();
  std::string innerRight = generateUniqueVarName();
  std::shared_ptr<ParsedQuery::GraphPattern> childPlan =
      seedFromPropertyPath(innerLeft, path._children[0], innerRight);
  std::shared_ptr<ParsedQuery::GraphPattern> p =
      std::make_shared<ParsedQuery::GraphPattern>();
  std::shared_ptr<ParsedQuery::GraphPatternOperation> op =
      std::make_shared<ParsedQuery::GraphPatternOperation>(
          ParsedQuery::GraphPatternOperation::Type::TRANS_PATH);
  new (&op->_pathData._left) std::string(left);
  new (&op->_pathData._right) std::string(right);
  new (&op->_pathData._innerLeft) std::string(innerLeft);
  new (&op->_pathData._innerRight) std::string(innerRight);
  op->_pathData._min = 1;
  op->_pathData._max = std::numeric_limits<size_t>::max();
  op->_pathData._childGraphPattern = childPlan;
  p->_children.push_back(op);
  return p;
}

// _____________________________________________________________________________
std::shared_ptr<ParsedQuery::GraphPattern> QueryPlanner::seedFromTransitiveMin(
    const std::string& left, const PropertyPath& path,
    const std::string& right) {
  std::string innerLeft = generateUniqueVarName();
  std::string innerRight = generateUniqueVarName();
  std::shared_ptr<ParsedQuery::GraphPattern> childPlan =
      seedFromPropertyPath(innerLeft, path._children[0], innerRight);
  std::shared_ptr<ParsedQuery::GraphPattern> p =
      std::make_shared<ParsedQuery::GraphPattern>();
  std::shared_ptr<ParsedQuery::GraphPatternOperation> op =
      std::make_shared<ParsedQuery::GraphPatternOperation>(
          ParsedQuery::GraphPatternOperation::Type::TRANS_PATH);
  // The strings are inside of a union, they need to be constructed in place
  new (&op->_pathData._left) std::string(left);
  new (&op->_pathData._right) std::string(right);
  new (&op->_pathData._innerLeft) std::string(innerLeft);
  new (&op->_pathData._innerRight) std::string(innerRight);
  op->_pathData._min = std::max(uint_fast16_t(1), path._limit);
  op->_pathData._max = std::numeric_limits<size_t>::max();
  op->_pathData._childGraphPattern = childPlan;
  p->_children.push_back(op);
  return p;
}

// _____________________________________________________________________________
std::shared_ptr<ParsedQuery::GraphPattern> QueryPlanner::seedFromTransitiveMax(
    const std::string& left, const PropertyPath& path,
    const std::string& right) {
  std::string innerLeft = generateUniqueVarName();
  std::string innerRight = generateUniqueVarName();
  std::shared_ptr<ParsedQuery::GraphPattern> childPlan =
      seedFromPropertyPath(innerLeft, path._children[0], innerRight);
  std::shared_ptr<ParsedQuery::GraphPattern> p =
      std::make_shared<ParsedQuery::GraphPattern>();
  std::shared_ptr<ParsedQuery::GraphPatternOperation> op =
      std::make_shared<ParsedQuery::GraphPatternOperation>(
          ParsedQuery::GraphPatternOperation::Type::TRANS_PATH);
  new (&op->_pathData._left) std::string(left);
  new (&op->_pathData._right) std::string(right);
  new (&op->_pathData._innerLeft) std::string(innerLeft);
  new (&op->_pathData._innerRight) std::string(innerRight);
  op->_pathData._min = 1;
  op->_pathData._max = path._limit;
  op->_pathData._childGraphPattern = childPlan;
  p->_children.push_back(op);
  return p;
}

// _____________________________________________________________________________
std::shared_ptr<ParsedQuery::GraphPattern> QueryPlanner::seedFromInverse(
    const std::string& left, const PropertyPath& path,
    const std::string& right) {
  return seedFromPropertyPath(right, path._children[0], left);
}

// _____________________________________________________________________________
std::shared_ptr<ParsedQuery::GraphPattern> QueryPlanner::seedFromIri(
    const std::string& left, const PropertyPath& path,
    const std::string& right) {
  std::shared_ptr<ParsedQuery::GraphPattern> p =
      std::make_shared<ParsedQuery::GraphPattern>();
  p->_whereClauseTriples.push_back(SparqlTriple(left, path, right));
  return p;
}

std::shared_ptr<ParsedQuery::GraphPattern> QueryPlanner::uniteGraphPatterns(
    const std::vector<std::shared_ptr<ParsedQuery::GraphPattern>>& patterns)
    const {
  using GraphPattern = ParsedQuery::GraphPattern;
  using GraphPatternOperation = ParsedQuery::GraphPatternOperation;
  // Build a tree of union operations
  auto p = std::make_shared<GraphPattern>();
  p->_children.push_back(std::make_shared<GraphPatternOperation>(
      GraphPatternOperation::Type::UNION,
      std::initializer_list<std::shared_ptr<GraphPattern>>{patterns[0],
                                                           patterns[1]}));
  for (size_t i = 2; i < patterns.size(); i++) {
    std::shared_ptr<GraphPattern> next = std::make_shared<GraphPattern>();
    next->_children.push_back(std::make_shared<GraphPatternOperation>(
        GraphPatternOperation::Type::UNION,
        std::initializer_list<std::shared_ptr<GraphPattern>>{p, patterns[i]}));
    p = next;
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
  auto& tree = *plan._qet.get();
  AD_CHECK(node._wordPart.size() > 0);
  auto textOp = std::make_shared<TextOperationWithoutFilter>(
      _qec, node._wordPart, node._variables, node._cvar);
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
              auto orderBy =
                  std::make_shared<OrderBy>(_qec, a[i]._qet, sortIndices);
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
              auto orderBy =
                  std::make_shared<OrderBy>(_qec, b[j]._qet, sortIndices);
              right->setVariableColumns(b[j]._qet->getVariableColumns());
              right->setOperation(QueryExecutionTree::ORDER_BY, orderBy);
            }
            // TODO(florian): consider replacing this with a multicolumn join.
            // Create the join operation.
            SubtreePlan plan(_qec);
            auto& tree = *plan._qet.get();
            auto join = std::make_shared<TwoColumnJoin>(_qec, left, right, jcs);
            tree.setVariableColumns(join->getVariableColumns());
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
          auto textOp = std::make_shared<TextOperationWithFilter>(
              _qec, noFilter.getWordPart(), noFilter.getVars(),
              noFilter.getCVar(), filterPlan._qet, otherPlanJc);
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
          size_t subtree_col = 0;

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
              subtree_col = jcs[0][1];
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
              subtree_col = jcs[0][0];
              replaceJoin = true;
            }
          }
          if (replaceJoin) {
            SubtreePlan plan(_qec);
            auto& tree = *plan._qet.get();
            auto scan = std::make_shared<HasPredicateScan>(
                _qec, HasPredicateScan::ScanType::SUBQUERY_S);
            scan->setSubtree(other);
            scan->setSubtreeSubjectColumn(subtree_col);
            scan->setObject(static_cast<HasPredicateScan*>(
                                hasPredicateScan->getRootOperation().get())
                                ->getObject());
            tree.setVariableColumns(scan->getVariableColumns());
            tree.setOperation(QueryExecutionTree::HAS_RELATION_SCAN, scan);
            plan._idsOfIncludedNodes = a[i]._idsOfIncludedNodes;
            plan.addAllNodes(b[j]._idsOfIncludedNodes);
            plan._idsOfIncludedFilters = a[i]._idsOfIncludedFilters;
            plan._idsOfIncludedFilters |= b[j]._idsOfIncludedFilters;
            candidates[getPruningKey(plan, scan->resultSortedOn())]
                .emplace_back(plan);
            continue;
          }
        }

        // Test for binding the other operation to the left side of the
        // transitive path
        if ((a[i]._qet.get()->getType() ==
                 QueryExecutionTree::OperationType::TRANSITIVE_PATH &&
             jcs[0][0] == 0) ||
            (b[j]._qet.get()->getType() ==
                 QueryExecutionTree::OperationType::TRANSITIVE_PATH &&
             jcs[0][1] == 0)) {
          std::shared_ptr<TransitivePath> srcpath;
          std::shared_ptr<QueryExecutionTree> other;
          size_t otherCol;
          if (a[i]._qet.get()->getType() ==
              QueryExecutionTree::OperationType::TRANSITIVE_PATH) {
            srcpath = std::reinterpret_pointer_cast<TransitivePath>(
                a[i]._qet->getRootOperation());
            other = b[j]._qet;
            otherCol = jcs[0][1];
          } else {
            other = a[i]._qet;
            srcpath = std::reinterpret_pointer_cast<TransitivePath>(
                b[j]._qet->getRootOperation());
            otherCol = jcs[0][0];
          }

          // Do not bind the side of a path twice
          if (!srcpath->isBound() &&
              other->getSizeEstimate() < srcpath->getSizeEstimate()) {
            // The left or right side is a TRANSITIVE_PATH and its join column
            // corresponds to the left side of its input.

            const vector<size_t>& otherSortedOn = other->resultSortedOn();
            if (otherSortedOn.size() == 0 || otherSortedOn[0] != otherCol) {
              if (other->getType() == QueryExecutionTree::SCAN) {
                continue;
              }
              auto sort = std::make_shared<Sort>(_qec, other, jcs[0][0]);
              std::shared_ptr<QueryExecutionTree> sortedOther =
                  std::make_shared<QueryExecutionTree>(_qec);
              sortedOther->setVariableColumns(other->getVariableColumns());
              sortedOther->setContextVars(other->getContextVars());
              sortedOther->setOperation(QueryExecutionTree::SORT, sort);
              other = sortedOther;
            }

            SubtreePlan plan(_qec);
            auto& tree = *plan._qet.get();
            auto newpath = srcpath->bindLeftSide(other, otherCol);
            tree.setVariableColumns(newpath->getVariableColumns());
            tree.setOperation(QueryExecutionTree::TRANSITIVE_PATH, newpath);
            plan._idsOfIncludedNodes = a[i]._idsOfIncludedNodes;
            plan.addAllNodes(b[j]._idsOfIncludedNodes);
            plan._idsOfIncludedFilters = a[i]._idsOfIncludedFilters;
            plan._idsOfIncludedFilters |= b[j]._idsOfIncludedFilters;
            candidates[getPruningKey(plan, newpath->resultSortedOn())]
                .emplace_back(plan);
            continue;
          }
        }

        // Test for binding the other operation to the right side of the
        // transitive path
        if ((a[i]._qet.get()->getType() ==
                 QueryExecutionTree::OperationType::TRANSITIVE_PATH &&
             jcs[0][0] == 1) ||
            (b[j]._qet.get()->getType() ==
                 QueryExecutionTree::OperationType::TRANSITIVE_PATH &&
             jcs[0][1] == 1)) {
          std::shared_ptr<TransitivePath> srcpath;
          std::shared_ptr<QueryExecutionTree> other;
          size_t otherCol;
          if (a[i]._qet.get()->getType() ==
              QueryExecutionTree::OperationType::TRANSITIVE_PATH) {
            srcpath = std::reinterpret_pointer_cast<TransitivePath>(
                a[i]._qet->getRootOperation());
            other = b[j]._qet;
            otherCol = jcs[0][1];
          } else {
            other = a[i]._qet;
            srcpath = std::reinterpret_pointer_cast<TransitivePath>(
                b[j]._qet->getRootOperation());
            otherCol = jcs[0][0];
          }
          // Do not bind the side of a path twice
          if (!srcpath->isBound() &&
              other->getSizeEstimate() < srcpath->getSizeEstimate()) {
            // The left or right side is a TRANSITIVE_PATH and its join column
            // corresponds to the left side of its input.

            const vector<size_t>& otherSortedOn = other->resultSortedOn();
            if (otherSortedOn.size() == 0 || otherSortedOn[0] != otherCol) {
              if (other->getType() == QueryExecutionTree::SCAN) {
                continue;
              }
              auto sort = std::make_shared<Sort>(_qec, other, jcs[0][0]);
              std::shared_ptr<QueryExecutionTree> sortedOther =
                  std::make_shared<QueryExecutionTree>(_qec);
              sortedOther->setVariableColumns(other->getVariableColumns());
              sortedOther->setContextVars(other->getContextVars());
              sortedOther->setOperation(QueryExecutionTree::SORT, sort);
              other = sortedOther;
            }

            SubtreePlan plan(_qec);
            auto& tree = *plan._qet.get();
            auto newpath = srcpath->bindRightSide(other, otherCol);
            tree.setVariableColumns(newpath->getVariableColumns());
            tree.setOperation(QueryExecutionTree::TRANSITIVE_PATH, newpath);
            plan._idsOfIncludedNodes = a[i]._idsOfIncludedNodes;
            plan.addAllNodes(b[j]._idsOfIncludedNodes);
            plan._idsOfIncludedFilters = a[i]._idsOfIncludedFilters;
            plan._idsOfIncludedFilters |= b[j]._idsOfIncludedFilters;
            candidates[getPruningKey(plan, newpath->resultSortedOn())]
                .emplace_back(plan);
            continue;
          }
        }

        // "NORMAL" CASE:
        // Check if a sub-result has to be re-sorted
        // TODO: replace with HashJoin maybe (or add variant to possible
        // plans).
        auto left = std::make_shared<QueryExecutionTree>(_qec);
        auto right = std::make_shared<QueryExecutionTree>(_qec);
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
          auto sort = std::make_shared<Sort>(_qec, a[i]._qet, jcs[0][0]);
          left->setVariableColumns(a[i]._qet->getVariableColumns());
          left->setContextVars(a[i]._qet->getContextVars());
          left->setOperation(QueryExecutionTree::SORT, sort);
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
          auto sort = std::make_shared<Sort>(_qec, b[j]._qet, jcs[0][1]);
          right->setVariableColumns(b[j]._qet->getVariableColumns());
          right->setContextVars(b[j]._qet.get()->getContextVars());
          right->setOperation(QueryExecutionTree::SORT, sort);
        }

        // TODO(florian): consider replacing this with a multicolumn join.
        // Create the join operation.
        SubtreePlan plan(_qec);
        auto& tree = *plan._qet.get();
        auto join =
            std::make_shared<Join>(_qec, left, right, jcs[0][0], jcs[0][1]);
        tree.setVariableColumns(join->getVariableColumns());
        tree.setContextVars(join->getContextVars());
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
  for (const auto& [key, value] : candidates) {
    (void)key;  // silence unused warning
    size_t minIndex = findCheapestExecutionTree(value);
    prunedPlans.push_back(value[minIndex]);
  }
  LOG(TRACE) << "Got " << prunedPlans.size() << " pruned plans from \n";
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
  auto orderByA = std::make_shared<OrderBy>(_qec, a._qet, sortIndicesA);
  auto orderByB = std::make_shared<OrderBy>(_qec, b._qet, sortIndicesB);

  if (!aSorted) {
    orderByPlanA._qet->setVariableColumns(a._qet->getVariableColumns());
    orderByPlanA._qet->setOperation(QueryExecutionTree::ORDER_BY, orderByA);
  }
  if (!bSorted) {
    orderByPlanB._qet->setVariableColumns(b._qet->getVariableColumns());
    orderByPlanB._qet->setOperation(QueryExecutionTree::ORDER_BY, orderByB);
  }

  auto join = std::make_shared<OptionalJoin>(
      _qec, aSorted ? a._qet : orderByPlanA._qet, a._isOptional,
      bSorted ? b._qet : orderByPlanB._qet, b._isOptional, jcs);
  QueryExecutionTree& tree = *plan._qet.get();
  tree.setVariableColumns(join->getVariableColumns());
  tree.setOperation(QueryExecutionTree::OPTIONAL_JOIN, join);

  plan._isOptional = a._isOptional && b._isOptional;
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
  auto orderByA = std::make_shared<OrderBy>(_qec, a._qet, sortIndicesA);
  auto orderByB = std::make_shared<OrderBy>(_qec, b._qet, sortIndicesB);

  if (!aSorted) {
    orderByPlanA._qet->setVariableColumns(a._qet->getVariableColumns());
    orderByPlanA._qet->setOperation(QueryExecutionTree::ORDER_BY, orderByA);
  }
  if (!bSorted) {
    orderByPlanB._qet->setVariableColumns(b._qet->getVariableColumns());
    orderByPlanB._qet->setOperation(QueryExecutionTree::ORDER_BY, orderByB);
  }

  auto join = std::make_shared<MultiColumnJoin>(
      _qec, aSorted ? a._qet : orderByPlanA._qet,
      bSorted ? b._qet : orderByPlanB._qet, jcs);
  QueryExecutionTree& tree = *plan._qet.get();
  tree.setVariableColumns(join->getVariableColumns());
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
  op->setLhsAsString(filter._lhsAsString);
  if (filter._type == SparqlFilter::REGEX) {
    op->setRegexIgnoreCase(filter._regexIgnoreCase);
  }
  return op;
}

// _____________________________________________________________________________
vector<vector<QueryPlanner::SubtreePlan>> QueryPlanner::fillDpTab(
    const QueryPlanner::TripleGraph& tg, const vector<SparqlFilter>& filters,
    const vector<const QueryPlanner::SubtreePlan*>& children,
    const vector<SparqlValues>& values) {
  size_t numSeeds = tg._nodeMap.size() + children.size() + values.size();
  LOG(TRACE) << "Fill DP table... (there are " << numSeeds
             << " operations to join)" << std::endl;
  vector<vector<SubtreePlan>> dpTab;
  dpTab.emplace_back(seedWithScansAndText(tg, children, values));
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

// _____________________________________________________________________________
void QueryPlanner::setEnablePatternTrick(bool enablePatternTrick) {
  _enablePatternTrick = enablePatternTrick;
}

// _________________________________________________________________________________
size_t QueryPlanner::findCheapestExecutionTree(
    const std::vector<SubtreePlan>& lastRow) const {
  AD_CHECK_GT(lastRow.size(), 0);
  size_t minCost = std::numeric_limits<size_t>::max();
  size_t minInd = 0;
  LOG(INFO) << "\nFinding the cheapest row in the optimizer\n";
  for (size_t i = 0; i < lastRow.size(); ++i) {
    [[maybe_unused]] auto repr = lastRow[i]._qet->asString();
    std::transform(repr.begin(), repr.end(), repr.begin(),
                   [](char c) { return c == '\n' ? ' ' : c; });

    size_t thisCost = lastRow[i].getCostEstimate();
    LOG(INFO) << "Estimated cost of " << thisCost << " for Tree " << repr
              << '\n';
    if (thisCost < minCost) {
      minCost = lastRow[i].getCostEstimate();
      minInd = i;
    }
    // make the tiebreaking deterministic for the UnitTests. The asString
    // should never be on a hot code path in practice.
    else if (thisCost == minCost && isInTestMode() &&
             lastRow[i]._qet->asString() < lastRow[minInd]._qet->asString()) {
      minCost = lastRow[i].getCostEstimate();
      minInd = i;
    }
  }
  LOG(INFO) << "Finished\n";
  return minInd;
};
