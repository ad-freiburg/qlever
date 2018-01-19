// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <algorithm>
#include "./QueryPlanner.h"
#include "IndexScan.h"
#include "Join.h"
#include "Sort.h"
#include "OrderBy.h"
#include "Distinct.h"
#include "Filter.h"
#include "OptionalJoin.h"
#include "TextOperationWithoutFilter.h"
#include "TextOperationWithFilter.h"
#include "TwoColumnJoin.h"

// _____________________________________________________________________________
QueryPlanner::QueryPlanner(QueryExecutionContext* qec) : _qec(qec) {}

// _____________________________________________________________________________
QueryExecutionTree QueryPlanner::createExecutionTree(
    const ParsedQuery& pq) const {


  // create an execution plan for every subtree
  std::vector<const ParsedQuery::GraphPattern*> patternsToProcess;
  patternsToProcess.push_back(&pq._rootGraphPattern);
  std::vector<SubtreePlan> patternPlans;
  for (int i = 0; i < pq._numGraphPatterns; i++) {
    // Using a loop instead of resize as there is no default constructor, and
    // distinct _qet values are needed.
    patternPlans.emplace_back(_qec);
  }
  LOG(DEBUG) << "Got " << patternPlans.size() << " subplans to create."
             << std::endl;

  while (!patternsToProcess.empty()) {
    const ParsedQuery::GraphPattern *pattern = patternsToProcess.back();
    patternsToProcess.pop_back();
    // queue all chlidrens for processing
    patternsToProcess.insert(patternsToProcess.end(),
                             pattern->_children.begin(),
                             pattern->_children.end());

    LOG(DEBUG) << "Creating execution plan.\n";
    TripleGraph tg = createTripleGraph(pattern);

    LOG(TRACE) << "Collapse text cliques..." << std::endl;;
    tg.collapseTextCliques();
    LOG(TRACE) << "Collapse text cliques done." << std::endl;
    vector<vector<SubtreePlan>> finalTab;

    finalTab = fillDpTab(tg, pattern->_filters);

    vector<SubtreePlan>& lastRow = finalTab.back();
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
    patternPlans[pattern->_id] = lastRow[minInd];
  }

  // join the created trees using optional joins on all of their common
  // variables
  // Create an inverse topological ordering of all nodes with children
  std::vector<const ParsedQuery::GraphPattern*> inverseTopo;
  patternsToProcess.push_back(&pq._rootGraphPattern);
  while (!patternsToProcess.empty()) {
    const ParsedQuery::GraphPattern *pattern = patternsToProcess.back();
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
    for (int i = inverseTopo.size() - 1; i >= 0; i--) {
      LOG(DEBUG) << "merging node with children using OptionalJoin."
                 << std::endl;
      const ParsedQuery::GraphPattern *pattern = inverseTopo[i];
      // Init the joins by taking the parent and its first child, then
      // succesively join with the next child.
      // TODO the order of joins can be optimized
      std::vector<SubtreePlan> plans;

      plans.push_back(optionalJoin(patternPlans[pattern->_id],
                                   patternPlans[pattern->_children[0]->_id]));

      for (int j = 1; j < pattern->_children.size(); j++) {
        SubtreePlan &plan1 = plans.back();
        SubtreePlan &plan2 = patternPlans[pattern->_children[j]->_id];
        plans.push_back(optionalJoin(plan1, plan2));
      }
      // Replace the old pattern with the new one that merges all children.
      patternPlans[pattern->_id] = plans.back();
    }
  }
  SubtreePlan final = patternPlans[0];

  // Add global modifiers to the query

  // If there is an order by clause, add another row to the table and
  // just add an order by / sort to every previous result if needed.
  // If the ordering is perfect already, just copy the plan.
  if (pq._orderBy.size() > 0) {
    // TODO (florian) use the optimized version once more for single GraphPattern querys
    // finalTab.emplace_back(getOrderByRow(pq, finalTab));
    SubtreePlan orderByPlan(_qec);
    vector<pair<size_t, bool>> sortIndices;
    for (auto& ord : pq._orderBy) {
      sortIndices.emplace_back(
          pair<size_t, bool>{
              final._qet.get()->getVariableColumn(ord._key),
              ord._desc});
    }
    std::shared_ptr<Operation>
        ob(new OrderBy(_qec, final._qet, sortIndices));
    QueryExecutionTree &tree = *orderByPlan._qet.get();
    tree.setVariableColumns(final._qet.get()->getVariableColumnMap());
    tree.setOperation(QueryExecutionTree::ORDER_BY, ob);
    tree.setContextVars(final._qet.get()->getContextVars());

    final = orderByPlan;
  }
  std::cout << final._qet->asString() << std::endl;

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
        auto ind = final._qet.get()->getVariableColumnMap().find(
            var)->second;
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
          auto ind = final._qet.get()->getVariableColumnMap().find(
              cVar)->second;
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
                  final._qet.get()->resultSortedOn())
        != keepIndices.end()) {
      std::shared_ptr<Operation>
          distinct(new Distinct(_qec, final._qet, keepIndices));
      distinctTree.setOperation(QueryExecutionTree::DISTINCT, distinct);
    } else {
      if (keepIndices.size() == 1) {
        std::shared_ptr<QueryExecutionTree> tree(new QueryExecutionTree(_qec));
        std::shared_ptr<Operation>
            sort(new Sort(_qec, final._qet, keepIndices[0]));
        tree->setVariableColumns(
            final._qet.get()->getVariableColumnMap());
        tree->setOperation(QueryExecutionTree::SORT, sort);
        tree->setContextVars(final._qet.get()->getContextVars());
        std::shared_ptr<Operation>
            distinct(new Distinct(_qec, tree, keepIndices));
        distinctTree.setOperation(QueryExecutionTree::DISTINCT, distinct);
      } else {
        std::shared_ptr<QueryExecutionTree> tree(new QueryExecutionTree(_qec));
        vector<pair<size_t, bool>> obCols;
        for (auto& i : keepIndices) {
          obCols.emplace_back(std::make_pair(i, false));
        }
        std::shared_ptr<Operation>
            ob(new OrderBy(_qec, final._qet, obCols));
        tree->setVariableColumns(
            final._qet.get()->getVariableColumnMap());
        tree->setOperation(QueryExecutionTree::ORDER_BY, ob);
        tree->setContextVars(final._qet.get()->getContextVars());
        std::shared_ptr<Operation>
            distinct(new Distinct(_qec, tree, keepIndices));
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

// _____________________________________________________________________________
vector<QueryPlanner::SubtreePlan> QueryPlanner::getOrderByRow(
    const ParsedQuery& pq,
    const vector<vector<SubtreePlan>>& dpTab) const {
  const vector<SubtreePlan>& previous = dpTab[dpTab.size() - 1];
  vector<SubtreePlan> added;
  added.reserve(previous.size());
  for (size_t i = 0; i < previous.size(); ++i) {
    SubtreePlan plan(_qec);
    auto& tree = *plan._qet.get();
    plan._idsOfIncludedNodes = previous[i]._idsOfIncludedNodes;
    plan._idsOfIncludedFilters = previous[i]._idsOfIncludedFilters;
    if (pq._orderBy.size() == 1 && !pq._orderBy[0]._desc) {
      size_t col = previous[i]._qet.get()->getVariableColumn(
          pq._orderBy[0]._key);
      if (col == previous[i]._qet.get()->resultSortedOn()) {
        // Already sorted perfectly
        added.push_back(previous[i]);
      } else {
        std::shared_ptr<Operation> sort(new Sort(_qec, previous[i]._qet, col));
        tree.setVariableColumns(
            previous[i]._qet.get()->getVariableColumnMap());
        tree.setOperation(QueryExecutionTree::SORT, sort);
        tree.setContextVars(previous[i]._qet.get()->getContextVars());
        added.push_back(plan);
      }
    } else {
      vector<pair<size_t, bool>> sortIndices;
      for (auto& ord : pq._orderBy) {
        sortIndices.emplace_back(
            pair<size_t, bool>{
                previous[i]._qet.get()->getVariableColumn(ord._key),
                ord._desc});
      }
      std::shared_ptr<Operation>
          ob(new OrderBy(_qec, previous[i]._qet, sortIndices));
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
  for (auto& t: pq._rootGraphPattern._whereClauseTriples) {
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
bool QueryPlanner::isWords(const string& elem) {
  return !isVariable(elem) && elem.size() > 0 && elem[0] != '<';
}

// _____________________________________________________________________________
QueryPlanner::TripleGraph QueryPlanner::createTripleGraph(
    const ParsedQuery::GraphPattern *pattern) const {
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
    tg._nodeStorage.emplace_back(
        TripleGraph::Node(tg._nodeStorage.size(), t));
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
    const QueryPlanner::TripleGraph& tg) const {
  vector<SubtreePlan> seeds;
  for (size_t i = 0; i < tg._nodeMap.size(); ++i) {
    const TripleGraph::Node& node = *tg._nodeMap.find(i)->second;
    if (node._cvar.size() > 0) {
      seeds.push_back(getTextLeafPlan(node));
    } else {
      if (node._variables.size() == 0) {
        AD_THROW(ad_semsearch::Exception::BAD_QUERY,
                 "Triples should have at least one variable. Not the case in: "
                 + node._triple.asString());
      }
      if (node._variables.size() == 1) {
        // Just pick one direction, they should be equivalent.
        SubtreePlan plan(_qec);
        plan._idsOfIncludedNodes |= (1 << i);
        auto& tree = *plan._qet.get();
        if (isVariable(node._triple._s)) {
          std::shared_ptr<Operation>
              scan(new IndexScan(_qec, IndexScan::ScanType::POS_BOUND_O));
          static_cast<IndexScan*>(scan.get())->setPredicate(node._triple._p);
          static_cast<IndexScan*>(scan.get())->setObject(node._triple._o);
          tree.setOperation(QueryExecutionTree::OperationType::SCAN,
                            scan);
          tree.setVariableColumn(node._triple._s, 0);
        } else if (isVariable(node._triple._o)) {
          std::shared_ptr<Operation>
              scan(new IndexScan(_qec, IndexScan::ScanType::PSO_BOUND_S));
          static_cast<IndexScan*>(scan.get())->setPredicate(node._triple._p);
          static_cast<IndexScan*>(scan.get())->setSubject(node._triple._s);
          tree.setOperation(QueryExecutionTree::OperationType::SCAN,
                            scan);
          tree.setVariableColumn(node._triple._o, 0);
        } else {
          std::shared_ptr<Operation>
              scan(new IndexScan(_qec, IndexScan::ScanType::SOP_BOUND_O));
          static_cast<IndexScan*>(scan.get())->setSubject(node._triple._s);
          static_cast<IndexScan*>(scan.get())->setObject(node._triple._o);
          tree.setOperation(QueryExecutionTree::OperationType::SCAN,
                            scan);
          tree.setVariableColumn(node._triple._p, 0);
        }
        seeds.push_back(plan);
      } else if (node._variables.size() == 2) {
        // Add plans for both possible scan directions.
        if (!isVariable(node._triple._p)) {
          {
            SubtreePlan plan(_qec);
            plan._idsOfIncludedNodes |= (1 << i);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation>
                scan(new IndexScan(_qec, IndexScan::ScanType::PSO_FREE_S));
            static_cast<IndexScan*>(scan.get())->setPredicate(node._triple._p);
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN,
                              scan);
            tree.setVariableColumn(node._triple._s, 0);
            tree.setVariableColumn(node._triple._o, 1);
            seeds.push_back(plan);
          }
          {
            SubtreePlan plan(_qec);
            plan._idsOfIncludedNodes |= (1 << i);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation>
                scan(new IndexScan(_qec, IndexScan::ScanType::POS_FREE_O));
            static_cast<IndexScan*>(scan.get())->setPredicate(node._triple._p);
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN,
                              scan);
            tree.setVariableColumn(node._triple._o, 0);
            tree.setVariableColumn(node._triple._s, 1);
            seeds.push_back(plan);
          }
        } else if (!isVariable(node._triple._s)) {
          {
            SubtreePlan plan(_qec);
            plan._idsOfIncludedNodes |= (1 << i);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation>
                scan(new IndexScan(_qec, IndexScan::ScanType::SPO_FREE_P));
            static_cast<IndexScan*>(scan.get())->setSubject(node._triple._s);
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN,
                              scan);
            tree.setVariableColumn(node._triple._p, 0);
            tree.setVariableColumn(node._triple._o, 1);
            seeds.push_back(plan);
          }
          {
            SubtreePlan plan(_qec);
            plan._idsOfIncludedNodes |= (1 << i);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation>
                scan(new IndexScan(_qec, IndexScan::ScanType::SOP_FREE_O));
            static_cast<IndexScan*>(scan.get())->setSubject(node._triple._s);
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN,
                              scan);
            tree.setVariableColumn(node._triple._o, 0);
            tree.setVariableColumn(node._triple._p, 1);
            seeds.push_back(plan);
          }
        } else if (!isVariable(node._triple._o)) {
          {
            SubtreePlan plan(_qec);
            plan._idsOfIncludedNodes |= (1 << i);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation>
                scan(new IndexScan(_qec, IndexScan::ScanType::OSP_FREE_S));
            static_cast<IndexScan*>(scan.get())->setObject(node._triple._o);
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN,
                              scan);
            tree.setVariableColumn(node._triple._s, 0);
            tree.setVariableColumn(node._triple._p, 1);
            seeds.push_back(plan);
          }
          {
            SubtreePlan plan(_qec);
            plan._idsOfIncludedNodes |= (1 << i);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation>
                scan(new IndexScan(_qec, IndexScan::ScanType::OPS_FREE_P));
            static_cast<IndexScan*>(scan.get())->setObject(node._triple._o);
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN,
                              scan);
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
            std::shared_ptr<Operation>
                scan(
                new IndexScan(_qec, IndexScan::ScanType::FULL_INDEX_SCAN_SPO));
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN,
                              scan);
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
            std::shared_ptr<Operation>
                scan(
                new IndexScan(_qec, IndexScan::ScanType::FULL_INDEX_SCAN_SOP));
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN,
                              scan);
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
            std::shared_ptr<Operation>
                scan(
                new IndexScan(_qec, IndexScan::ScanType::FULL_INDEX_SCAN_PSO));
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN,
                              scan);
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
            std::shared_ptr<Operation>
                scan(
                new IndexScan(_qec, IndexScan::ScanType::FULL_INDEX_SCAN_POS));
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN,
                              scan);
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
            std::shared_ptr<Operation>
                scan(
                new IndexScan(_qec, IndexScan::ScanType::FULL_INDEX_SCAN_OSP));
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN,
                              scan);
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
            std::shared_ptr<Operation>
                scan(
                new IndexScan(_qec, IndexScan::ScanType::FULL_INDEX_SCAN_OPS));
            static_cast<IndexScan*>(scan.get())->precomputeSizeEstimate();
            tree.setOperation(QueryExecutionTree::OperationType::SCAN,
                              scan);
            tree.setVariableColumn(node._triple._o, 0);
            tree.setVariableColumn(node._triple._p, 1);
            tree.setVariableColumn(node._triple._s, 2);
            seeds.push_back(plan);
          }
        } else {
          AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
                   "With only 2 permutations registered (no -a option), "
                       "triples should have at most two variables. "
                       "Not the case in: "
                   + node._triple.asString());
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
  std::shared_ptr<Operation>
      textOp(new TextOperationWithoutFilter(_qec,
                                            node._wordPart,
                                            node._variables.size() - 1));
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
  LOG(TRACE) << "Considering joins that merge " << a.size()
             << " and " << b.size() << " plans...\n";
  for (size_t i = 0; i < a.size(); ++i) {
    for (size_t j = 0; j < b.size(); ++j) {
      if (connected(a[i], b[j], tg)) {
        // Find join variable(s) / columns.
        auto jcs = getJoinColumns(a[i], b[j]);
        if (jcs.size() > 2) {
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
            std::shared_ptr<QueryExecutionTree>
                left(new QueryExecutionTree(_qec));
            std::shared_ptr<QueryExecutionTree>
                right(new QueryExecutionTree(_qec));
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
              std::shared_ptr<Operation>
                  orderBy(new OrderBy(_qec, a[i]._qet, sortIndices));
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
              std::shared_ptr<Operation>
                  orderBy(new OrderBy(_qec, b[j]._qet, sortIndices));
              right->setVariableColumns(b[j]._qet->getVariableColumnMap());
              right->setOperation(QueryExecutionTree::ORDER_BY, orderBy);
            }
            // Create the join operation.
            SubtreePlan plan(_qec);
            auto& tree = *plan._qet.get();
            std::shared_ptr<Operation>
                join(new TwoColumnJoin(_qec, left, right, jcs));
            tree.setVariableColumns(
                static_cast<TwoColumnJoin*>(join.get())->getVariableColumns());
            tree.setOperation(QueryExecutionTree::TWO_COL_JOIN, join);
            plan._idsOfIncludedFilters = a[i]._idsOfIncludedFilters;
            plan._idsOfIncludedNodes = a[i]._idsOfIncludedNodes;
            plan.addAllNodes(b[j]._idsOfIncludedNodes);
            candidates[getPruningKey(
                plan,
                jcs[c][(0 + swap) % 2])].emplace_back(plan);
          }
          continue;
        }

        // CASE: JOIN ON ONE COLUMN ONLY.
        if (
            (a[i]._qet.get()->getType() ==
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
               b[j]._qet.get()->getSizeEstimate()
               > a[i]._qet.get()->getSizeEstimate())) {
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
              *static_cast<const TextOperationWithoutFilter*>(textPlan._qet->getRootOperation().get());
          std::shared_ptr<Operation>
              textOp(new TextOperationWithFilter(_qec, noFilter.getWordPart(),
                                                 noFilter.getNofVars(),
                                                 filterPlan._qet, otherPlanJc));
          tree.setOperation(
              QueryExecutionTree::OperationType::TEXT_WITH_FILTER,
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
            } else if (
                filterPlan._qet.get()->getVariableColumnMap().count(it->first)
                == 0) {
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
        std::shared_ptr<Operation>
            join(new Join(_qec, left, right, jcs[0][0], jcs[0][1]));
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
QueryPlanner::SubtreePlan QueryPlanner::optionalJoin(const SubtreePlan &a,
                                                     const SubtreePlan &b) const
{
  SubtreePlan plan(_qec);

  std::vector<std::array<Id, 2>> jcs = getJoinColumns(a, b);

  // a and b need to be ordered properly first
  // TODO (florian) only sort if they are not sorted yet.
  vector<pair<size_t, bool>> sortIndicesA;
  vector<pair<size_t, bool>> sortIndicesB;
  for (array<Id, 2> &jc : jcs) {
    sortIndicesA.push_back(std::make_pair(jc[0], false));
    sortIndicesB.push_back(std::make_pair(jc[1], false));
  }

  SubtreePlan orderByPlanA(_qec), orderByPlanB(_qec);

  std::shared_ptr<Operation>
      orderByA(new OrderBy(_qec, a._qet, sortIndicesA));
  std::shared_ptr<Operation>
      orderByB(new OrderBy(_qec, b._qet, sortIndicesB));

  orderByPlanA._qet->setVariableColumns(a._qet->getVariableColumnMap());
  orderByPlanA._qet->setOperation(QueryExecutionTree::ORDER_BY, orderByA);
  orderByPlanB._qet->setVariableColumns(b._qet->getVariableColumnMap());
  orderByPlanB._qet->setOperation(QueryExecutionTree::ORDER_BY, orderByB);

  std::shared_ptr<Operation> join(
        new OptionalJoin(_qec,
                         orderByPlanA._qet,
                         false,
                         orderByPlanB._qet,
                         true, jcs));
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
      os << i << " {TextOP for " << _nodeMap.find(i)->second->_cvar <<
         ", wordPart: \"" << _nodeMap.find(i)->second->_wordPart << "\"} : (";
    }

    for (size_t j = 0; j < _adjLists[i].size(); ++j) {
      os << _adjLists[i][j];
      if (j < _adjLists[i].size() - 1) { os << ", "; }
    }
    os << ')';
    if (i < _adjLists.size() - 1) { os << '\n'; }
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
  if ((a._idsOfIncludedNodes & b._idsOfIncludedNodes) != 0) { return false; }

  for (size_t i = 0; i < 64; ++i) {
    if (((a._idsOfIncludedNodes >> i) & 1) == 0) { continue; }
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
vector<array<size_t, 2>> QueryPlanner::getJoinColumns(
    const QueryPlanner::SubtreePlan& a,
    const QueryPlanner::SubtreePlan& b) const {
  vector<array<size_t, 2>> jcs;
  for (auto it = a._qet.get()->getVariableColumnMap().begin();
       it != a._qet.get()->getVariableColumnMap().end();
       ++it) {
    auto itt = b._qet.get()->getVariableColumnMap().find(it->first);
    if (itt != b._qet.get()->getVariableColumnMap().end()) {
      jcs.push_back(array<size_t, 2>{{it->second, itt->second}});
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
    vector<QueryPlanner::SubtreePlan>& row,
    const vector<SparqlFilter>& filters,
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
  // Fix: Also copy (CHANGE not all plans but TextOperation) without applying the filter.
  // Problem: If the method gets called multiple times, plans with filters
  // May be duplicated. To prevent this, calling code has to ensure
  // That the method is only called once on each row.
  // Similarly this affects the (albeit rare) fact that a filter is directly
  // applicable after a scan of a huge relation where a subsequent
  // join with a small result could be translated into one or more scans
  // directly.
  // This also helps with cases where applying the filter later is better.
  // Finally, the replace flag can be set to enforce that all filters are applied.
  // This should be done for the last row in the DPTab so that no filters are missed.
  for (size_t n = 0; n < row.size(); ++n) {
    if (row[n]._qet->getType() == QueryExecutionTree::SCAN &&
        row[n]._qet->getResultWidth() == 3) {
      // Do not apply filters to dummies!
      continue;
    }
    for (size_t i = 0; i < filters.size(); ++i) {
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
        auto& tree = *newPlan._qet.get();
        if (isVariable(filters[i]._rhs)) {
          std::shared_ptr<Operation>
              filter(new Filter(_qec, row[n]._qet, filters[i]._type,
                                row[n]._qet.get()->getVariableColumn(
                                    filters[i]._lhs),
                                row[n]._qet.get()->getVariableColumn(
                                    filters[i]._rhs)));
          tree.setOperation(QueryExecutionTree::FILTER, filter);
        } else {
          string compWith = filters[i]._rhs;
          Id entityId = 0;
          if (_qec) {
            if (ad_utility::isXsdValue(filters[i]._rhs)) {
              compWith = ad_utility::convertValueLiteralToIndexWord(compWith);
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
            }
          }
          std::shared_ptr<Operation>
              filter(new Filter(_qec, row[n]._qet, filters[i]._type,
                                row[n]._qet.get()->getVariableColumn(
                                    filters[i]._lhs),
                                std::numeric_limits<size_t>::max(),
                                entityId));
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
    const QueryPlanner::TripleGraph& tg,
    const vector<SparqlFilter>& filters) const {
  LOG(TRACE) << "Fill DP table... (there are " << tg._nodeMap.size()
             << " triples to join)" << std::endl;;
  vector<vector<SubtreePlan>> dpTab;
  dpTab.emplace_back(seedWithScansAndText(tg));
  applyFiltersIfPossible(dpTab.back(), filters, tg._nodeMap.size() == 1);

  for (size_t k = 2; k <= tg._nodeMap.size(); ++k) {
    LOG(TRACE) << "Producing plans that unite " << k << " triples."
               << std::endl;;
    dpTab.emplace_back(vector<SubtreePlan>());
    for (size_t i = 1; i * 2 <= k; ++i) {
      auto newPlans = merge(dpTab[i - 1], dpTab[k - i - 1], tg);
      if (newPlans.size() == 0) {
        continue;
      }
      dpTab[k - 1].insert(dpTab[k - 1].end(), newPlans.begin(),
                          newPlans.end());
      applyFiltersIfPossible(dpTab.back(), filters, k == tg._nodeMap.size());
    }
    if (dpTab[k - 1].size() == 0) {
      AD_THROW(ad_semsearch::Exception::BAD_QUERY,
               "Could not find a suitable execution tree. "
                   "Likely cause: Queries that require joins of the full "
                   "index with itself are not supported at the moment.");
    }
  }

  LOG(TRACE) << "Fill DP table done." << std::endl;;
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
          _nodeMap.find(i)->second->_triple._p == INTERNAL_TEXT_MATCH_PREDICATE);
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
        vector<SparqlFilter> filters = pickFilters(origFilters,
                                                   reachableNodes);
        auto recursiveResult = withoutText.splitAtContextVars(filters,
                                                              cTMapNextIteration);
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
          vector<SparqlFilter> filters = pickFilters(origFilters,
                                                     rNodes);
          auto recursiveResult = smallerGraph.splitAtContextVars(
              filters,
              cTMapNextIteration);
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
    size_t startNode,
    ad_utility::HashSet<size_t> leaveOut) const {
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
QueryPlanner::TripleGraph::TripleGraph() :
    _adjLists(), _nodeMap(), _nodeStorage() {

}

// _____________________________________________________________________________
void QueryPlanner::TripleGraph::collapseTextCliques() {
  // TODO: Could use more refactoring.
  // In Earlier versions there were no ql:contains... predicates but
  // a symmetric <in-text> predicate. Therefore some parts are still more
  // complex than need be.


  // Create a map from context var to triples it occurs in (the cliques).
  ad_utility::HashMap<string, vector<size_t>> cvarsToTextNodes(
      identifyTextCliques());
  if (cvarsToTextNodes.size() == 0) { return; }
  // Now turn each such clique into a new node the represents that whole
  // text operation clique.
  size_t id = 0;
  vector<Node> textNodes;
  ad_utility::HashMap<size_t, size_t> removedNodeIds;
  vector<std::set<size_t>> tnAdjSetsToOldIds;
  for (auto it = cvarsToTextNodes.begin();
       it != cvarsToTextNodes.end(); ++it) {
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
    const string& contextVar,
    const string& entityVar,
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
