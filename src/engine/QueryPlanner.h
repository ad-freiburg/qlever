// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#pragma once
#include <set>
#include <vector>

#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "engine/CheckUsePatternTrick.h"
#include "engine/Filter.h"
#include "engine/QueryExecutionTree.h"
#include "parser/ParsedQuery.h"

using std::vector;

class QueryPlanner {
 public:
  explicit QueryPlanner(QueryExecutionContext* qec);

  // Create the best execution tree for the given query according to the
  // optimization algorithm and cost estimates of the QueryPlanner.
  QueryExecutionTree createExecutionTree(ParsedQuery& pq);

  class TripleGraph {
   public:
    TripleGraph();

    TripleGraph(const TripleGraph& other);

    TripleGraph& operator=(const TripleGraph& other);

    TripleGraph(const TripleGraph& other, vector<size_t> keepNodes);

    struct Node {
      Node(size_t id, SparqlTriple t) : id_(id), triple_(std::move(t)) {
        if (isVariable(triple_._s)) {
          _variables.insert(triple_._s.getVariable());
        }
        if (isVariable(triple_._p)) {
          _variables.insert(Variable{triple_._p._iri});
        }
        if (isVariable(triple_._o)) {
          _variables.insert(triple_._o.getVariable());
        }
      }

      Node(size_t id, Variable cvar, std::string word, SparqlTriple t)
          : Node(id, std::move(t)) {
        cvar_ = std::move(cvar);
        wordPart_ = std::move(word);
      }

      Node(const Node& other) = default;

      Node& operator=(const Node& other) = default;

      // Returns true if the two nodes equal apart from the id
      // and the order of variables
      bool isSimilar(const Node& other) const {
        return triple_ == other.triple_ && cvar_ == other.cvar_ &&
               wordPart_ == other.wordPart_ && _variables == other._variables;
      }

      bool isTextNode() const { return cvar_.has_value(); }

      friend std::ostream& operator<<(std::ostream& out, const Node& n) {
        out << "id: " << n.id_ << " triple: " << n.triple_.asString()
            << " vars_ ";
        for (const auto& s : n._variables) {
          out << s.name() << ", ";
        }
        // TODO<joka921> Should the `cvar` and the `wordPart` be stored
        // together?
        if (n.cvar_.has_value()) {
          out << " cvar " << n.cvar_.value().name() << " wordPart "
              << n.wordPart_.value();
        }
        return out;
      }

      size_t id_;
      SparqlTriple triple_;
      ad_utility::HashSet<Variable> _variables;
      std::optional<Variable> cvar_ = std::nullopt;
      std::optional<std::string> wordPart_ = std::nullopt;
    };

    // Allows for manually building triple graphs for testing
    explicit TripleGraph(
        const std::vector<std::pair<Node, std::vector<size_t>>>& init);

    // Checks for id and order independent equality
    bool isSimilar(const TripleGraph& other) const;
    string asString() const;

    bool isTextNode(size_t i) const;

    vector<vector<size_t>> _adjLists;
    ad_utility::HashMap<size_t, Node*> _nodeMap;
    std::list<TripleGraph::Node> _nodeStorage;

    vector<size_t> bfsLeaveOut(size_t startNode,
                               ad_utility::HashSet<size_t> leaveOut) const;

   private:
    vector<std::pair<TripleGraph, vector<SparqlFilter>>> splitAtContextVars(
        const vector<SparqlFilter>& origFilters,
        ad_utility::HashMap<string, vector<size_t>>& contextVarTotextNodes)
        const;

    vector<SparqlFilter> pickFilters(const vector<SparqlFilter>& origFilters,
                                     const vector<size_t>& nodes) const;
  };

  class SubtreePlan {
   public:
    enum Type { BASIC, OPTIONAL, MINUS };

    explicit SubtreePlan(QueryExecutionContext* qec)
        : _qet(std::make_shared<QueryExecutionTree>(qec)) {}

    template <typename Operation>
    SubtreePlan(QueryExecutionContext* qec,
                std::shared_ptr<Operation> operation)
        : _qet{std::make_shared<QueryExecutionTree>(qec,
                                                    std::move(operation))} {}

    std::shared_ptr<QueryExecutionTree> _qet;
    std::shared_ptr<ResultTable> _cachedResult;
    bool _isCached = false;
    uint64_t _idsOfIncludedNodes = 0;
    uint64_t _idsOfIncludedFilters = 0;
    Type type = Type::BASIC;

    size_t getCostEstimate() const;

    size_t getSizeEstimate() const;

    void addAllNodes(uint64_t otherNodes);
  };

  // A helper class to find connected componenents of an RDF query using DFS.
  class QueryGraph {
   private:
    // A simple class to represent a graph node as well as some data for a DFS.
    class Node {
     public:
      const SubtreePlan* plan_;
      ad_utility::HashSet<Node*> adjacentNodes_{};
      // Was this node already visited during DFS.
      bool visited_ = false;
      // Index of the connected component of this node (will be set to a value
      // >= 0 by the DFS.
      int64_t componentIndex_ = -1;
      // Construct from a non-owning pointer.
      explicit Node(const SubtreePlan* plan) : plan_{plan} {}
    };
    // Storage for all the `Node`s that a graph contains.
    std::vector<std::shared_ptr<Node>> nodes_;

    // Default constructor
    QueryGraph() = default;

   public:
    // Return the indices of the connected component for each of the `node`s.
    // The return value will have exactly the same size as `node`s and
    // `result[i]` will be the index of the connected component of `nodes[i]`.
    // The connected components will be contiguous and start at 0.
    static std::vector<size_t> computeConnectedComponents(
        const std::vector<SubtreePlan>& nodes) {
      QueryGraph graph;
      graph.setupGraph(nodes);
      return graph.dfsForAllNodes();
    }

   private:
    // The actual implementation of `setupGraph`. First build a
    // graph from the `leafOperations` and then run DFS and return the result.
    void setupGraph(const std::vector<SubtreePlan>& leafOperations);

    // Run a single DFS startint at the `startNode`. All nodes that are
    // connected to this node (including the node itself) will have
    // `visited_ == true` and  `componentIndex_ == componentIndex` after the
    // call. Only works if `dfs` hasn't been called before on the `startNode` or
    // any node connected to it. (Exceptions to this rule are the recursive
    // calls from `dfs` itself).
    void dfs(Node* startNode, size_t componentIndex);

    // Run `dfs` repeatedly on nodes that were so far undiscovered until all
    // nodes are discovered, which means that all connected components have been
    // identified. Then return the indices of the connected components.
    std::vector<size_t> dfsForAllNodes();
  };

  [[nodiscard]] TripleGraph createTripleGraph(
      const parsedQuery::BasicGraphPattern* pattern) const;

  void addNodeToTripleGraph(const TripleGraph::Node&, TripleGraph&) const;

  void setEnablePatternTrick(bool enablePatternTrick);

  // Create a set of possible execution trees for the given parsed query. The
  // best (cheapest) execution tree according to the QueryPlanner is part of
  // that set. When the query has no `ORDER BY` clause, the set contains one
  // optimal execution tree for each possible ordering (by one column) of the
  // result. This is relevant for subqueries, which are currently optimized
  // independently of the rest of the query, but where it depends on the rest
  // of the query, which ordering of the result is best.
  [[nodiscard]] std::vector<SubtreePlan> createExecutionTrees(ParsedQuery& pq);

 private:
  QueryExecutionContext* _qec;

  // Used to count the number of unique variables created using
  // generateUniqueVarName
  size_t _internalVarCount;

  bool _enablePatternTrick;

  [[nodiscard]] std::vector<QueryPlanner::SubtreePlan> optimize(
      ParsedQuery::GraphPattern* rootPattern);

  // Add all the possible index scans for the triple represented by the node.
  // The triple is "ordinary" in the sense that it is neither a text triple with
  // ql:contains-word nor a special pattern trick triple.
  template <typename PushPlanFunction, typename AddedIndexScanFunction>
  void seedFromOrdinaryTriple(const TripleGraph::Node& node,
                              const PushPlanFunction& pushPlan,
                              const AddedIndexScanFunction& addIndexScan);

  // Helper function used by the seedFromOrdinaryTriple function
  template <typename PushPlanFunction, typename AddedIndexScanFunction>
  void indexScanSingleVarCase(const TripleGraph::Node& node,
                              const PushPlanFunction& pushPlan,
                              const AddedIndexScanFunction& addIndexScan);

  // Helper function used by the seedFromOrdinaryTriple function
  template <typename AddedIndexScanFunction>
  void indexScanTwoVarsCase(const TripleGraph::Node& node,
                            const AddedIndexScanFunction& addIndexScan) const;

  // Helper function used by the seedFromOrdinaryTriple function
  template <typename AddedIndexScanFunction>
  void indexScanThreeVarsCase(const TripleGraph::Node& node,
                              const AddedIndexScanFunction& addIndexScan) const;

  /**
   * @brief Fills children with all operations that are associated with a single
   * node in the triple graph (e.g. IndexScans).
   */
  [[nodiscard]] vector<SubtreePlan> seedWithScansAndText(
      const TripleGraph& tg,
      const vector<vector<QueryPlanner::SubtreePlan>>& children);

  /**
   * @brief Returns a subtree plan that will compute the values for the
   * variables in this single triple. Depending on the triple's PropertyPath
   * this subtree can be arbitrarily large.
   */
  [[nodiscard]] vector<SubtreePlan> seedFromPropertyPathTriple(
      const SparqlTriple& triple);

  /**
   * @brief Returns a parsed query for the property path.
   */
  [[nodiscard]] std::shared_ptr<ParsedQuery::GraphPattern> seedFromPropertyPath(
      const TripleComponent& left, const PropertyPath& path,
      const TripleComponent& right);

  [[nodiscard]] std::shared_ptr<ParsedQuery::GraphPattern> seedFromSequence(
      const TripleComponent& left, const PropertyPath& path,
      const TripleComponent& right);
  [[nodiscard]] std::shared_ptr<ParsedQuery::GraphPattern> seedFromAlternative(
      const TripleComponent& left, const PropertyPath& path,
      const TripleComponent& right);
  [[nodiscard]] std::shared_ptr<ParsedQuery::GraphPattern> seedFromTransitive(
      const TripleComponent& left, const PropertyPath& path,
      const TripleComponent& right, size_t min, size_t max);
  [[nodiscard]] std::shared_ptr<ParsedQuery::GraphPattern> seedFromInverse(
      const TripleComponent& left, const PropertyPath& path,
      const TripleComponent& right);
  [[nodiscard]] std::shared_ptr<ParsedQuery::GraphPattern> seedFromIri(
      const TripleComponent& left, const PropertyPath& path,
      const TripleComponent& right);

  [[nodiscard]] Variable generateUniqueVarName();

  // Creates a tree of unions with the given patterns as the trees leaves
  [[nodiscard]] ParsedQuery::GraphPattern uniteGraphPatterns(
      std::vector<ParsedQuery::GraphPattern>&& patterns) const;

  /**
   * @brief Merges two rows of the dp optimization table using various types of
   * joins.
   * @return A new row for the dp table that contains plans created by joining
   * the result of a plan in a and a plan in b.
   */
  [[nodiscard]] vector<SubtreePlan> merge(const vector<SubtreePlan>& a,
                                          const vector<SubtreePlan>& b,
                                          const TripleGraph& tg) const;

  [[nodiscard]] std::vector<QueryPlanner::SubtreePlan> createJoinCandidates(
      const SubtreePlan& a, const SubtreePlan& b,
      std::optional<TripleGraph> tg) const;

  // Used internally by `createJoinCandidates`. If `a` or `b` is a transitive
  // path operation and the other input can be bound to this transitive path
  // (see `TransitivePath.cpp` for details), then returns that bound transitive
  // path. Else returns `std::nullopt`
  [[nodiscard]] static std::optional<SubtreePlan> createJoinWithTransitivePath(
      SubtreePlan a, SubtreePlan b,
      const std::vector<std::array<ColumnIndex, 2>>& jcs);

  // Used internally by `createJoinCandidates`. If  `a` or `b` is a
  // `HasPredicateScan` with a variable as a subject (`?x ql:has-predicate
  // <VariableOrIri>`) and `a` and `b` can be joined on that subject variable,
  // then returns a `HasPredicateScan` that takes the other input as a subtree.
  // Else returns `std::nullopt`.
  [[nodiscard]] static std::optional<SubtreePlan>
  createJoinWithHasPredicateScan(
      SubtreePlan a, SubtreePlan b,
      const std::vector<std::array<ColumnIndex, 2>>& jcs);

  // Used internally by `createJoinCandidates`. If  `a` or `b` is a
  // `TextOperationWithoutFilter` create a `TextOperationWithFilter` that takes
  // the result of the other input as the filter input. Else return
  // `std::nullopt`.
  [[nodiscard]] static std::optional<SubtreePlan> createJoinAsTextFilter(
      SubtreePlan a, SubtreePlan b,
      const std::vector<std::array<ColumnIndex, 2>>& jcs);

  [[nodiscard]] vector<SubtreePlan> getOrderByRow(
      const ParsedQuery& pq,
      const std::vector<std::vector<SubtreePlan>>& dpTab) const;

  [[nodiscard]] vector<SubtreePlan> getGroupByRow(
      const ParsedQuery& pq,
      const std::vector<std::vector<SubtreePlan>>& dpTab) const;

  [[nodiscard]] vector<SubtreePlan> getDistinctRow(
      const parsedQuery::SelectClause& selectClause,
      const vector<vector<SubtreePlan>>& dpTab) const;

  [[nodiscard]] vector<SubtreePlan> getPatternTrickRow(
      const parsedQuery::SelectClause& selectClause,
      const vector<vector<SubtreePlan>>& dpTab,
      const checkUsePatternTrick::PatternTrickTuple& patternTrickTuple);

  [[nodiscard]] vector<SubtreePlan> getHavingRow(
      const ParsedQuery& pq, const vector<vector<SubtreePlan>>& dpTab) const;

  [[nodiscard]] bool connected(const SubtreePlan& a, const SubtreePlan& b,
                               const TripleGraph& graph) const;

  [[nodiscard]] std::vector<std::array<ColumnIndex, 2>> getJoinColumns(
      const SubtreePlan& a, const SubtreePlan& b) const;

  [[nodiscard]] string getPruningKey(
      const SubtreePlan& plan,
      const vector<ColumnIndex>& orderedOnColumns) const;

  [[nodiscard]] void applyFiltersIfPossible(
      std::vector<SubtreePlan>& row, const std::vector<SparqlFilter>& filters,
      bool replaceInsteadOfAddPlans) const;

  /**
   * @brief Optimize a set of triples, filters and precomputed candidates
   * for child graph patterns
   *
   *
   * Optimize every GraphPattern starting with the leaves of the
   * GraphPattern tree.

   * Strategy:
   * Create a graph.
   * Each triple corresponds to a node, there is an edge between two nodes
   * iff they share a variable.

   * TripleGraph tg = createTripleGraph(&arg);

   * Each node/triple corresponds to a scan (more than one way possible),
   * each edge corresponds to a possible join.

   * Enumerate and judge possible query plans using a DP table.
   * Each ExecutionTree for a sub-problem gives an estimate:
   * There are estimates for cost and size ( and multiplicity per column).
   * Start bottom up, i.e. with the scans for triples.
   * Always merge two solutions from the table by picking one possible
   * join. A join is possible, if there is an edge between the results.
   * Therefore we keep track of all edges that touch a sub-result.
   * When joining two sub-results, the results edges are those that belong
   * to exactly one of the two input sub-trees.
   * If two of them have the same target, only one out edge is created.
   * All edges that are shared by both subtrees, are checked if they are
   * covered by the join or if an extra filter/select is needed.

   * The algorithm then creates all possible plans for 1 to n triples.
   * To generate a plan for k triples, all subsets between i and k-i are
   * joined.

   * Filters are now added to the mix when building execution plans.
   * Without them, a plan has an execution tree and a set of
   * covered triple nodes.
   * With them, it also has a set of covered filters.
   * A filter can be applied as soon as all variables that occur in the
   * filter Are covered by the query. This is also always the place where
   * this is done.

   * Text operations form cliques (all triples connected via the context
   * cvar). Detect them and turn them into nodes with stored word part and
   * edges to connected variables.

   * Each text operation has two ways how it can be used.
   * 1) As leave in the bottom row of the tab.
   * According to the number of connected variables, the operation creates
   * a cross product with n entities that can be used in subsequent joins.
   * 2) as intermediate unary (downwards) nodes in the execution tree.
   * This is a bit similar to sorts: they can be applied after each step
   * and will filter on one variable.
   * Cycles have to be avoided (by previously removing a triple and using
   * it as a filter later on).
   */
  [[nodiscard]] vector<vector<SubtreePlan>> fillDpTab(
      const TripleGraph& graph, const vector<SparqlFilter>& fs,
      const vector<vector<SubtreePlan>>& children);
  std::vector<QueryPlanner::SubtreePlan>

  // Internal subroutine of `fillDpTab` that  only works on a single connected
  // component of the input. Throws if the subtrees in the `connectedComponent`
  // are not in fact connected (via their variables).
  runDynamicProgrammingOnConnectedComponent(
      std::vector<SubtreePlan> connectedComponent,
      const vector<SparqlFilter>& filters, const TripleGraph& tg) const;

  [[nodiscard]] SubtreePlan getTextLeafPlan(
      const TripleGraph::Node& node) const;

  /**
   * @brief return the index of the cheapest execution tree in the argument.
   *
   * If we are in the unit test mode, this is deterministic by additionally
   * sorting by the cache key when comparing equally cheap indices, else the
   * first element that has the minimum index is returned.
   */
  [[nodiscard]] size_t findCheapestExecutionTree(
      const std::vector<SubtreePlan>& lastRow) const;

  /// if this Planner is not associated with a queryExecutionContext we are only
  /// in the unit test mode
  [[nodiscard]] bool isInTestMode() const { return _qec == nullptr; }
};
