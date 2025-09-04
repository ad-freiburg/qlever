// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_ENGINE_TRANSITIVEPATHIMPL_H
#define QLEVER_SRC_ENGINE_TRANSITIVEPATHIMPL_H

#include <utility>

#include "engine/TransitivePathBase.h"
#include "util/Iterators.h"
#include "util/Timer.h"

using IdWithGraphs = absl::InlinedVector<std::pair<Id, Id>, 1>;

namespace detail {

// Helper struct that allows to group a read-only view of a column of a table
// with a reference to the table itself and a local vocabulary (used to ensure
// the correct lifetime).
template <typename ColumnType>
struct TableColumnWithVocab {
  PayloadTable payload_;
  ColumnType startNodes_;
  LocalVocab vocab_;

  // Explicit to prevent issues with co_yield and lifetime.
  // See https://gcc.gnu.org/bugzilla/show_bug.cgi?id=103909 for more info.
  TableColumnWithVocab(std::optional<IdTableView<0>> payload,
                       ColumnType startNodes, LocalVocab vocab)
      : payload_{std::move(payload)},
        startNodes_{std::move(startNodes)},
        vocab_{std::move(vocab)} {}

  // Return a range substituting undefined values with all corresponding values
  // from `edges`. If `tuple` doesn't contain any undefined values, then
  // the range returned by this function just returns the tuple itself.
  static decltype(auto) expandUndef(const auto& tuple,
                                    [[maybe_unused]] const auto& edges,
                                    bool checkGraph) {
    // This is the unbound case, e.g. `?x wdt:P279+ ?y` where the left side of
    // the P279+ is guaranteed to be defined.
    if constexpr (std::is_same_v<ColumnType, SetWithGraph>) {
      return ql::views::single(tuple);
    } else {
      const auto& [startId, graphId] = tuple;
      if (startId.isUndefined() || (checkGraph && graphId.isUndefined()))
          [[unlikely]] {
        return edges.getEquivalentIdAndMatchingGraphs(startId);
      } else {
        return IdWithGraphs{tuple};
      }
    }
  }
};
};  // namespace detail

/**
 * @class TransitivePathImpl
 * @brief This class implements common functions for the concrete TransitivePath
 * classes TransitivePathBinSearch and TransitivePathHashMap. The template can
 * be set to a map data structure which is used for the transitive hull
 * computation.
 *
 * @tparam T A map data structure for the transitive hull computation.
 */
template <typename T>
class TransitivePathImpl : public TransitivePathBase {
  // Tuple-like class
  using ZippedType = ql::ranges::range_value_t<
      ::ranges::zip_view<ql::span<const Id>, ::ranges::repeat_view<Id>>>;
  using TableColumnWithVocab = detail::TableColumnWithVocab<
      ad_utility::InputRangeTypeErased<ZippedType>>;

 public:
  using TransitivePathBase::TransitivePathBase;

  /**
   * @brief Compute the transitive hull with a bound side.
   * This function is called when the startSide is bound and
   * it is a variable. The other IdTable contains the result
   * of the start side and will be used to get the start nodes.
   *
   * @param sub A shared pointer to the sub result. Needs to be kept alive for
   * the lifetime of this generator.
   * @param startSide The start side for the transitive hull
   * @param targetSide The target side for the transitive hull
   * @param startSideResult The Result of the startSide
   * @param yieldOnce If true, the generator will yield only a single time.
   */
  Result::Generator computeTransitivePathBound(
      std::shared_ptr<const Result> sub, const TransitivePathSide& startSide,
      const TransitivePathSide& targetSide,
      std::shared_ptr<const Result> startSideResult, bool yieldOnce) const {
    ad_utility::Timer timer{ad_utility::Timer::Started};

    auto edges = setupEdgesMap(sub->idTable(), startSide, targetSide);
    auto nodes = setupNodes(startSide, std::move(startSideResult));
    // Setup nodes returns a generator, so this time measurement won't include
    // the time for each iteration, but every iteration step should have
    // constant overhead, which should be safe to ignore.
    runtimeInfo().addDetail("Initialization time", timer.msecs());

    NodeGenerator hull = transitiveHull(
        std::move(edges), sub->getCopyOfLocalVocab(), std::move(nodes),
        startSide.value_, targetSide.value_, yieldOnce);

    const auto& [tree, joinColumn] = startSide.treeAndCol_.value();
    size_t numberOfPayloadColumns =
        tree->getResultWidth() - numJoinColumnsWith(tree, joinColumn);
    auto result = fillTableWithHull(std::move(hull), startSide.outputCol_,
                                    targetSide.outputCol_, yieldOnce,
                                    numberOfPayloadColumns);

    // Iterate over generator to prevent lifetime issues
    for (auto& pair : result) {
      co_yield pair;
    }
  };

  /**
   * @brief Compute the transitive hull.
   * This function is called when no side is bound (or an id).
   *
   * @param sub A shared pointer to the sub result. Needs to be kept alive for
   * the lifetime of this generator.
   * @param startSide The start side for the transitive hull
   * @param targetSide The target side for the transitive hull
   * @param yieldOnce If true, the generator will yield only a single time.
   */

  Result::Generator computeTransitivePath(std::shared_ptr<const Result> sub,
                                          const TransitivePathSide& startSide,
                                          const TransitivePathSide& targetSide,
                                          bool yieldOnce) const {
    ad_utility::Timer timer{ad_utility::Timer::Started};

    auto edges = setupEdgesMap(sub->idTable(), startSide, targetSide);
    auto nodes = setupNodes(sub->idTable(), startSide, edges);

    runtimeInfo().addDetail("Initialization time", timer.msecs());

    // Technically we should pass the localVocab of `sub` here, but this will
    // just lead to a merge with itself later on in the pipeline.
    detail::TableColumnWithVocab<const decltype(nodes)&> tableInfo{
        std::nullopt, nodes, LocalVocab{}};

    NodeGenerator hull = transitiveHull(
        std::move(edges), sub->getCopyOfLocalVocab(), ql::span{&tableInfo, 1},
        startSide.value_, targetSide.value_, yieldOnce);

    // We don't pass a payload table, so our `inputWidth` is 0.
    auto result = fillTableWithHull(std::move(hull), startSide.outputCol_,
                                    targetSide.outputCol_, yieldOnce, 0);

    // Iterate over generator to prevent lifetime issues
    for (auto& pair : result) {
      co_yield pair;
    }
  }

 protected:
  /**
   * @brief Compute the result for this TransitivePath operation
   * This function chooses the start and target side for the transitive
   * hull computation. This choice of the start side has a large impact
   * on the time it takes to compute the hull. The set of nodes on the
   * start side should be as small as possible.
   *
   * @return Result The result of the TransitivePath operation
   */
  Result computeResult(bool requestLaziness) override {
    auto [startSide, targetSide] = decideDirection();
    // In order to traverse the graph represented by this result, we need random
    // access across the whole table, so it doesn't make sense to lazily compute
    // the result.
    std::shared_ptr<const Result> subRes = subtree_->getResult(false);

    if (startSide.isBoundVariable()) {
      std::shared_ptr<const Result> sideRes =
          startSide.treeAndCol_.value().first->getResult(true);

      auto gen =
          computeTransitivePathBound(std::move(subRes), startSide, targetSide,
                                     std::move(sideRes), !requestLaziness);

      return requestLaziness ? Result{std::move(gen), resultSortedOn()}
                             : Result{cppcoro::getSingleElement(std::move(gen)),
                                      resultSortedOn()};
    }
    auto gen = computeTransitivePath(std::move(subRes), startSide, targetSide,
                                     !requestLaziness);
    return requestLaziness ? Result{std::move(gen), resultSortedOn()}
                           : Result{cppcoro::getSingleElement(std::move(gen)),
                                    resultSortedOn()};
  }

  /**
   * @brief Depth-first search to find connected nodes in the graph.
   * @param edges The adjacency lists, mapping Ids (nodes) to their connected
   * Ids.
   * @param startNode The node to start the search from.
   * @param target Optional target Id. If supplied, only paths which end in this
   * Id are added to the result.
   * @return A set of connected nodes in the graph.
   */
  Set findConnectedNodes(const T& edges, Id startNode,
                         const std::optional<Id>& target) const {
    std::vector<std::pair<Id, size_t>> stack;
    ad_utility::HashSetWithMemoryLimit<Id> marks{allocator()};
    Set connectedNodes{allocator()};
    stack.emplace_back(startNode, 0);

    while (!stack.empty()) {
      checkCancellation();
      auto [node, steps] = stack.back();
      stack.pop_back();

      if (steps <= maxDist_ && !marks.contains(node)) {
        if (steps >= minDist_) {
          marks.insert(node);
          if (!target.has_value() || node == target.value()) {
            connectedNodes.insert(node);
          }
        }

        const auto& successors = edges.successors(node);
        for (auto successor : successors) {
          stack.emplace_back(successor, steps + 1);
        }
      }
    }
    return connectedNodes;
  }

  /**
   * @brief Compute the transitive hull starting at the given nodes,
   * using the given Map.
   *
   * @param edges Adjacency lists, mapping Ids (nodes) to their connected
   * Ids.
   * @param edgesVocab The `LocalVocab` holding the vocabulary of the edges.
   * @param startNodes A range that yields an instantiation of
   * `TableColumnWithVocab` that can be consumed to create a transitive hull.
   * @param start Start `TripleComponent`. If it's a variable, and the same as
   * an optional graph variable, values where these values are not equal are
   * skipped.
   * @param target Target `TripleComponent`. If it's not a variable, paths that
   * don't end with a matching value are discarded.
   * @param yieldOnce This has to be set to the same value as the consuming
   * code. When set to true, this will prevent yielding the same LocalVocab over
   * and over again to make merging faster (because merging with an empty
   * LocalVocab is a no-op).
   * @return Map Maps each Id to its connected Ids in the transitive hull
   */
  CPP_template(typename Node)(requires ql::ranges::range<Node>) NodeGenerator
      transitiveHull(T edges, LocalVocab edgesVocab, Node startNodes,
                     TripleComponent start, TripleComponent target,
                     bool yieldOnce) const {
    ad_utility::Timer timer{ad_utility::Timer::Stopped};
    // `targetId` is only ever used for comparisons, and never stored in the
    // result, so we use a separate local vocabulary.
    LocalVocab targetHelper;
    const auto& index = getIndex();
    std::optional<Id> targetId =
        target.isVariable()
            ? std::nullopt
            : std::optional{std::move(target).toValueId(
                  index.getVocab(), targetHelper, index.encodedIriManager())};
    bool sameVariableOnBothSides =
        !targetId.has_value() && lhs_.value_ == rhs_.value_;
    bool endsWithGraphVariable =
        !targetId.has_value() && graphVariable_ == target.getVariable();
    bool startsWithGraphVariable =
        start.isVariable() && graphVariable_ == start.getVariable();
    for (auto&& tableColumn : startNodes) {
      timer.cont();
      LocalVocab mergedVocab = std::move(tableColumn.vocab_);
      mergedVocab.mergeWith(edgesVocab);
      for (const auto& [currentRow, pair] :
           ::ranges::views::enumerate(tableColumn.startNodes_)) {
        for (const auto& [startNode, graphId] :
             tableColumn.expandUndef(pair, edges, graphVariable_.has_value())) {
          // Skip generation of values for `SELECT * { GRAPH ?g { ?g a* ?x } }`
          // where both `?g` variables are not the same.
          if (startsWithGraphVariable && startNode != graphId) {
            continue;
          }
          if (sameVariableOnBothSides) {
            targetId = startNode;
          } else if (endsWithGraphVariable) {
            targetId = graphId;
          }
          edges.setGraphId(graphId);
          Set connectedNodes = findConnectedNodes(edges, startNode, targetId);
          if (!connectedNodes.empty()) {
            runtimeInfo().addDetail("Hull time", timer.msecs());
            timer.stop();
            co_yield NodeWithTargets{startNode,
                                     graphId,
                                     std::move(connectedNodes),
                                     mergedVocab.clone(),
                                     tableColumn.payload_,
                                     static_cast<size_t>(currentRow)};
            timer.cont();
            // Reset vocab to prevent merging the same vocab over and over
            // again.
            if (yieldOnce) {
              mergedVocab = LocalVocab{};
            }
          }
        }
      }
      timer.stop();
    }
  }

  /**
   * @brief Prepare a Map and a nodes vector for the transitive hull
   * computation.
   *
   * @param sub The sub table result
   * @param startSide The TransitivePathSide where the edges start
   * @param edges Templated datastructure representing the edges of the graph
   * @return Set A set of starting nodes for the transitive hull computation
   */
  SetWithGraph setupNodes(const IdTable& sub,
                          const TransitivePathSide& startSide,
                          const T& edges) const {
    AD_CORRECTNESS_CHECK(minDist_ != 0,
                         "If minDist_ is 0 with a hardcoded side, we should "
                         "call the overload for a bound transitive path.");
    SetWithGraph result{allocator()};
    // var -> var
    if (startSide.isVariable()) {
      if (graphVariable_.has_value()) {
        for (const auto& [id, graph] : ::ranges::views::zip(
                 sub.getColumn(startSide.subCol_),
                 sub.getColumn(
                     subtree_->getVariableColumn(graphVariable_.value())))) {
          result.emplace(id, graph);
        }
      } else {
        for (Id id : sub.getColumn(startSide.subCol_)) {
          result.emplace(id, Id::makeUndefined());
        }
      }
      return result;
    }
    // id -> var|id
    LocalVocab helperVocab;
    Id startId = TripleComponent{startSide.value_}.toValueId(
        getIndex().getVocab(), helperVocab, getIndex().encodedIriManager());
    // Make sure we retrieve the Id from an IndexScan, so we don't have to pass
    // this LocalVocab around. If it's not present then no result needs to be
    // returned anyways. This also augments the id with matching graph ids.
    auto idAndGraphs = edges.getEquivalentIdAndMatchingGraphs(startId);
    result.insert(idAndGraphs.begin(), idAndGraphs.end());
    return result;
  }

  /**
   * @brief Prepare a Map and a nodes vector for the transitive hull
   * computation.
   *
   * @param startSide The TransitivePathSide where the edges start
   * @param startSideResult A `Result` wrapping an `IdTable` containing the Ids
   * for the startSide
   * @return ad_utility::InputRangeTypeErased<TableColumnWithVocab> A range for
   * the transitive hull computation
   */
  ad_utility::InputRangeTypeErased<TableColumnWithVocab> setupNodes(
      const TransitivePathSide& startSide,
      std::shared_ptr<const Result> startSideResult) const {
    using namespace ad_utility;
    const auto& [tree, joinColumn] = startSide.treeAndCol_.value();
    size_t cols = tree->getResultWidth();
    std::optional<ColumnIndex> graphColumn = getActualGraphColumnIndex(tree);
    std::vector<ColumnIndex> columnsWithoutJoinColumns =
        computeColumnsWithoutJoinColumns(joinColumn, cols, graphColumn);
    auto columnsToRange = [graphColumn = std::move(graphColumn),
                           joinColumn](const auto& idTable) {
      ql::span<const Id> startNodes = idTable.getColumn(joinColumn);
      return graphColumn.has_value()
                 ? InputRangeTypeErased{zipColumns(
                       startNodes, idTable.getColumn(graphColumn.value()))}
                 : InputRangeTypeErased{padWithMissingGraph(startNodes)};
    };

    auto toView = [columnsWithoutJoinColumns = std::move(
                       columnsWithoutJoinColumns)](const IdTable& idTable) {
      return idTable.asColumnSubsetView(columnsWithoutJoinColumns);
    };

    if (startSideResult->isFullyMaterialized()) {
      return InputRangeTypeErased(lazySingleValueRange(
          [toView = std::move(toView),
           columnsToRange = std::move(columnsToRange),
           startSideResult = std::move(startSideResult)]() {
            const IdTable& idTable = startSideResult->idTable();
            return TableColumnWithVocab{toView(idTable),
                                        columnsToRange(idTable),
                                        startSideResult->getCopyOfLocalVocab()};
          }));
    }

    return InputRangeTypeErased(CachingTransformInputRange(
        startSideResult->idTables(),
        // the lambda uses a buffer to ensure the lifetime of the pointer to
        // the idTable, but releases ownership of the localVocab
        [toView = std::move(toView), columnsToRange = std::move(columnsToRange),
         buf = std::optional<Result::IdTableVocabPair>{std::nullopt}](
            auto& idTableAndVocab) mutable {
          buf = std::move(idTableAndVocab);
          auto& [idTable, localVocab] = buf.value();
          return TableColumnWithVocab{toView(idTable), columnsToRange(idTable),
                                      std::move(localVocab)};
        }));
  }

  virtual T setupEdgesMap(const IdTable& dynSub,
                          const TransitivePathSide& startSide,
                          const TransitivePathSide& targetSide) const = 0;

 private:
  // Helper function to filter the join column to not add it twice to the
  // result.
  static std::vector<ColumnIndex> computeColumnsWithoutJoinColumns(
      ColumnIndex joinColumn, size_t totalColumns,
      std::optional<ColumnIndex> graphColumn) {
    std::vector<ColumnIndex> columnsWithoutJoinColumn;
    uint8_t graphPadding = graphColumn.has_value() && joinColumn != graphColumn;
    AD_CORRECTNESS_CHECK(totalColumns > graphPadding);
    columnsWithoutJoinColumn.reserve(totalColumns - graphPadding - 1);
    ql::ranges::copy(
        ql::views::iota(static_cast<size_t>(0), totalColumns) |
            ql::views::filter([joinColumn, &graphColumn](size_t i) {
              return i != joinColumn && i != graphColumn;
            }),
        std::back_inserter(columnsWithoutJoinColumn));
    return columnsWithoutJoinColumn;
  }

  // Create a zipped view that returns `Id::makeUndefined()` for the graph
  // column.
  static auto padWithMissingGraph(ql::span<const Id> input) {
    return ::ranges::views::zip(input,
                                ::ranges::views::repeat(Id::makeUndefined()));
  }

  // Create a zipped view from two columns.
  static auto zipColumns(ql::span<const Id> input,
                         ql::span<const Id> graphInput) {
    return ::ranges::views::zip(input, graphInput);
  }
};

#endif  // QLEVER_SRC_ENGINE_TRANSITIVEPATHIMPL_H
