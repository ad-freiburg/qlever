// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_ENGINE_TRANSITIVEPATHIMPL_H
#define QLEVER_SRC_ENGINE_TRANSITIVEPATHIMPL_H

#include <utility>

#include "engine/TransitivePathBase.h"
#include "util/Timer.h"

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
};
};  // namespace detail

using IdWithGraphs = absl::InlinedVector<std::pair<Id, Id>, 1>;

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
  using TableColumnWithVocab = detail::TableColumnWithVocab<ql::span<const Id>>;

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

    NodeGenerator hull =
        transitiveHull(edges, sub->getCopyOfLocalVocab(), std::move(nodes),
                       targetSide.value_, yieldOnce);

    auto result = fillTableWithHull(
        std::move(hull), startSide.outputCol_, targetSide.outputCol_, yieldOnce,
        startSide.treeAndCol_.value().first->getResultWidth() - 1);

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
    detail::TableColumnWithVocab<const Set&> tableInfo{std::nullopt, nodes,
                                                       LocalVocab{}};

    NodeGenerator hull =
        transitiveHull(edges, sub->getCopyOfLocalVocab(),
                       ql::span{&tableInfo, 1}, targetSide.value_, yieldOnce);

    auto result = fillTableWithHull(std::move(hull), startSide.outputCol_,
                                    targetSide.outputCol_, yieldOnce);

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
   * @param target Target `TripleComponent`. If it's not a variable, paths that
   * don't end with a matching value are discarded.
   * @param yieldOnce This has to be set to the same value as the consuming
   * code. When set to true, this will prevent yielding the same LocalVocab over
   * and over again to make merging faster (because merging with an empty
   * LocalVocab is a no-op).
   * @return Map Maps each Id to its connected Ids in the transitive hull
   */
  CPP_template(typename Node)(requires ql::ranges::range<Node>) NodeGenerator
      transitiveHull(const T& edges, LocalVocab edgesVocab, Node startNodes,
                     TripleComponent target, bool yieldOnce) const {
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
    for (auto&& tableColumn : startNodes) {
      timer.cont();
      LocalVocab mergedVocab = std::move(tableColumn.vocab_);
      mergedVocab.mergeWith(edgesVocab);
      size_t currentRow = 0;
      for (Id startNode : tableColumn.startNodes_) {
        if (sameVariableOnBothSides) {
          targetId = startNode;
        }
        Set connectedNodes = findConnectedNodes(edges, startNode, targetId);
        if (!connectedNodes.empty()) {
          runtimeInfo().addDetail("Hull time", timer.msecs());
          timer.stop();
          co_yield NodeWithTargets{startNode, std::move(connectedNodes),
                                   mergedVocab.clone(), tableColumn.payload_,
                                   currentRow};
          timer.cont();
          // Reset vocab to prevent merging the same vocab over and over again.
          if (yieldOnce) {
            mergedVocab = LocalVocab{};
          }
        }
        currentRow++;
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
  Set setupNodes(const IdTable& sub, const TransitivePathSide& startSide,
                 const T& edges) const {
    AD_CORRECTNESS_CHECK(minDist_ != 0,
                         "If minDist_ is 0 with a hardcoded side, we should "
                         "call the overload for a bound transitive path.");
    Set result{allocator()};
    // var -> var
    if (startSide.isVariable()) {
      auto col = sub.getColumn(startSide.subCol_);
      result.insert(col.begin(), col.end());
      return result;
    }
    // id -> var|id
    LocalVocab helperVocab;
    Id startId = TripleComponent{startSide.value_}.toValueId(
        getIndex().getVocab(), helperVocab, getIndex().encodedIriManager());
    // Make sure we retrieve the Id from an IndexScan, so we don't have to pass
    // this LocalVocab around. If it's not present then no result needs to be
    // returned anyways.
    const auto& idsWithGraph = edges.getEquivalentIdAndMatchingGraphs(startId);
    // For now we don't support GRAPH yet, but only have it for a faster review
    // cycle.
    AD_CORRECTNESS_CHECK(idsWithGraph.size() <= 1);
    for (const auto& [id, graphId] : idsWithGraph) {
      result.insert(id);
    }
    return result;
  }

  /**
   * @brief Prepare a Map and a nodes vector for the transitive hull
   * computation.
   *
   * @param startSide The TransitivePathSide where the edges start
   * @param startSideResult A `Result` wrapping an `IdTable` containing the Ids
   * for the startSide
   * @return cppcoro::generator<TableColumnWithVocab> An generator for
   * the transitive hull computation
   */
  static ad_utility::InputRangeTypeErased<TableColumnWithVocab> setupNodes(
      const TransitivePathSide& startSide,
      std::shared_ptr<const Result> startSideResult) {
    using namespace ad_utility;

    const auto& [startOperation, joinColumn] = startSide.treeAndCol_.value();
    std::vector<ColumnIndex> columnsWithoutJoinColumn =
        computeColumnsWithoutJoinColumn(joinColumn,
                                        startOperation->getResultWidth());

    auto toView = [columnsWithoutJoinColumn = std::move(
                       columnsWithoutJoinColumn)](const IdTable& idTable) {
      return idTable.asColumnSubsetView(columnsWithoutJoinColumn);
    };
    auto getStartNodes = [joinColumn](const IdTable& idTable) {
      return idTable.getColumn(joinColumn);
    };

    if (startSideResult->isFullyMaterialized()) {
      return InputRangeTypeErased(lazySingleValueRange(
          [toView = std::move(toView), getStartNodes = std::move(getStartNodes),
           startSideResult = std::move(startSideResult)]() {
            const IdTable& idTable = startSideResult->idTable();
            return TableColumnWithVocab{toView(idTable), getStartNodes(idTable),
                                        startSideResult->getCopyOfLocalVocab()};
          }));
    }

    return InputRangeTypeErased(CachingTransformInputRange(
        startSideResult->idTables(),
        // the lambda uses a buffer to ensure the lifetime of the pointer to the
        // idTable, but releases ownership of the localVocab
        [toView = std::move(toView), getStartNodes,
         buf = std::optional<Result::IdTableVocabPair>{std::nullopt}](
            auto& idTableAndVocab) mutable {
          buf = std::move(idTableAndVocab);
          auto& [idTable, localVocab] = buf.value();
          return TableColumnWithVocab{toView(idTable), getStartNodes(idTable),
                                      std::move(localVocab)};
        }));
  }

  virtual T setupEdgesMap(const IdTable& dynSub,
                          const TransitivePathSide& startSide,
                          const TransitivePathSide& targetSide) const = 0;

 private:
  // Helper function to filter the join column to not add it twice to the
  // result.
  static std::vector<ColumnIndex> computeColumnsWithoutJoinColumn(
      ColumnIndex joinColumn, size_t totalColumns) {
    std::vector<ColumnIndex> columnsWithoutJoinColumn;
    AD_CORRECTNESS_CHECK(totalColumns > 0);
    columnsWithoutJoinColumn.reserve(totalColumns - 1);
    ql::ranges::copy(ql::views::iota(static_cast<size_t>(0), totalColumns) |
                         ql::views::filter([joinColumn](size_t i) {
                           return i != joinColumn;
                         }),
                     std::back_inserter(columnsWithoutJoinColumn));
    return columnsWithoutJoinColumn;
  }
};

#endif  // QLEVER_SRC_ENGINE_TRANSITIVEPATHIMPL_H
