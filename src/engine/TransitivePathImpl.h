// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#pragma once

#include <utility>

#include "engine/CallFixedSize.h"
#include "engine/TransitivePathBase.h"
#include "util/Exception.h"
#include "util/Timer.h"

namespace detail {
template <typename ColumnType>
struct TableColumnWithVocab {
  const IdTable* table_;
  ColumnType column_;
  LocalVocab vocab_;

  // Explicit to prevent issues with co_yield and lifetime.
  // See https://gcc.gnu.org/bugzilla/show_bug.cgi?id=103909 for more info.
  TableColumnWithVocab(const IdTable* table, ColumnType column,
                       LocalVocab vocab)
      : table_{table}, column_{std::move(column)}, vocab_{std::move(vocab)} {};
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
  using TableColumnWithVocab =
      detail::TableColumnWithVocab<std::span<const Id>>;

 public:
  TransitivePathImpl(QueryExecutionContext* qec,
                     std::shared_ptr<QueryExecutionTree> child,
                     TransitivePathSide leftSide, TransitivePathSide rightSide,
                     size_t minDist, size_t maxDist)
      : TransitivePathBase(qec, std::move(child), std::move(leftSide),
                           std::move(rightSide), minDist, maxDist) {};

  /**
   * @brief Compute the transitive hull with a bound side.
   * This function is called when the startSide is bound and
   * it is a variable. The other IdTable contains the result
   * of the start side and will be used to get the start nodes.
   *
   * @param dynSub The IdTable for the sub result
   * @param startSide The start side for the transitive hull
   * @param targetSide The target side for the transitive hull
   * @param startSideResult The Result of the startSide
   */
  Result::Generator computeTransitivePathBound(
      std::shared_ptr<const Result> sub, const TransitivePathSide& startSide,
      const TransitivePathSide& targetSide,
      std::shared_ptr<const Result> startSideResult, bool yieldOnce) const {
    auto timer = ad_utility::Timer(ad_utility::Timer::Stopped);
    timer.start();

    auto edges = setupEdgesMap(sub->idTable(), startSide, targetSide);
    auto nodes = setupNodes(startSide, std::move(startSideResult));

    timer.stop();
    auto initTime = timer.msecs();
    timer.start();

    NodeGenerator hull =
        transitiveHull(edges, sub->getCopyOfLocalVocab(), std::move(nodes),
                       targetSide.isVariable()
                           ? std::nullopt
                           : std::optional{std::get<Id>(targetSide.value_)});

    timer.stop();
    auto hullTime = timer.msecs();
    timer.start();

    auto result = fillTableWithHull(
        std::move(hull), startSide.outputCol_, targetSide.outputCol_,
        startSide.treeAndCol_.value().second, yieldOnce,
        startSide.treeAndCol_.value().first->getResultWidth());

    // Iterate over generator to prevent lifetime issues
    for (auto& pair : result) {
      co_yield pair;
    }

    timer.stop();
    auto fillTime = timer.msecs();

    auto& info = runtimeInfo();
    info.addDetail("Initialization time", initTime.count());
    info.addDetail("Hull time", hullTime.count());
    info.addDetail("IdTable fill time", fillTime.count());
  };

  /**
   * @brief Compute the transitive hull.
   * This function is called when no side is bound (or an id).
   *
   * @param dynSub The IdTable for the sub result
   * @param startSide The start side for the transitive hull
   * @param targetSide The target side for the transitive hull
   */

  Result::Generator computeTransitivePath(std::shared_ptr<const Result> sub,
                                          const TransitivePathSide& startSide,
                                          const TransitivePathSide& targetSide,
                                          bool yieldOnce) const {
    auto timer = ad_utility::Timer(ad_utility::Timer::Stopped);
    timer.start();

    auto edges = setupEdgesMap(sub->idTable(), startSide, targetSide);
    auto nodesWithDuplicates =
        setupNodes(sub->idTable(), startSide, targetSide);
    Set nodesWithoutDuplicates{allocator()};
    for (const auto& span : nodesWithDuplicates) {
      nodesWithoutDuplicates.insert(span.begin(), span.end());
    }

    timer.stop();
    auto initTime = timer.msecs();
    timer.start();

    // Technically we should pass the localVocab of `sub` here, but this will
    // just lead to a merge with itself later on in the pipeline.
    detail::TableColumnWithVocab<const Set&> tableInfo{
        nullptr, nodesWithoutDuplicates, LocalVocab{}};

    NodeGenerator hull = transitiveHull(
        edges, sub->getCopyOfLocalVocab(), std::span{&tableInfo, 1},
        targetSide.isVariable()
            ? std::nullopt
            : std::optional{std::get<Id>(targetSide.value_)});

    timer.stop();
    auto hullTime = timer.msecs();
    timer.start();

    auto result = fillTableWithHull(std::move(hull), startSide.outputCol_,
                                    targetSide.outputCol_, yieldOnce);

    // Iterate over generator to prevent lifetime issues
    for (auto& pair : result) {
      co_yield pair;
    }

    timer.stop();
    auto fillTime = timer.msecs();

    auto& info = runtimeInfo();
    info.addDetail("Initialization time", initTime.count());
    info.addDetail("Hull time", hullTime.count());
    info.addDetail("IdTable fill time", fillTime.count());
  };

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
  ProtoResult computeResult(bool requestLaziness) override {
    if (minDist_ == 0 && !isBoundOrId() && lhs_.isVariable() &&
        rhs_.isVariable()) {
      AD_THROW(
          "This query might have to evaluate the empty path, which is "
          "currently "
          "not supported");
    }
    auto [startSide, targetSide] = decideDirection();
    std::shared_ptr<const Result> subRes = subtree_->getResult();

    if (startSide.isBoundVariable()) {
      std::shared_ptr<const Result> sideRes =
          startSide.treeAndCol_.value().first->getResult(true);

      auto gen =
          computeTransitivePathBound(std::move(subRes), startSide, targetSide,
                                     std::move(sideRes), !requestLaziness);

      return requestLaziness
                 ? ProtoResult{std::move(gen), resultSortedOn()}
                 : ProtoResult{cppcoro::getSingleElement(std::move(gen)),
                               resultSortedOn()};
    }
    auto gen = computeTransitivePath(std::move(subRes), startSide, targetSide,
                                     !requestLaziness);

    // NOTE: The only place, where the input to a transitive path operation is
    // not an index scan (which has an empty local vocabulary by default) is the
    // `LocalVocabTest`. But it doesn't harm to propagate the local vocab here
    // either.
    return requestLaziness
               ? ProtoResult{std::move(gen), resultSortedOn()}
               : ProtoResult{cppcoro::getSingleElement(std::move(gen)),
                             resultSortedOn()};
  };

  /**
   * @brief Compute the transitive hull starting at the given nodes,
   * using the given Map.
   *
   * @param edges Adjacency lists, mapping Ids (nodes) to their connected
   * Ids.
   * @param nodes A list of Ids. These Ids are used as starting points for the
   * transitive hull. Thus, this parameter guides the performance of this
   * algorithm.
   * @param target Optional target Id. If supplied, only paths which end
   * in this Id are added to the hull.
   * @return Map Maps each Id to its connected Ids in the transitive hull
   */
  NodeGenerator transitiveHull(const T& edges, LocalVocab edgesVocab,
                               std::ranges::range auto startNodes,
                               std::optional<Id> target) const {
    std::vector<std::pair<Id, size_t>> stack;
    ad_utility::HashSetWithMemoryLimit<Id> marks{
        getExecutionContext()->getAllocator()};
    for (auto&& tableColumn : startNodes) {
      LocalVocab mergedVocab = std::move(tableColumn.vocab_);
      mergedVocab.mergeWith(std::span{&edgesVocab, 1});
      for (Id startNode : tableColumn.column_) {
        Set connectedNodes{getExecutionContext()->getAllocator()};
        marks.clear();
        stack.clear();
        stack.push_back({startNode, 0});

        if (minDist_ == 0 &&
            (!target.has_value() || startNode == target.value())) {
          connectedNodes.insert(startNode);
        }

        while (!stack.empty()) {
          checkCancellation();
          auto [node, steps] = stack.back();
          stack.pop_back();

          if (steps <= maxDist_ && marks.count(node) == 0) {
            if (steps >= minDist_) {
              marks.insert(node);
              if (!target.has_value() || node == target.value()) {
                connectedNodes.insert(node);
              }
            }

            const auto& successors = edges.successors(node);
            for (auto successor : successors) {
              stack.push_back({successor, steps + 1});
            }
          }
        }
        if (!marks.empty()) {
          co_yield NodeWithTargets{startNode, std::move(connectedNodes),
                                   mergedVocab.clone(), tableColumn.table_};
        }
      }
    }
  }

  /**
   * @brief Prepare a Map and a nodes vector for the transitive hull
   * computation.
   *
   * @param sub The sub table result
   * @param startSide The TransitivePathSide where the edges start
   * @param targetSide The TransitivePathSide where the edges end
   * @return std::vector<std::span<const Id>> An Id vector (nodes) for the
   * transitive hull computation
   */
  std::vector<std::span<const Id>> setupNodes(
      const IdTable& sub, const TransitivePathSide& startSide,
      const TransitivePathSide& targetSide) const {
    std::vector<std::span<const Id>> result;

    // id -> var|id
    if (!startSide.isVariable()) {
      result.emplace_back(&std::get<Id>(startSide.value_), 1);
      // var -> var
    } else {
      std::span<const Id> startNodes = sub.getColumn(startSide.subCol_);
      result.emplace_back(startNodes);
      if (minDist_ == 0) {
        std::span<const Id> targetNodes = sub.getColumn(targetSide.subCol_);
        result.emplace_back(targetNodes);
      }
    }

    return result;
  };

  /**
   * @brief Prepare a Map and a nodes vector for the transitive hull
   * computation.
   *
   * @param startSide The TransitivePathSide where the edges start
   * @param startSideTable An IdTable containing the Ids for the startSide
   * @return cppcoro::generator<TableColumnWithVocab> An Id vector (nodes) for
   * the transitive hull computation
   */
  cppcoro::generator<TableColumnWithVocab> setupNodes(
      const TransitivePathSide& startSide,
      std::shared_ptr<const Result> startSideResult) const {
    if (startSideResult->isFullyMaterialized()) {
      // Bound -> var|id
      std::span<const Id> startNodes = startSideResult->idTable().getColumn(
          startSide.treeAndCol_.value().second);
      co_yield TableColumnWithVocab{&startSideResult->idTable(), startNodes,
                                    startSideResult->getCopyOfLocalVocab()};
    } else {
      for (auto& [idTable, localVocab] : startSideResult->idTables()) {
        // Bound -> var|id
        std::span<const Id> startNodes =
            idTable.getColumn(startSide.treeAndCol_.value().second);
        co_yield TableColumnWithVocab{&idTable, startNodes,
                                      std::move(localVocab)};
      }
    }
  };

  virtual T setupEdgesMap(const IdTable& dynSub,
                          const TransitivePathSide& startSide,
                          const TransitivePathSide& targetSide) const = 0;
};
