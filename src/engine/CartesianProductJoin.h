//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_CARTESIANPRODUCTJOIN_H
#define QLEVER_CARTESIANPRODUCTJOIN_H

#include "engine/Operation.h"

class CartesianProductJoin : public Operation {
 public:
  using Children = std::vector<std::shared_ptr<QueryExecutionTree>>;

 private:
  Children children_;
  auto childView() {
    return std::views::transform(children_, [](auto& child) -> Operation& {
      return *child->getRootOperation();
    });
  }
  auto childView() const {
    return std::views::transform(children_,
                                 [](auto& child) -> const Operation& {
                                   return *child->getRootOperation();
                                 });
  }

 public:
  // Constructor
  explicit CartesianProductJoin(QueryExecutionContext* executionContext,
                                Children children)
      : Operation{executionContext}, children_{std::move(children)} {
    AD_CONTRACT_CHECK(!children_.empty());
    AD_CONTRACT_CHECK(std::ranges::all_of(
        children_, [](auto& child) { return child != nullptr; }));
  }

  /// get non-owning pointers to all the held subtrees to actually use the
  /// Execution Trees as trees
  std::vector<QueryExecutionTree*> getChildren() override {
    std::vector<QueryExecutionTree*> result;
    result.reserve(children_.size());
    for (auto& child : children_) {
      result.push_back(child.get());
    }
    return result;
  }

 private:
  // The individual implementation of `asString` (see above) that has to be
  // customized by every child class.
  string asStringImpl(size_t indent) const override {
    return "CARTESIAN PRODUCT JOIN " +
           ad_utility::lazyStrJoin(
               std::views::transform(
                   childView(),
                   [indent](auto& child) { return child.asString(indent); }),
               " ");
  }

 public:
  // Gets a very short (one line without line ending) descriptor string for
  // this Operation.  This string is used in the RuntimeInformation
  string getDescriptor() const override { return "Cartesian Product Join"; }
  size_t getResultWidth() const override {
    auto view = childView() | std::views::transform(&Operation::getResultWidth);
    return std::accumulate(view.begin(), view.end(), 0ul, std::plus{});
  }

  size_t getCostEstimate() override {
    return getSizeEstimate();
    // TODO<joka921> Add the costs of the children.
  }

 private:
  uint64_t getSizeEstimateBeforeLimit() override {
    auto view =
        childView() | std::views::transform(&Operation::getSizeEstimate);
    return std::accumulate(view.begin(), view.end(), 1ul, std::multiplies{});
  }

  VariableToColumnMap computeVariableToColumnMap() const override;

 public:
  float getMultiplicity([[maybe_unused]] size_t col) override {
    // Deliberately a dummy as we always perform this operation last.
    return 1;
  }
  bool knownEmptyResult() override {
    return std::ranges::any_of(children_, [](auto& child) {
      return child->getRootOperation()->knownEmptyResult();
    });
  }

  [[nodiscard]] bool supportsLimit() const override { return true; }

 protected:
  vector<ColumnIndex> resultSortedOn() const override { return {}; }

 private:
  //! Compute the result of the query-subtree rooted at this element..
  ResultTable computeResult() override;
};

#endif  // QLEVER_CARTESIANPRODUCTJOIN_H
