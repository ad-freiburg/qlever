//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <memory>

namespace ad_utility {
template <typename T>
class LinkedList;

template <typename T>
class Node {
  using type = std::remove_reference_t<T>;
  friend LinkedList<T>;
  type payload_;
  std::shared_ptr<Node> next_;

 public:
  explicit Node(type payload) : payload_{std::move(payload)}, next_{nullptr} {}
  [[nodiscard]] constexpr const type& getPayload() const noexcept {
    return payload_;
  }
  [[nodiscard]] bool hasNext() const { return next_ != nullptr; }
  [[nodiscard]] std::shared_ptr<Node> next() const { return next_; }
};

template <typename T>
class LinkedList {
 private:
  using type = std::remove_reference_t<T>;
  std::shared_ptr<Node<type>> head_{};
  std::shared_ptr<Node<type>> tail_{};

  LinkedList(std::shared_ptr<Node<type>> initial)
      : head_{initial}, tail_{initial} {}

 public:
  explicit LinkedList(type initial)
      : LinkedList{std::make_shared<Node<type>>(std::move(initial))} {}

  void append(type payload) {
    auto newNode = std::make_shared<Node<type>>(std::move(payload));
    if (head_ == nullptr) {
      head_ = newNode;
    }
    if (tail_ != nullptr) {
      tail_->next_ = newNode;
    }
    tail_ = newNode;
  }

  std::shared_ptr<Node<type>> getHead() { return head_; }
};
}  // namespace ad_utility
