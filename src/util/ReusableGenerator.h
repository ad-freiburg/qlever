//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef REUSABLEGENERATOR_H
#define REUSABLEGENERATOR_H

#include <optional>
#include <vector>

#include "util/Exception.h"
#include "util/Generator.h"
#include "util/Synchronized.h"

namespace ad_utility {

template <typename T>
class ReusableGenerator {
  using GenIterator = typename cppcoro::generator<T>::iterator;
  using Reference = typename GenIterator::reference;
  using Pointer = typename GenIterator::pointer;

  class ComputationStorage {
    friend ReusableGenerator;
    cppcoro::generator<T> generator_;
    std::optional<GenIterator> generatorIterator_{};
    std::vector<T> cachedValues_{};

    explicit ComputationStorage(cppcoro::generator<T> generator)
        : generator_{std::move(generator)} {}

   public:
    ComputationStorage(ComputationStorage&& other) = default;
    ComputationStorage(const ComputationStorage& other) = delete;
    ComputationStorage& operator=(ComputationStorage&& other) = default;
    ComputationStorage& operator=(const ComputationStorage& other) = delete;

   private:
    void advanceTo(size_t index) {
      AD_CONTRACT_CHECK(index <= cachedValues_.size());
      if (index != cachedValues_.size()) {
        return;
      }
      if (generatorIterator_.has_value()) {
        AD_CONTRACT_CHECK(generatorIterator_.value() != generator_.end());
        ++generatorIterator_.value();
      } else {
        generatorIterator_ = generator_.begin();
      }
      if (generatorIterator_.value() != generator_.end()) {
        cachedValues_.emplace_back(std::move(*generatorIterator_));
      }
    }

    Reference getCachedValue(size_t index) { return cachedValues_.at(index); }

    bool isDone(size_t index) const noexcept {
      return index == cachedValues_.size() && generatorIterator_.has_value() &&
             generatorIterator_.value() == generator_.end();
    }
  };
  std::shared_ptr<Synchronized<ComputationStorage>> computationStorage_;

 public:
  explicit ReusableGenerator(cppcoro::generator<T> generator)
      : computationStorage_{std::make_shared<Synchronized<ComputationStorage>>(
            ComputationStorage{std::move(generator)})} {}

  ReusableGenerator(ReusableGenerator&& other) = default;
  ReusableGenerator(const ReusableGenerator& other) = delete;
  ReusableGenerator& operator=(ReusableGenerator&& other) = default;
  ReusableGenerator& operator=(const ReusableGenerator& other) = delete;

  class IteratorSentinel {};

  class Iterator {
    size_t currentIndex_ = 0;
    std::weak_ptr<Synchronized<ComputationStorage>> storage_;

    explicit Iterator(std::weak_ptr<Synchronized<ComputationStorage>> storage)
        : storage_{storage} {
      storage_->advanceTo(currentIndex_);
    }

    friend bool operator==(const Iterator& it, IteratorSentinel) noexcept {
      return !it.storage_.lock()->isDone(it.currentIndex_);
    }

    friend bool operator!=(const Iterator& it, IteratorSentinel s) noexcept {
      return !(it == s);
    }

    friend bool operator==(IteratorSentinel s, const Iterator& it) noexcept {
      return (it == s);
    }

    friend bool operator!=(IteratorSentinel s, const Iterator& it) noexcept {
      return it != s;
    }

    Iterator& operator++() {
      ++currentIndex_;
      storage_.lock()->advanceTo(currentIndex_);
      return *this;
    }

    // Need to provide post-increment operator to implement the 'Range' concept.
    void operator++(int) { (void)operator++(); }

    Reference operator*() const noexcept {
      return storage_.lock()->getCachedValue(currentIndex_);
    }

    Pointer operator->() const noexcept { return std::addressof(operator*()); }
  };

  Iterator begin() noexcept { return Iterator{}; }

  IteratorSentinel end() const noexcept { return IteratorSentinel{}; }

  cppcoro::generator<T> extractGenerator() && {
    auto lock = computationStorage_->wlock();
    cppcoro::generator<T> result{std::move(lock->generator_)};
    computationStorage_.reset();
    return result;
  }
};
};  // namespace ad_utility

#endif  // REUSABLEGENERATOR_H
