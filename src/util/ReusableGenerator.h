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
#include "util/UniqueCleanup.h"

namespace ad_utility {

class IteratorExpired : std::exception {};

// TODO<RobinTF> Plans for this class: Rename this class to cache-aware
// generator or something. Introduce the concept of an "owner" of a generator
// which bounds generation to a maximum storage size, throwing exceptions
// if a non-owning iterator is consuming too slow, and blocking if non-owning
// iterators are too fast. Ownership can expire if the "owning iterator" is
// destroyed. It clears cached values after itself. It needs to be able to hold
// a callback to be called whenever the stored size changes. Also when the
// generator is completely consumed and when the maximum cache size would be
// exceeded if no elements were deleted at the front of the cache to make sure
// this entry is evicted from the cache.

template <typename T>
class ReusableGenerator {
  using GenIterator = typename cppcoro::generator<T>::iterator;
  using Reference = const T&;
  using Pointer = const T*;

  class ComputationStorage {
    friend ReusableGenerator;
    cppcoro::generator<T> generator_;
    std::optional<GenIterator> generatorIterator_{};
    std::vector<std::optional<T>> cachedValues_{};
    // TODO<RobinTF> make sure we error out when a non-master iterator is
    // consumed before the initial master iterator
    bool masterExists_ = true;
    std::function<bool()> onSizeChanged_{};
    std::function<void(bool)> onGeneratorFinished_{};

    explicit ComputationStorage(cppcoro::generator<T> generator)
        : generator_{std::move(generator)} {}

   public:
    ComputationStorage(ComputationStorage&& other) = default;
    ComputationStorage(const ComputationStorage& other) = delete;
    ComputationStorage& operator=(ComputationStorage&& other) = default;
    ComputationStorage& operator=(const ComputationStorage& other) = delete;

   private:
    void advanceTo(size_t index, bool isMaster) {
      AD_CONTRACT_CHECK(index <= cachedValues_.size());
      if (index != cachedValues_.size()) {
        if (!cachedValues_.at(index).has_value()) {
          throw IteratorExpired{};
        }
        return;
      }
      if (masterExists_) {
        if (isMaster) {
          // TODO wake up condition variable
        } else {
          // TODO wait for condition variable
        }
      }
      if (generatorIterator_.has_value()) {
        AD_CONTRACT_CHECK(generatorIterator_.value() != generator_.end());
        ++generatorIterator_.value();
      } else {
        generatorIterator_ = generator_.begin();
      }
      if (generatorIterator_.value() != generator_.end()) {
        cachedValues_.emplace_back(std::move(*generatorIterator_.value()));
        // False on onSizeChange means the value got too big.
        if (onSizeChanged_ && !onSizeChanged_()) {
          for (size_t i = 0; i < cachedValues_.size() - 1; i++) {
            if (cachedValues_.at(i).has_value()) {
              cachedValues_.at(i).reset();
              if (onSizeChanged_()) {
                break;
              }
            }
          }
        }
      } else if (onGeneratorFinished_) {
        onGeneratorFinished_(cachedValues_.empty() ||
                             cachedValues_.at(0).has_value());
      }
    }

    Reference getCachedValue(size_t index) const {
      if (!cachedValues_.at(index).has_value()) {
        throw IteratorExpired{};
      }
      return cachedValues_.at(index).value();
    }

    bool isDone(size_t index) noexcept {
      return index == cachedValues_.size() && generatorIterator_.has_value() &&
             generatorIterator_.value() == generator_.end();
    }

    void clearMaster() {
      AD_CORRECTNESS_CHECK(masterExists_);
      masterExists_ = false;
      // TODO wake up condition variable
    }

    void setOnSizeChanged(std::function<bool()> onSizeChanged) {
      onSizeChanged_ = std::move(onSizeChanged);
    }

    void setOnGeneratorFinished(std::function<void(bool)> onGeneratorFinished) {
      onGeneratorFinished_ = std::move(onGeneratorFinished);
    }

    void forEachCachedValue(
        const std::invocable<const T&> auto& function) const {
      for (const auto& optional : cachedValues_) {
        if (optional.has_value()) {
          function(optional.value());
        }
      }
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
    unique_cleanup::UniqueCleanup<
        std::weak_ptr<Synchronized<ComputationStorage>>>
        storage_;
    bool isMaster_;

   public:
    explicit Iterator(std::weak_ptr<Synchronized<ComputationStorage>> storage,
                      bool isMaster)
        : storage_{storage,
                   [isMaster](auto&& storage) {
                     if (isMaster) {
                       storage.lock()->wlock()->clearMaster();
                     }
                   }},
          isMaster_{isMaster} {
      storage_->lock()->wlock()->advanceTo(currentIndex_, isMaster);
    }

    friend bool operator==(const Iterator& it, IteratorSentinel) noexcept {
      return !it.storage_->lock()->wlock()->isDone(it.currentIndex_);
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
      storage_->lock()->wlock()->advanceTo(currentIndex_, isMaster_);
      return *this;
    }

    // Need to provide post-increment operator to implement the 'Range' concept.
    void operator++(int) { (void)operator++(); }

    Reference operator*() const noexcept {
      return storage_->lock()->rlock()->getCachedValue(currentIndex_);
    }

    Pointer operator->() const noexcept { return std::addressof(operator*()); }
  };

  Iterator begin(bool isMaster = false) const noexcept {
    return Iterator{computationStorage_, isMaster};
  }

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
