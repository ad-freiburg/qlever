//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef REUSABLEGENERATOR_H
#define REUSABLEGENERATOR_H

#include <chrono>
#include <optional>
#include <shared_mutex>
#include <vector>

#include "util/Exception.h"
#include "util/Generator.h"
#include "util/Synchronized.h"
#include "util/UniqueCleanup.h"

namespace ad_utility {

class IteratorExpired : std::exception {};

template <typename T>
class CacheableGenerator {
  using GenIterator = typename cppcoro::generator<T>::iterator;
  using Reference = const T&;
  using Pointer = const T*;

  enum class MasterIteratorState { NOT_STARTED, MASTER_STARTED, MASTER_DONE };

  class Iterator;

  class ComputationStorage {
    friend CacheableGenerator;
    friend Iterator;
    mutable std::shared_mutex mutex_;
    std::condition_variable_any conditionVariable_;
    cppcoro::generator<T> generator_;
    std::optional<GenIterator> generatorIterator_{};
    std::vector<std::optional<T>> cachedValues_{};
    MasterIteratorState masterState_ = MasterIteratorState::NOT_STARTED;
    // Returns true if cache needs to shrink, accepts a parameter that tells the
    // callback if we actually can shrink
    std::function<bool(bool)> onSizeChanged_{};
    std::function<void(bool)> onGeneratorFinished_{};
    std::function<void(std::chrono::milliseconds)> onNextChunkComputed_{};

   public:
    explicit ComputationStorage(cppcoro::generator<T> generator)
        : generator_{std::move(generator)} {}
    ComputationStorage(ComputationStorage&& other) = delete;
    ComputationStorage(const ComputationStorage& other) = delete;
    ComputationStorage& operator=(ComputationStorage&& other) = delete;
    ComputationStorage& operator=(const ComputationStorage& other) = delete;

   private:
    void advanceTo(size_t index, bool isMaster) {
      std::unique_lock lock{mutex_};
      AD_CONTRACT_CHECK(index <= cachedValues_.size());
      // Make sure master iterator does exist and we're not blocking
      // indefinitely
      if (isMaster) {
        AD_CORRECTNESS_CHECK(masterState_ != MasterIteratorState::MASTER_DONE);
        masterState_ = MasterIteratorState::MASTER_STARTED;
      } else {
        AD_CORRECTNESS_CHECK(masterState_ != MasterIteratorState::NOT_STARTED);
      }
      if (index < cachedValues_.size()) {
        if (!cachedValues_.at(index).has_value()) {
          throw IteratorExpired{};
        }
        return;
      }
      if (masterState_ == MasterIteratorState::MASTER_STARTED) {
        if (!isMaster) {
          conditionVariable_.wait(lock, [this, index]() {
            return (generatorIterator_.has_value() &&
                    generatorIterator_.value() == generator_.end()) ||
                   index < cachedValues_.size();
          });
          return;
        }
      }
      auto start = std::chrono::steady_clock::now();
      if (generatorIterator_.has_value()) {
        AD_CONTRACT_CHECK(generatorIterator_.value() != generator_.end());
        ++generatorIterator_.value();
      } else {
        generatorIterator_ = generator_.begin();
      }
      auto stop = std::chrono::steady_clock::now();
      if (onNextChunkComputed_) {
        onNextChunkComputed_(
            std::chrono::duration_cast<std::chrono::milliseconds>(stop -
                                                                  start));
      }
      if (generatorIterator_.value() != generator_.end()) {
        cachedValues_.emplace_back(std::move(*generatorIterator_.value()));
        if (onSizeChanged_) {
          if (onSizeChanged_(true)) {
            tryShrinkCache();
          }
        }
      } else if (onGeneratorFinished_) {
        onGeneratorFinished_(cachedValues_.empty() ||
                             cachedValues_.at(0).has_value());
      }
      if (isMaster) {
        conditionVariable_.notify_all();
      }
    }

    Reference getCachedValue(size_t index) const {
      std::shared_lock lock{mutex_};
      if (!cachedValues_.at(index).has_value()) {
        throw IteratorExpired{};
      }
      return cachedValues_.at(index).value();
    }

    bool isDone(size_t index) noexcept {
      std::shared_lock lock{mutex_};
      return index == cachedValues_.size() && generatorIterator_.has_value() &&
             generatorIterator_.value() == generator_.end();
    }

    void clearMaster() {
      std::unique_lock lock{mutex_};
      AD_CORRECTNESS_CHECK(masterState_ != MasterIteratorState::MASTER_DONE);
      masterState_ = MasterIteratorState::MASTER_DONE;
      lock.unlock();
      conditionVariable_.notify_all();
    }

    void setOnSizeChanged(std::function<bool(bool)> onSizeChanged) {
      std::lock_guard lock{mutex_};
      onSizeChanged_ = std::move(onSizeChanged);
    }

    void setOnGeneratorFinished(std::function<void(bool)> onGeneratorFinished) {
      std::lock_guard lock{mutex_};
      onGeneratorFinished_ = std::move(onGeneratorFinished);
    }

    void setOnNextChunkComputed(
        std::function<void(std::chrono::milliseconds)> onNextChunkComputed) {
      std::lock_guard lock{mutex_};
      onNextChunkComputed_ = std::move(onNextChunkComputed);
    }

    void forEachCachedValue(
        const std::invocable<const T&> auto& function) const {
      std::shared_lock lock{mutex_};
      for (const auto& optional : cachedValues_) {
        if (optional.has_value()) {
          function(optional.value());
        }
      }
    }

    void tryShrinkCache() {
      size_t maxBound = cachedValues_.size() - 1;
      for (size_t i = 0; i < maxBound; i++) {
        if (cachedValues_.at(i).has_value()) {
          cachedValues_.at(i).reset();
          if (onSizeChanged_) {
            bool isShrinkable = i < maxBound - 1;
            if (onSizeChanged_(isShrinkable)) {
              AD_CONTRACT_CHECK(!isShrinkable);
            } else {
              break;
            }
          }
        }
      }
    }
  };
  std::shared_ptr<ComputationStorage> computationStorage_;

 public:
  explicit CacheableGenerator(cppcoro::generator<T> generator)
      : computationStorage_{
            std::make_shared<ComputationStorage>(std::move(generator))} {}

  CacheableGenerator(CacheableGenerator&& other) = default;
  CacheableGenerator(const CacheableGenerator& other) = delete;
  CacheableGenerator& operator=(CacheableGenerator&& other) = default;
  CacheableGenerator& operator=(const CacheableGenerator& other) = delete;

  class IteratorSentinel {};

  class Iterator {
    size_t currentIndex_ = 0;
    unique_cleanup::UniqueCleanup<std::weak_ptr<ComputationStorage>> storage_;
    bool isMaster_;

    auto storage() const {
      auto pointer = storage_->lock();
      AD_CORRECTNESS_CHECK(pointer);
      return pointer;
    }

   public:
    explicit Iterator(std::weak_ptr<ComputationStorage> storage, bool isMaster)
        : storage_{storage,
                   [isMaster](auto&& storage) {
                     if (isMaster) {
                       auto pointer = storage.lock();
                       AD_CORRECTNESS_CHECK(pointer);
                       pointer->clearMaster();
                     }
                   }},
          isMaster_{isMaster} {
      this->storage()->advanceTo(currentIndex_, isMaster);
    }

    friend bool operator==(const Iterator& it, IteratorSentinel) noexcept {
      return !it.storage()->isDone(it.currentIndex_);
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
      storage()->advanceTo(currentIndex_, isMaster_);
      return *this;
    }

    // Need to provide post-increment operator to implement the 'Range' concept.
    void operator++(int) { (void)operator++(); }

    Reference operator*() const noexcept {
      return storage()->getCachedValue(currentIndex_);
    }

    Pointer operator->() const noexcept { return std::addressof(operator*()); }
  };

  Iterator begin(bool isMaster = false) const noexcept {
    return Iterator{computationStorage_, isMaster};
  }

  IteratorSentinel end() const noexcept { return IteratorSentinel{}; }

  cppcoro::generator<T> extractGenerator() && {
    std::unique_lock lock{computationStorage_->mutex_};
    cppcoro::generator<T> result{std::move(computationStorage_->generator_)};
    computationStorage_.reset();
    return result;
  }

  void forEachCachedValue(const std::invocable<const T&> auto& function) const {
    computationStorage_->forEachCachedValue(function);
  }

  void setOnSizeChanged(std::function<bool(bool)> onSizeChanged) {
    computationStorage_->setOnSizeChanged(std::move(onSizeChanged));
  }

  void setOnGeneratorFinished(std::function<void(bool)> onGeneratorFinished) {
    computationStorage_->setOnGeneratorFinished(std::move(onGeneratorFinished));
  }

  void setOnNextChunkComputed(
      std::function<void(std::chrono::milliseconds)> onNextChunkComputed) {
    computationStorage_->setOnNextChunkComputed(std::move(onNextChunkComputed));
  }
};
};  // namespace ad_utility

#endif  // REUSABLEGENERATOR_H
