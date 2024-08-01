//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <atomic>
#include <chrono>
#include <optional>
#include <shared_mutex>
#include <vector>

#include "util/Exception.h"
#include "util/Generator.h"
#include "util/Synchronized.h"
#include "util/Timer.h"
#include "util/UniqueCleanup.h"

namespace ad_utility {

class IteratorExpired : public std::exception {};

template <typename T>
struct DefaultSizeCounter {
  uint64_t operator()(const std::remove_reference_t<T>&) const { return 1; }
};

template <typename T, InvocableWithExactReturnType<
                          uint64_t, const std::remove_reference_t<T>&>
                          SizeCounter = DefaultSizeCounter<T>>
class CacheableGenerator {
  using GenIterator = typename cppcoro::generator<T>::iterator;

  enum class MasterIteratorState { NOT_STARTED, MASTER_STARTED, MASTER_DONE };

  class ComputationStorage {
    friend CacheableGenerator;
    mutable std::shared_mutex mutex_;
    std::condition_variable_any conditionVariable_;
    cppcoro::generator<T> generator_;
    std::optional<GenIterator> generatorIterator_{};
    std::vector<std::shared_ptr<T>> cachedValues_{};
    MasterIteratorState masterState_ = MasterIteratorState::NOT_STARTED;
    SizeCounter sizeCounter_{};
    std::atomic<uint64_t> currentSize_ = 0;
    uint64_t maxSize_ = std::numeric_limits<uint64_t>::max();
    std::function<void(std::optional<std::chrono::milliseconds>)>
        onSizeChanged_{};

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
        if (!cachedValues_.at(index)) {
          throw IteratorExpired{};
        }
        return;
      }
      if (generatorIterator_.has_value() &&
          generatorIterator_.value() == generator_.end()) {
        return;
      }
      if (masterState_ == MasterIteratorState::MASTER_STARTED && !isMaster) {
        conditionVariable_.wait(lock, [this, index]() {
          return (generatorIterator_.has_value() &&
                  generatorIterator_.value() == generator_.end()) ||
                 index < cachedValues_.size();
        });
        return;
      }
      Timer timer{Timer::Started};
      if (generatorIterator_.has_value()) {
        AD_CONTRACT_CHECK(generatorIterator_.value() != generator_.end());
        ++generatorIterator_.value();
      } else {
        generatorIterator_ = generator_.begin();
      }
      timer.stop();
      if (generatorIterator_.value() != generator_.end()) {
        auto pointer =
            std::make_shared<T>(std::move(*generatorIterator_.value()));
        currentSize_.fetch_add(sizeCounter_(*pointer));
        cachedValues_.push_back(std::move(pointer));
        if (onSizeChanged_) {
          onSizeChanged_(std::chrono::milliseconds{timer.msecs()});
        }
        tryShrinkCacheIfNeccessary();
      }
      if (isMaster) {
        conditionVariable_.notify_all();
      }
    }

    std::shared_ptr<const T> getCachedValue(size_t index) const {
      std::shared_lock lock{mutex_};
      if (!cachedValues_.at(index)) {
        throw IteratorExpired{};
      }
      return cachedValues_.at(index);
    }

    // Needs to be public in order to compile with gcc 11 & 12
   public:
    bool isDone(size_t index) noexcept {
      std::shared_lock lock{mutex_};
      return index >= cachedValues_.size() && generatorIterator_.has_value() &&
             generatorIterator_.value() == generator_.end();
    }

   private:
    void clearMaster() {
      std::unique_lock lock{mutex_};
      AD_CORRECTNESS_CHECK(masterState_ != MasterIteratorState::MASTER_DONE);
      masterState_ = MasterIteratorState::MASTER_DONE;
      lock.unlock();
      conditionVariable_.notify_all();
    }

    void setOnSizeChanged(
        std::function<void(std::optional<std::chrono::milliseconds>)>
            onSizeChanged) noexcept {
      std::unique_lock lock{mutex_};
      onSizeChanged_ = std::move(onSizeChanged);
    }

    void tryShrinkCacheIfNeccessary() {
      if (currentSize_ <= maxSize_) {
        return;
      }
      size_t maxBound = cachedValues_.size() - 1;
      for (size_t i = 0; i < maxBound; i++) {
        auto& pointer = cachedValues_.at(i);
        if (pointer) {
          currentSize_.fetch_add(sizeCounter_(*pointer));
          pointer.reset();
          if (onSizeChanged_) {
            onSizeChanged_(std::nullopt);
          }
          if (currentSize_ <= maxSize_ || i >= maxBound - 1) {
            break;
          }
        }
      }
    }

    void setMaxSize(uint64_t maxSize) {
      std::unique_lock lock{mutex_};
      maxSize_ = maxSize;
    }
  };

  std::shared_ptr<ComputationStorage> computationStorage_;

 public:
  explicit CacheableGenerator(cppcoro::generator<T> generator)
      : computationStorage_{
            std::make_shared<ComputationStorage>(std::move(generator))} {}

  CacheableGenerator(CacheableGenerator&& other) noexcept = default;
  CacheableGenerator(const CacheableGenerator& other) noexcept = delete;
  CacheableGenerator& operator=(CacheableGenerator&& other) noexcept = default;
  CacheableGenerator& operator=(const CacheableGenerator& other) noexcept =
      delete;

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
        : storage_{std::move(storage),
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
      return it.storage()->isDone(it.currentIndex_);
    }

    friend bool operator==(IteratorSentinel s, const Iterator& it) noexcept {
      return (it == s);
    }

    Iterator& operator++() {
      ++currentIndex_;
      storage()->advanceTo(currentIndex_, isMaster_);
      return *this;
    }

    // Need to provide post-increment operator to implement the 'Range' concept.
    void operator++(int) { (void)operator++(); }

    std::shared_ptr<const T> operator*() const {
      return storage()->getCachedValue(currentIndex_);
    }
  };

  Iterator begin(bool isMaster = false) const {
    return Iterator{computationStorage_, isMaster};
  }

  IteratorSentinel end() const noexcept { return IteratorSentinel{}; }

  void setOnSizeChanged(
      std::function<void(std::optional<std::chrono::milliseconds>)>
          onSizeChanged) noexcept {
    computationStorage_->setOnSizeChanged(std::move(onSizeChanged));
  }

  uint64_t getCurrentSize() const {
    return computationStorage_->currentSize_.load();
  }

  void setMaxSize(uint64_t maxSize) {
    computationStorage_->setMaxSize(maxSize);
  }
};
};  // namespace ad_utility
