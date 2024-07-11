//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef CACHEABLEGENERATOR_H
#define CACHEABLEGENERATOR_H

#include <absl/cleanup/cleanup.h>

#include <atomic>
#include <chrono>
#include <optional>
#include <shared_mutex>
#include <thread>
#include <vector>

#include "util/Exception.h"
#include "util/Generator.h"
#include "util/Synchronized.h"
#include "util/UniqueCleanup.h"

namespace ad_utility {

class IteratorExpired : public std::exception {};

template <typename T>
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
    std::atomic<std::thread::id> currentOwningThread{};
    // Returns true if cache needs to shrink, accepts a parameter that tells the
    // callback if we actually can shrink, if the size changed because of a
    // newly generated entry and the entry about to be removed or newly added.
    std::function<bool(bool, bool, std::shared_ptr<const T>)> onSizeChanged_{};
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
      currentOwningThread = std::this_thread::get_id();
      absl::Cleanup cleanup{
          [this]() { currentOwningThread = std::thread::id{}; }};
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
        auto pointer =
            std::make_shared<T>(std::move(*generatorIterator_.value()));
        cachedValues_.push_back(pointer);
        if (onSizeChanged_ && onSizeChanged_(true, true, std::move(pointer))) {
          tryShrinkCache();
        }
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
        std::function<bool(bool, bool, std::shared_ptr<const T>)>
            onSizeChanged) noexcept {
      std::unique_lock lock{mutex_, std::defer_lock};
      if (currentOwningThread != std::this_thread::get_id()) {
        lock.lock();
      }
      onSizeChanged_ = std::move(onSizeChanged);
    }

    std::function<bool(bool, bool, std::shared_ptr<const T>)>
    resetOnSizeChanged() noexcept {
      std::unique_lock lock{mutex_, std::defer_lock};
      if (currentOwningThread != std::this_thread::get_id()) {
        lock.lock();
      }
      return std::move(onSizeChanged_);
    }

    void setOnNextChunkComputed(std::function<void(std::chrono::milliseconds)>
                                    onNextChunkComputed) noexcept {
      std::unique_lock lock{mutex_, std::defer_lock};
      if (currentOwningThread != std::this_thread::get_id()) {
        lock.lock();
      }
      onNextChunkComputed_ = std::move(onNextChunkComputed);
    }

    void tryShrinkCache() {
      size_t maxBound = cachedValues_.size() - 1;
      for (size_t i = 0; i < maxBound; i++) {
        auto& pointer = cachedValues_.at(i);
        if (pointer) {
          auto movedPointer = std::move(pointer);
          if (onSizeChanged_) {
            bool isShrinkable = i < maxBound - 1;
            if (onSizeChanged_(isShrinkable, false, std::move(movedPointer))) {
              AD_CONTRACT_CHECK(isShrinkable);
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
      std::function<bool(bool, bool, std::shared_ptr<const T>)>
          onSizeChanged) noexcept {
    computationStorage_->setOnSizeChanged(std::move(onSizeChanged));
  }

  std::function<bool(bool, bool, std::shared_ptr<const T>)>
  resetOnSizeChanged() {
    return computationStorage_->resetOnSizeChanged();
  }

  void setOnNextChunkComputed(std::function<void(std::chrono::milliseconds)>
                                  onNextChunkComputed) noexcept {
    computationStorage_->setOnNextChunkComputed(std::move(onNextChunkComputed));
  }
};
};  // namespace ad_utility

#endif  // CACHEABLEGENERATOR_H
