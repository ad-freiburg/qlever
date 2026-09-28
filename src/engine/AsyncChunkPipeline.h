// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_ASYNCCHUNKPIPELINE_H
#define QLEVER_SRC_ENGINE_ASYNCCHUNKPIPELINE_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>

#include "util/Exception.h"
#include "util/Generator.h"
#include "util/Log.h"

namespace qlever::export_pipeline {

namespace detail {
// C++17-compatible detection of a `.size()` member function. A raw
// `requires`-expression is C++20-only, but this header is also compiled with
// `CMAKE_CXX_STANDARD_MANUALLY_OVERRIDDEN=17`.
template <typename T, typename = void>
struct HasSize : std::false_type {};
template <typename T>
struct HasSize<T, std::void_t<decltype(std::declval<const T&>().size())>>
    : std::true_type {};
template <typename T>
inline constexpr bool HasSize_v = HasSize<T>::value;
}  // namespace detail

// _____________________________________________________________________________
// Diagnostic accounting and performance metrics for the double-buffering
// pipeline. Encapsulates all bookkeeping so callers need not track state.
struct PipelineStats {
  size_t totalChunksProduced{0};
  size_t totalChunksConsumed{0};
  size_t totalBytesProduced{0};
  size_t totalBytesConsumed{0};
  size_t backpressureStalls{0};
  size_t consumerWaitStalls{0};
};

// Forward declaration of ChunkSink for high-level producer callbacks.
template <typename ChunkType>
class ChunkSink;

// _____________________________________________________________________________
// Deep Module: Asynchronous Double-Buffered Chunk Pipeline.
//
// Solves the lockstep execution bottleneck during export streaming:
// In lockstep streaming, the CPU generates chunk k, waits while chunk k is
// transmitted over the network socket, and only then starts generating chunk
// k+1. This leaves the CPU idle during network transmission and the network
// idle during chunk generation.
//
// `AsyncChunkPipeline` decouples generation and transmission via a bounded
// 2-slot ring buffer:
//   - Slot 1: Active chunk currently being consumed / sent over the network.
//   - Slot 2: Background worker simultaneously evaluating, decompressing, and
//             formatting the subsequent chunk.
//
// Safe Backpressure: If network transmission is slower than chunk generation,
// the producer suspends once capacity (default 2 slots) is reached, preventing
// unbounded memory accumulation.
//
// Invariants & Error Handling:
//   - Bounded invariant: buffer size never exceeds capacity.
//   - Exceptions thrown on the background thread are safely captured and
//     re-thrown on the consumer thread upon `pop()` / generator iteration.
//   - Early cancellation triggers an immediate unblocking signal, allowing
//     background threads to terminate cleanly without thread or resource leaks.
template <typename ChunkType = std::string>
class AsyncChunkPipeline {
 public:
  // ___________________________________________________________________________
  // Precondition: `capacity >= 1`. Default is 2 (double buffering).
  explicit AsyncChunkPipeline(size_t capacity = 2) : capacity_{capacity} {
    AD_CONTRACT_CHECK(capacity_ >= 1);
  }

  // Non-copyable, non-movable to ensure synchronized thread-safety.
  AsyncChunkPipeline(const AsyncChunkPipeline&) = delete;
  AsyncChunkPipeline& operator=(const AsyncChunkPipeline&) = delete;
  AsyncChunkPipeline(AsyncChunkPipeline&&) = delete;
  AsyncChunkPipeline& operator=(AsyncChunkPipeline&&) = delete;

  // ___________________________________________________________________________
  // Destructor cancels the pipeline and wakes any blocked threads.
  ~AsyncChunkPipeline() { cancel(); }

  // ___________________________________________________________________________
  // Producer API: Push a newly generated chunk into the pipeline.
  // Blocks if buffer is full (backpressure) until a slot is freed.
  // Returns true on success; returns false if pipeline is cancelled.
  bool push(ChunkType chunk) {
    std::unique_lock<std::mutex> lock(mutex_);

    if (isCancelled_) {
      return false;
    }

    // Apply backpressure if buffer reached capacity.
    if (buffer_.size() >= capacity_) {
      stats_.backpressureStalls++;
      cvNotFull_.wait(lock, [this]() {
        return buffer_.size() < capacity_ || isCancelled_;
      });
      if (isCancelled_) {
        return false;
      }
    }

    if constexpr (detail::HasSize_v<ChunkType>) {
      stats_.totalBytesProduced += chunk.size();
    }
    stats_.totalChunksProduced++;
    buffer_.push(std::move(chunk));

    cvNotEmpty_.notify_one();
    return true;
  }

  // ___________________________________________________________________________
  // Producer API: Signal that all chunks have been generated.
  void finish() {
    std::lock_guard<std::mutex> lock(mutex_);
    isFinished_ = true;
    cvNotEmpty_.notify_all();
    cvNotFull_.notify_all();
  }

  // ___________________________________________________________________________
  // Producer API: Record an exception caught during generation.
  // The captured exception will be rethrown when consumer calls `pop()`.
  void setException(std::exception_ptr exceptionPtr) {
    std::lock_guard<std::mutex> lock(mutex_);
    exception_ = std::move(exceptionPtr);
    isFinished_ = true;
    cvNotEmpty_.notify_all();
    cvNotFull_.notify_all();
  }

  // ___________________________________________________________________________
  // Consumer API: Retrieve the next chunk.
  // Blocks if buffer is currently empty and production is still ongoing.
  // Returns `std::nullopt` when stream is finished and all chunks were
  // consumed. Rethrows captured producer exception if one occurred.
  std::optional<ChunkType> pop() {
    std::unique_lock<std::mutex> lock(mutex_);

    // If an error was already set and buffer is drained, rethrow immediately.
    if (exception_ && buffer_.empty()) {
      std::rethrow_exception(exception_);
    }

    while (buffer_.empty() && !isFinished_ && !isCancelled_) {
      stats_.consumerWaitStalls++;
      cvNotEmpty_.wait(lock, [this]() {
        return !buffer_.empty() || isFinished_ || isCancelled_ || exception_;
      });
    }

    if (exception_ && buffer_.empty()) {
      std::rethrow_exception(exception_);
    }

    if (buffer_.empty()) {
      return std::nullopt;
    }

    ChunkType chunk = std::move(buffer_.front());
    buffer_.pop();

    if constexpr (detail::HasSize_v<ChunkType>) {
      stats_.totalBytesConsumed += chunk.size();
    }
    stats_.totalChunksConsumed++;

    cvNotFull_.notify_one();
    return chunk;
  }

  // ___________________________________________________________________________
  // Cancellation API: Signal early consumer cancellation (e.g. broken pipe).
  // Holds the mutex so the flag update and the wakeups are atomic with
  // respect to the predicate checks in `push`/`pop`: notifying without the
  // lock risks a missed wakeup when a waiter checks the predicate after the
  // flag is set but before it starts waiting.
  void cancel() {
    std::lock_guard<std::mutex> lock(mutex_);
    isCancelled_ = true;
    cvNotFull_.notify_all();
    cvNotEmpty_.notify_all();
  }

  // ___________________________________________________________________________
  // Status accessors.
  [[nodiscard]] bool isCancelled() const noexcept { return isCancelled_; }

  [[nodiscard]] bool isFinished() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return isFinished_;
  }

  [[nodiscard]] bool empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return buffer_.empty();
  }

  [[nodiscard]] size_t size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return buffer_.size();
  }

  [[nodiscard]] size_t capacity() const noexcept { return capacity_; }

  [[nodiscard]] PipelineStats stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stats_;
  }

  // ___________________________________________________________________________
  // High-Level Adapter: Wrap an existing synchronous `cppcoro::generator`
  // with asynchronous double-buffering.
  //
  // Spawns a dedicated background worker to evaluate and buffer chunks ahead
  // of the consumer, overlapping compute and network I/O.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  static cppcoro::generator<ChunkType> makeDoubleBuffered(
      cppcoro::generator<ChunkType> sourceGenerator, size_t capacity = 2) {
    AD_CONTRACT_CHECK(capacity >= 1);
    auto pipeline = std::make_shared<AsyncChunkPipeline<ChunkType>>(capacity);

    // Launch background worker thread to pull chunks eagerly.
    std::thread worker(
        [pipeline, source = std::move(sourceGenerator)]() mutable {
          try {
            for (auto&& chunk : source) {
              if (pipeline->isCancelled()) {
                break;
              }
              if (!pipeline->push(std::move(chunk))) {
                break;
              }
            }
            pipeline->finish();
          } catch (...) {
            pipeline->setException(std::current_exception());
          }
        });

    // Cancel and join the worker upon generator exit.
    auto guard = std::make_shared<WorkerGuard>();
    guard->pipe = pipeline;
    guard->thread = std::move(worker);

    while (true) {
      auto chunkOpt = pipeline->pop();
      if (!chunkOpt.has_value()) {
        break;
      }
      co_yield std::move(chunkOpt.value());
    }
  }

  // ___________________________________________________________________________
  // High-Level Adapter: Stream chunks from a producer callable into a
  // double-buffered generator.
  //
  // The callable receives `ChunkSink<ChunkType>& sink` and calls `sink.push()`.
  template <typename ProducerFunc>
  static cppcoro::generator<ChunkType> pipelineStream(ProducerFunc producerFunc,
                                                      size_t capacity = 2) {
    AD_CONTRACT_CHECK(capacity >= 1);
    auto pipeline = std::make_shared<AsyncChunkPipeline<ChunkType>>(capacity);

    std::thread worker([pipeline, func = std::move(producerFunc)]() mutable {
      try {
        ChunkSink<ChunkType> sink(pipeline);
        func(sink);
        pipeline->finish();
      } catch (...) {
        pipeline->setException(std::current_exception());
      }
    });

    // Cancel and join the worker upon generator exit.
    auto guard = std::make_shared<WorkerGuard>();
    guard->pipe = pipeline;
    guard->thread = std::move(worker);

    while (true) {
      auto chunkOpt = pipeline->pop();
      if (!chunkOpt.has_value()) {
        break;
      }
      co_yield std::move(chunkOpt.value());
    }
  }
#endif

 private:
  // RAII guard ensuring a background worker is cancelled and joined when the
  // adapter generator exits. NOTE: the user-declared destructor suppresses the
  // implicit move constructor, so the adapters populate it in place instead of
  // moving it into the `shared_ptr` (moving would fall back to the deleted
  // copy of the `std::thread` member).
  struct WorkerGuard {
    std::shared_ptr<AsyncChunkPipeline<ChunkType>> pipe;
    std::thread thread;
    ~WorkerGuard() {
      if (pipe) {
        pipe->cancel();
      }
      if (thread.joinable()) {
        thread.join();
      }
    }
  };

  const size_t capacity_;
  mutable std::mutex mutex_;
  std::condition_variable cvNotEmpty_;
  std::condition_variable cvNotFull_;
  std::queue<ChunkType> buffer_;
  std::atomic<bool> isCancelled_{false};
  bool isFinished_{false};
  std::exception_ptr exception_{nullptr};
  PipelineStats stats_{};
};

// _____________________________________________________________________________
// Sink handle provided to producer callbacks in `pipelineStream`.
template <typename ChunkType = std::string>
class ChunkSink {
 private:
  std::shared_ptr<AsyncChunkPipeline<ChunkType>> pipeline_;

 public:
  explicit ChunkSink(std::shared_ptr<AsyncChunkPipeline<ChunkType>> pipeline)
      : pipeline_{std::move(pipeline)} {
    AD_CONTRACT_CHECK(pipeline_ != nullptr);
  }

  // Push chunk into the pipeline, blocking on backpressure if full.
  // Returns true on success; false if cancelled.
  bool push(ChunkType chunk) { return pipeline_->push(std::move(chunk)); }

  // Check whether consumer cancelled early.
  [[nodiscard]] bool isCancelled() const noexcept {
    return pipeline_->isCancelled();
  }
};

}  // namespace qlever::export_pipeline

#endif  // QLEVER_SRC_ENGINE_ASYNCCHUNKPIPELINE_H
