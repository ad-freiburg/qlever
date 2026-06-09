// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <johannes.kalmbach@gmail.com>

#ifndef QLEVER_BATCHEDPIPELINE_H
#define QLEVER_BATCHEDPIPELINE_H

#include <future>
#include <utility>

#include "backports/iterator.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/TaskQueue.h"
#include "util/Timer.h"
#include "util/TupleHelpers.h"
#include "util/UninitializedAllocator.h"

namespace ad_pipeline {

using namespace ad_tuple_helpers;
using Timer = ad_utility::Timer;

namespace detail {
using ThreadsafeTimer = ad_utility::timer::ThreadSafeTimer;

// Holds pre-created thread pools for a pipeline built by
// `setupParallelPipeline`.
struct PipelineExecutor {
  ad_utility::TaskQueue<> batcherPool_;
  ad_utility::TaskQueue<> orchestratorPool_;
  ad_utility::TaskQueue<> workerPool_;
};

/* This is used as a return value for the pickupBatch calls of our pipeline
 * elements If the  is false this means that our
 * pipeline is exhausted and there is no point in asking it for further batches.
 */
template <class T>
struct Batch {
  bool isPipelineGood_ = true;  // if set to false, this was the last (and
                                // possibly incomplete) batch, else there might
                                // be more content waiting in the pipeline.
  std::vector<T, ad_utility::default_init_allocator<T>>
      content_;  // the actual payload
};

template <typename Creator>
CPP_requires(HasGetBatch, requires(Creator creator)(creator.getBatch()));

/*
 * The Batcher class takes a Creator (A Functor that returns a std::optional<T>
 * and will call the creator and store the results. A call to pickupBatch will
 * return a batch of these results and trigger the concurrent calculation of the
 * next batch. Once the creator returns std::nullopt, the Batcher will stop
 * calling it. The next call to produce batch will then set its first element
 * (the a flag) to false as a signal that this batcher is exhausted and return a
 * possibly incomplete batch.
 */
template <class Creator>
class Batcher {
 public:
  using ValueT = std::decay_t<decltype(std::declval<Creator>()().value())>;

 private:
  size_t batchSize_;
  std::unique_ptr<Creator> creator_;
  std::unique_ptr<ThreadsafeTimer> waitingTime_{
      std::make_unique<ThreadsafeTimer>()};
  ad_utility::TaskQueue<>* pool_;
  std::future<detail::Batch<ValueT>> fut_;

 public:
  // construct from the batchSize and Creator Rvalue Reference
  /// @brief Don't use this, call setupParallelPipeline instead
  Batcher(size_t batchSize, Creator&& creator, ad_utility::TaskQueue<>* pool)
      : batchSize_(batchSize),
        creator_(std::make_unique<Creator>(std::move(creator))),
        pool_(pool) {
    orderNextBatch();
  }

  Batcher(Batcher&&) = default;
  Batcher& operator=(Batcher&&) = default;

  // Wait for the `std::future` to terminate.
  ~Batcher() {
    if (fut_.valid()) {
      ad_utility::ignoreExceptionIfThrows([this]() { fut_.wait(); });
    }
  }

  /* Get the next batch of values. A batch contains exactly batchSize values
   * unless this was the last batch. Then result.isPipelineGood_ will be false
   * and the batch might have < batchSize elements
   */
  Batch<ValueT> pickupBatch() {
    try {
      auto timer = waitingTime_->startMeasurement();
      auto res = fut_.get();
      orderNextBatch();
      return res;
    } catch (const std::future_error& e) {
      throw std::runtime_error(
          "encountered std::future_error in the Batcher class. Should never "
          "happen, please report this");
    }
  }

  // get the accumulated time that calls to pickupBatch had to block until they
  // could return the next batch
  std::vector<Timer::Duration> getWaitingTime() const {
    return {waitingTime_->value()};
  }

  // returns the batchSize. The last batch might be smaller
  [[nodiscard]] size_t getBatchSize() const { return batchSize_; }

 private:
  // start assembling the next batch via the dedicated thread pool
  void orderNextBatch() {
    // since the unique_ptr creator_ owns the creator,
    // the captured pointer will stay valid even while this
    // class is moved.
    fut_ = pool_->submit([bs = batchSize_, ptr = creator_.get()]() {
      return produceBatchInternal(bs, ptr);
    });
  }

  /* retrieve values from the creator and store them in the Batch result.
   * Once we have reached batchSize Elements or the creator returns std::nullopt
   * we return. In the latter case, result.first is false
   */
  static detail::Batch<ValueT> produceBatchInternal(size_t batchSize,
                                                    Creator* creator) {
    detail::Batch<ValueT> res;
    // If the Creator type has a method `getBatch`, use this method to produce
    // the batch in one step, otherwise produce the batch value by value.
    if constexpr (CPP_requires_ref(HasGetBatch, Creator)) {
      auto opt = creator->getBatch();
      if (!opt) {
        res.isPipelineGood_ = false;
        return res;
      }
      res.isPipelineGood_ = true;
      res.content_.reserve(opt->size());
      ql::ranges::move(*opt, std::back_inserter(res.content_));
      return res;
    } else {
      res.isPipelineGood_ = true;
      res.content_.reserve(batchSize);
      for (size_t i = 0; i < batchSize; ++i) {
        auto opt = (*creator)();
        if (!opt) {
          res.isPipelineGood_ = false;
          return res;
        }
        res.content_.push_back(std::move(opt.value()));
      }
      return res;
    }
  }
};

/*
 * This class represents an intermediate step of the Pipeline.
 * It gets batches from the PreviousStage and Applies the Transformer(s) (see
 * below) to each element of the batch and returns the produced batch via the
 * pickupBatch() method. Each time a transformed batch is returned, we issue
 * transformation of the next batch in a concurrent thread
 *
 * Concerning the Parallelism, there are two cases
 * case 1: There is only one Transformer. we start <Parallelism> threads
 * that execute the FirstTransformer concurrently on consecutive part of the
 * batch. If Parallelism > 1 this means that the FirstTransformer must be
 * threadsafe
 *
 * case 2: <Parallelism> == Total Number of Transformers
 * Then the FirstTransformer is applied to the first batchSize /
 * #AllTransformers elements and so on. In this case the return type of the
 * FirstTransformer determines the produced Value type and the return types of
 * the remaining transformers must be implicitly convertible to this type.
 */
template <size_t Parallelism, class PreviousStage, class FirstTransformer,
          class... Transformers>
class BatchedPipeline {
 public:
  // either the same transformer is applied in all parallel Branches, or there
  // is exactly one transformer for all
  static_assert(sizeof...(Transformers) == 0 ||
                sizeof...(Transformers) + 1 == Parallelism);

  // the value type the previous pipeline stage delivers to us
  using InT = std::decay_t<
      decltype(std::declval<PreviousStage&>().pickupBatch().content_[0])>;
  // the value type this BatchedPipeline produces
  using ResT = std::invoke_result_t<FirstTransformer, InT>;

 private:
  std::unique_ptr<ThreadsafeTimer> waitingTime_{
      std::make_unique<ThreadsafeTimer>()};
  // the unique_ptrs to our Transformers
  using uniquePtrTuple = toUniquePtrTuple_t<FirstTransformer, Transformers...>;
  // raw non-owning pointers to the transformers
  using rawPtrTuple = toRawPtrTuple_t<uniquePtrTuple>;
  uniquePtrTuple transformers_;
  rawPtrTuple rawTransformers_;
  std::unique_ptr<PreviousStage> previousStage_;
  // Transform sub-batches in parallel.
  ad_utility::TaskQueue<>* workerPool_;
  // 1 dedicated thread that blocks on the previous stage and then dispatches
  // work to `workerPool_`. Kept separate to avoid deadlocking the worker
  // threads with a blocking task.
  ad_utility::TaskQueue<>* orchestratorPool_;
  std::future<Batch<ResT>> fut_;

 public:
  /* TODO<joka921>: Currently the Transformers have to be copy-constructible.
   * could be changed to some kind of perfect forwarding should this ever become
   * an issue, but copying is pretty much the default for Function objects in
   * C++
   */
  BatchedPipeline(PipelineExecutor& exec, PreviousStage&& p, FirstTransformer t,
                  Transformers... ts)
      : transformers_(toUniquePtrTuple(t, ts...)),
        rawTransformers_(toRawPtrTuple(transformers_)),
        previousStage_(std::make_unique<PreviousStage>(std::move(p))),
        workerPool_(&exec.workerPool_),
        orchestratorPool_(&exec.orchestratorPool_) {
    orderNextBatch();
  }

  BatchedPipeline(BatchedPipeline&&) = default;
  BatchedPipeline& operator=(BatchedPipeline&&) = default;

  // Wait for the `std::future` to terminate.
  ~BatchedPipeline() {
    if (fut_.valid()) {
      ad_utility::ignoreExceptionIfThrows([this]() { fut_.wait(); });
    }
  }

  // _____________________________________________________________________
  Batch<ResT> pickupBatch() {
    try {
      auto timer = waitingTime_->startMeasurement();
      auto res = fut_.get();
      orderNextBatch();
      return res;
    } catch (std::future_error& e) {
      throw std::runtime_error(
          "encountered std::future_error in the BatchedPipeline class. Should "
          "never happen, please report this");
    }
  }

  // Asynchronously prepare the next batch. The orchestrator task runs on the
  // dedicated `orchestratorPool_` thread. Within it the per-element worker
  // tasks are dispatched to `workerPool_`.
  void orderNextBatch() {
    auto lambda =
        [p = previousStage_.get(), batchSize = previousStage_->getBatchSize(),
         workerPool = workerPool_,
         orchestratorPool = orchestratorPool_](auto... transformerPtrs) {
          return orchestratorPool->submit(
              [p, batchSize, workerPool, transformerPtrs...]() {
                return produceBatchInternal(p, batchSize, workerPool,
                                            transformerPtrs...);
              });
        };
    fut_ = std::apply(lambda, rawTransformers_);
  }

  // for this and all previous steps of the pipeline, get the total blocking
  // time in calls to produce batch
  std::vector<Timer::Duration> getWaitingTime() const {
    auto res = previousStage_->getWaitingTime();
    res.push_back(waitingTime_->msecs());
    return res;
  }

  // Return the batch size
  [[nodiscard]] size_t getBatchSize() const {
    return previousStage_->getBatchSize();
  }

 private:
  /* The actual transformation logic
   * is a static function to easily pass it to a std::future
   * takes raw pointers to the transformers and the previous stage, so the
   * ownership remains with the unique_ptrs within the class.
   * This implies that this class can be safely moved, even this method runs in
   * parallel also takes the actual batchSize of the pipeline. That way it can
   * apply the correct transformer to the correct element even for the last
   * (possibly incomplete batch)
   */
  template <typename... TransformerPtrs>
  static Batch<ResT> produceBatchInternal(PreviousStage* previousStage,
                                          size_t inBatchSize,
                                          ad_utility::TaskQueue<>* pool,
                                          TransformerPtrs... transformers) {
    auto inBatch = previousStage->pickupBatch();
    Batch<ResT> result;
    result.isPipelineGood_ = inBatch.isPipelineGood_;
    result.content_.resize(inBatch.content_.size());
    const size_t batchSize = inBatchSize / Parallelism;
    auto futures = setupParallelismImpl(
        batchSize, inBatch.content_, result.content_,
        std::make_index_sequence<Parallelism>{}, pool, transformers...);
    for (size_t i = 0; i < Parallelism; ++i) {
      futures[i].get();
    }
    return result;
  }

  // If the range [beg, end) is to be divided into <Parallelism> subranges of
  // size <batchSize> returns the iterator pair belonging to the subrange number
  // <idx>. if batchSize * Parallelism > end-beg then the first ranges will have
  // size batchSize, then there will be an incomplete batch with the remaining
  // range and all other ranges will be empty.
  template <typename It>
  static std::pair<It, It> getBatchRange(It beg, It end, size_t batchSize,
                                         const size_t idx) {
    size_t size = end - beg;
    if (size < Parallelism) {
      return idx == 0 ? std::pair{beg, end} : std::pair{end, end};
    }
    batchSize = size / Parallelism;
    AD_CONTRACT_CHECK(batchSize > 0);
    std::pair<It, It> res;
    res.first = std::min(beg + idx * batchSize, end);
    res.second = idx < Parallelism - 1
                     ? std::min(beg + (idx + 1) * batchSize, end)
                     : end;
    return res;
  }

  // For each element x in [beg, end): move it, apply *transformer to it and
  // append it to *res
  template <typename It, typename ResIt, typename TransformerPtr>
  static void moveAndTransform(It beg, It end, ResIt res,
                               TransformerPtr transformer) {
    std::transform(ql::make_move_iterator(beg), ql::make_move_iterator(end),
                   res,
                   [transformer](typename std::decay_t<decltype(*beg)>&& x) {
                     return (*transformer)(std::move(x));
                   });
  }

  /**
   * @brief This function makes sure that each of the different transformers,
   * which possibly have different types (lambdas!) are applied to the correct
   * subbatch.
   * @tparam InVec std::vector of the PreviousStage's value type
   * @param batchSize the correct batchSize that will also be respected for
   * incomplete batches
   * @param index 0 <= index < Parallelism. The index of the next transformer
   * that is to be assigned. Only used for recursion purposes, must be 0 when
   * called externally
   * @param in The InVec of elements to transform
   * @param pool Raw pointer to the per-stage worker pool.
   * @param transformers Pointers to the remaining transformers
   */
  template <size_t... I, typename InVec, typename OutVec,
            typename... TransformerPtrs>
  static auto setupParallelismImpl(size_t batchSize, InVec& in, OutVec& out,
                                   std::index_sequence<I...>,
                                   ad_utility::TaskQueue<>* pool,
                                   TransformerPtrs... transformers) {
    AD_CORRECTNESS_CHECK(out.size() == in.size());
    if constexpr (sizeof...(I) == sizeof...(TransformerPtrs)) {
      return std::array{
          (createIthFuture<I>(batchSize, in, out, transformers, pool))...};
    } else if constexpr (sizeof...(TransformerPtrs) == 1) {
      // only one transformer that is applied to several threads
      auto onlyTransformer =
          std::get<0>(std::forward_as_tuple(transformers...));
      return std::array{
          (createIthFuture<I>(batchSize, in, out, onlyTransformer, pool))...};
    }
  }

  template <size_t Idx, typename InVec, typename OutVec,
            typename TransformerPtr>
  static std::future<void> createIthFuture(size_t batchSize, InVec& in,
                                           OutVec& out,
                                           TransformerPtr transformer,
                                           ad_utility::TaskQueue<>* pool) {
    auto [startIt, endIt] =
        getBatchRange(std::begin(in), std::end(in), batchSize, Idx);
    return pool->submit([transformer, startIt, endIt,
                         outIt = out.begin() + (startIt - in.begin())] {
      moveAndTransform(startIt, endIt, outIt, transformer);
    });
  }
};

/*
 * Factory function for the BatchedPipeline class
 * That allows us to explicitly specify the Parallelism parameter
 * and implicitly specify the other template types by the constructor
 * arguments.
 */
template <size_t Parallelism, class PreviousStage, class Transformer,
          class... OtherTransformers>
auto makeBatchedPipeline(PipelineExecutor& exec, PreviousStage&& p,
                         Transformer&& t, OtherTransformers&&... other) {
  return BatchedPipeline<Parallelism, std::decay_t<PreviousStage>,
                         std::decay_t<Transformer>,
                         std::decay_t<OtherTransformers>...>(
      exec, std::forward<PreviousStage>(p), std::forward<Transformer>(t),
      std::forward<OtherTransformers>(other)...);
}

/*
 * Base case for the recursive creation of a parallel Pipeline,
 * We are out of Transformers so only return the pipeline
 */
template <typename SoFar>
auto setupParallelPipelineRecursive(SoFar&& sofar) {
  return std::forward<SoFar>(sofar);
}

/*
 * Recursion For the setupParallelPipeline function.
 * This is the case where the nextTransformer is a tuple of <NextParallelism>
 * different transformers. (the size of the tuple is statically asserted inside
 * the BatchedPipeline class) This means that our next BatchedPipeline layer
 * will each of the transformers for exactly one of its <NextParallelism>
 * threads.
 */
template <size_t NextParallelism, size_t... Parallelisms, typename SoFar,
          typename... NextTransformer, typename... MoreTransformers>
auto setupParallelPipelineRecursive(PipelineExecutor& exec, SoFar&& sofar,
                                    std::tuple<NextTransformer...>&& next,
                                    MoreTransformers&&... transformers) {
  auto lambda = [&sofar, &exec](NextTransformer&&... transformers) {
    return makeBatchedPipeline<NextParallelism>(
        exec, std::forward<SoFar>(sofar), std::move(transformers)...);
  };

  return setupParallelPipelineRecursive<Parallelisms...>(
      std::apply(lambda, std::move(next)),
      std::forward<MoreTransformers>(transformers)...);
}

class Interface;  // forward declaration needed below for friend declaration

}  // namespace detail

// Holds a Pipeline (Object on which we can call "pickupBatch" and get a
// Batch<ValueType> and extracts its elements one by one. Internally buffers one
// batch and concurrently produces the nextBatch while it issues the element
// from the buffer one at a time via the getNextValue() method. An instantiation
// of this templated class is created by the calls to `setupParallelPipeline()`.
// It supports the getNextValue() interface that will return
// std::optional<ValueT> for each of the completely transformed elements in a
// pipeline and std::nullopt once it is exhausted. See the documentation of the
// setup* functions.
template <class Pipeline>
class BatchExtractor {
 public:
  /// The type of our elements after all transformations were applied
  using ValueT = std::decay_t<
      decltype(std::declval<Pipeline>().pickupBatch().content_[0])>;

 private:
  std::unique_ptr<Pipeline> pipeline_;
  std::vector<ValueT, ad_utility::default_init_allocator<ValueT>> buffer_;
  size_t bufferPosition_ = 0;
  bool isPipelineValid_ = true;

 public:
  /// Get the next completely transformed value from the pipeline. std::nullopt
  /// means that all elements have been extracted and the pipeline is exhausted.
  /// Might block if the pipeline is currently busy and the internal buffer is
  /// empty
  std::optional<ValueT> getNextValue() {
    if (bufferPosition_ == buffer_.size()) {
      if (!isPipelineValid_) {
        return std::nullopt;
      }
      auto res = pipeline_->pickupBatch();
      isPipelineValid_ = res.isPipelineGood_;
      buffer_ = std::move(res.content_);
      bufferPosition_ = 0;
      if (buffer_.empty()) {
        return std::nullopt;
      }
    }
    return std::move(buffer_[bufferPosition_++]);
  }

  /**
   * @brief for all steps in the pipeline, report how long their calls to
   * pickupBatch were blocking. TODO<joka921>: how useful is this measure?
   */
  [[nodiscard]] std::vector<Timer::Duration> getWaitingTime() const {
    return pipeline_->getWaitingTime();
  }

  /// return the batchSize
  [[nodiscard]] size_t getBatchSize() const {
    return pipeline_->getBatchSize();
  }

 private:
  // Pipelines are move-only types
  explicit BatchExtractor(Pipeline&& pipeline)
      : pipeline_(std::make_unique<Pipeline>(std::move(pipeline))) {}
  friend class detail::Interface;
};

namespace detail {
class Interface {
 public:
  // the actual implementation of setupParallelPipeline, hidden in this class
  // s.t. we can make the BatchExtractor's constructor private and expose a
  // minimal interface
  template <size_t... Parallelisms, typename Creator, typename... Ts>
  static auto setupParallelPipeline(PipelineExecutor& exec, size_t batchSize,
                                    Creator&& creator, Ts&&... transformers) {
    static_assert(sizeof...(Parallelisms) == sizeof...(Ts),
                  "setupParallelPipeline needs same number of parallelism "
                  "specifiers and transformers");
    auto batcher = detail::Batcher(batchSize, std::forward<Creator>(creator),
                                   &exec.batcherPool_);
    auto pipeline = detail::setupParallelPipelineRecursive<Parallelisms...>(
        exec, std::move(batcher), std::forward<Ts>(transformers)...);
    return BatchExtractor(std::move(pipeline));
  }
};
}  // namespace detail

/**
 * @brief setup a pipeline that efficiently creates and transforms values.
 * Concurrency is used between and within the different stages.
 *
 * With each Transformer there is a degree of Parallelism (a size_t) associated.
 * The transformer must be a std::tuple of p function objects (where p is the
 * corresponding degree of parallelism). The batch is split into p parts where
 * the first part is transformed by the first element of the tuple, the second
 * part by the second element... It is of course necessary, that the return
 * types of the function objects in the tuple are compatible, the return type of
 * this stage is derived from the first tuple element. The following guarantee
 * holds: If two (not necessary consecutive) stages i and j have the same degree
 * of parallelism p > 1 and both specify a tuple of p function objects then the
 * elements that are transformed by the k-th tuple element on stage i are also
 * transformed by the k-th tuple element on stage j.
 *
 * @tparam Parallelisms One size_t for each transformer, assigning it a degree
 * of parallelism
 * @param exec An instance of `PipelineExecutor` that holds the pre-created
 * thread pools for the pipeline. It must outlive the returned pipeline.
 * @param batchSize the size of the batches. Might influence Performance (time
 * vs. memory consumption)
 * @param creator repeated calls to creator() must create the initial values of
 * type T_0 one at a time as std::optional<T_0> a return value of std::nullopt
 * means that no more elements are created
 * @param transformers for each element a tuple of k function objects, where k
 * is the corresponding entry in the Parallelisms.
 * @return a BatchExtractor on which we can call getNextValue() to get
 * std::optional<ReturnTypeOfLastTransformers> here also a std::nullopt
 * signalizes the last value.
 */
template <size_t... Parallelisms, typename Creator, typename... Ts>
static auto setupParallelPipeline(detail::PipelineExecutor& exec,
                                  size_t batchSize, Creator&& creator,
                                  Ts&&... transformers) {
  return detail::Interface::setupParallelPipeline<Parallelisms...>(
      exec, batchSize, std::forward<Creator>(creator),
      std::forward<Ts>(transformers)...);
}

// Create a `PipelineExecutor` whose pre-created pools can be shared across
// multiple invocations of `setupParallelPipeline<Parallelism>`.
inline detail::PipelineExecutor makePipelineExecutor(size_t parallelism) {
  return {ad_utility::TaskQueue<>(1, 1), ad_utility::TaskQueue<>(1, 1),
          ad_utility::TaskQueue<>(parallelism, parallelism)};
}

}  // namespace ad_pipeline

#endif  // QLEVER_BATCHEDPIPELINE_H
