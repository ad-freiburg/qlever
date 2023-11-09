// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <johannes.kalmbach@gmail.com>

#ifndef QLEVER_BATCHEDPIPELINE_H
#define QLEVER_BATCHEDPIPELINE_H

#include <future>
#include <utility>

#include "util/Exception.h"
#include "util/Log.h"
#include "util/Timer.h"
#include "util/TupleHelpers.h"
#include "util/UninitializedAllocator.h"

namespace ad_pipeline {

using namespace ad_tuple_helpers;
using Timer = ad_utility::Timer;

namespace detail {
using AtomicMs = std::atomic<std::chrono::milliseconds::rep>;

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

  // construct from the batchSize and Creator Rvalue Reference
  /// @brief Don't use these, call setupPipeline or setupParallelPipeline
  /// instead
  Batcher(size_t batchSize, Creator&& creator)
      : batchSize_(batchSize),
        creator_(std::make_unique<Creator>(std::move(creator))) {
    orderNextBatch();
  }

  // construct from the batchSize and Creator const Reference
  /// @brief Don't use these, call setupPipeline or setupParallelPipeline
  /// instead
  Batcher(size_t batchSize, const Creator& creator)
      : batchSize_(batchSize), creator_(std::make_unique<Creator>(creator)) {
    orderNextBatch();
  }

  /* Get the next batch of values. A batch contains exactly batchSize values
   * unless this was the last batch. Then result.isPipelineGood_ will be false
   * and the batch might have < batchSize elements
   */
  Batch<ValueT> pickupBatch() {
    try {
      Timer timer{Timer::InitialStatus::Started};
      auto res = fut_.get();
      orderNextBatch();
      waitingTime_->fetch_add(timer.msecs().count());
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
    return {std::chrono::milliseconds{*waitingTime_}};
  }

  // returns the batchSize. The last batch might be smaller
  [[nodiscard]] size_t getBatchSize() const { return batchSize_; }

 private:
  size_t batchSize_;
  std::unique_ptr<Creator> creator_;
  std::unique_ptr<AtomicMs> waitingTime_{std::make_unique<AtomicMs>(0)};
  std::future<detail::Batch<ValueT>> fut_;

  // start assembling the next batch in parallel
  void orderNextBatch() {
    // since the unique_ptr creator_ owns the creator,
    // the captured pointer will stay valid even while this
    // class is moved.
    fut_ = std::async(std::launch::async,
                      [bs = batchSize_, ptr = creator_.get()]() {
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
    if constexpr (requires { creator->getBatch(); }) {
      auto opt = creator->getBatch();
      if (!opt) {
        res.isPipelineGood_ = false;
        return res;
      }
      res.isPipelineGood_ = true;
      res.content_.reserve(opt->size());
      std::ranges::move(*opt, std::back_inserter(res.content_));
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
  using AtomicMs = detail::AtomicMs;

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

  /* TODO<joka921>: Currently the Transformers have to be copy-constructible.
   * could be changed to some kind of perfect forwarding should this ever become
   * an issue, but copying is pretty much the default for Function objects in
   * C++
   */
  BatchedPipeline(PreviousStage&& p, FirstTransformer t, Transformers... ts)
      : transformers_(toUniquePtrTuple(t, ts...)),
        rawTransformers_(toRawPtrTuple(transformers_)),
        previousStage_(std::make_unique<PreviousStage>(std::move(p))) {
    orderNextBatch();
  }

  // _____________________________________________________________________
  Batch<ResT> pickupBatch() {
    try {
      Timer timer{Timer::InitialStatus::Started};
      auto res = fut_.get();
      orderNextBatch();
      waitingTime_->fetch_add(timer.msecs().count());
      return res;
    } catch (std::future_error& e) {
      throw std::runtime_error(
          "encountered std::future_error in the BatchedPipeline class. Should "
          "never happen, please report this");
    }
  }

  // asynchronously prepare the next Batch in a different thread
  void orderNextBatch() {
    auto lambda =
        [p = previousStage_.get(),
         batchSize = previousStage_->getBatchSize()](auto... transformerPtrs) {
          return std::async(
              std::launch::async, [p, batchSize, transformerPtrs...]() {
                return produceBatchInternal(p, batchSize, transformerPtrs...);
              });
        };
    fut_ = std::apply(lambda, rawTransformers_);
  }

  // for this and all previous steps of the pipeline, get the total blocking
  // time in calls to produce batch
  std::vector<Timer::Duration> getWaitingTime() const {
    auto res = previousStage_->getWaitingTime();
    res.push_back(std::chrono::milliseconds(*waitingTime_));
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
                                          TransformerPtrs... transformers) {
    auto inBatch = previousStage->pickupBatch();
    Batch<ResT> result;
    result.isPipelineGood_ = inBatch.isPipelineGood_;
    const size_t batchSize = inBatchSize / Parallelism;
    // We know the total size in advance, so we can preallocate the memory and
    // the threads can directly write to the final result.
    result.content_.resize(inBatch.content_.size());
    auto futures = setupParallelismImpl(
        batchSize, inBatch.content_, result.content_,
        std::make_index_sequence<Parallelism>{}, transformers...);
    // if we had multiple threads, we have to merge the partial results in the
    // correct order.
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
    std::transform(std::make_move_iterator(beg), std::make_move_iterator(end),
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
   * @param futuresPtr pointer to one future per Transformer that will be
   * associated with the respective subbatch calculation
   * @param batchSize the correct batchSize that will also be respected for
   * incomplete batches
   * @param index 0 <= index < Parallelism. The index of the next transformer
   * that is to be assigned. Only used for recursion purposes, must be 0 when
   * called externally
   * @param in The InVec of elements to transform
   * @param transformer Pointer to the first transformer
   * @param transformers Pointers to the remaining transformers
   */
  template <size_t... I, typename InVec, typename OutVec,
            typename... TransformerPtrs>
  static auto setupParallelismImpl(size_t batchSize, InVec& in, OutVec& out,
                                   std::index_sequence<I...>,
                                   TransformerPtrs... transformers) {
    AD_CORRECTNESS_CHECK(out.size() == in.size());
    if constexpr (sizeof...(I) == sizeof...(TransformerPtrs)) {
      return std::array{
          (createIthFuture<I>(batchSize, in, out, transformers))...};
    } else if constexpr (sizeof...(TransformerPtrs) == 1) {
      // only one transformer that is applied to several threads
      auto onlyTransformer =
          std::get<0>(std::forward_as_tuple(transformers...));
      return std::array{
          (createIthFuture<I>(batchSize, in, out, onlyTransformer))...};
    }
  }

  template <size_t Idx, typename InVec, typename OutVec,
            typename TransformerPtr>
  static std::future<void> createIthFuture(size_t batchSize, InVec& in,
                                           OutVec& out,
                                           TransformerPtr transformer) {
    auto [startIt, endIt] =
        getBatchRange(std::begin(in), std::end(in), batchSize, Idx);
    // start a thread for the transformer.
    return std::async(std::launch::async,
                      [transformer, startIt = startIt, endIt = endIt,
                       outIt = out.begin() + (startIt - in.begin())] {
                        std::vector<ResT> res;
                        moveAndTransform(startIt, endIt, outIt, transformer);
                      });
  }

 private:
  std::unique_ptr<AtomicMs> waitingTime_{std::make_unique<AtomicMs>(0)};
  // the unique_ptrs to our Transformers
  using uniquePtrTuple = toUniquePtrTuple_t<FirstTransformer, Transformers...>;
  // raw non-owning pointers to the transformers
  using rawPtrTuple = toRawPtrTuple_t<uniquePtrTuple>;
  uniquePtrTuple transformers_;
  rawPtrTuple rawTransformers_;
  std::unique_ptr<PreviousStage> previousStage_;
  std::future<Batch<ResT>> fut_;
};

/*
 * Factory function for the BatchedPipeline class
 * That allows us to explicitly specify the Parallelism parameter
 * and implicitly specify the other template types by the constructor
 * arguments.
 */
template <size_t Parallelism, class PreviousStage, class Transformer,
          class... OtherTransformers>
auto makeBatchedPipeline(PreviousStage&& p, Transformer&& t,
                         OtherTransformers&&... other) {
  return BatchedPipeline<Parallelism, std::decay_t<PreviousStage>,
                         std::decay_t<Transformer>,
                         std::decay_t<OtherTransformers>...>(
      std::forward<PreviousStage>(p), std::forward<Transformer>(t),
      std::forward<OtherTransformers>(other)...);
}

/* recursion base case, we are trying to setup a Pipeline but are out of
 * transformers, just return the pipeline used by the global setupPipeline
 * function
 */
template <typename SoFar>
auto setupPipelineRecursive(SoFar&& sofar) {
  return std::forward<SoFar>(sofar);
}

/* add one BatchedPipeline Layer to the pipeline sofar using the
 * first transformer and then recurse for the remaining transformers
 * Used by the global setupPipeline function
 */
template <typename SoFar, typename NextTransformer,
          typename... MoreTransformers>
auto setupPipelineRecursive(SoFar&& sofar, NextTransformer&& next,
                            MoreTransformers&&... transformers) {
  return setupPipelineRecursive(
      makeBatchedPipeline<1>(std::forward<SoFar>(sofar),
                             std::forward<NextTransformer>(next)),
      std::forward<MoreTransformers>(transformers)...);
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
 * This is the case where the nextTransformer is not a tuple.
 * This means that our next BatchedPipeline layer will use the
 * same transformer for all its <NextParallelis> threads.
 */
template <size_t NextParallelism, size_t... Parallelisms, typename SoFar,
          typename NextTransformer,
          typename = std::enable_if_t<!is_tuple<NextTransformer>::value>,
          typename... MoreTransformers>
auto setupParallelPipelineRecursive(SoFar&& sofar, NextTransformer&& next,
                                    MoreTransformers&&... transformers) {
  return setupParallelPipelineRecursive<Parallelisms...>(
      makeBatchedPipeline<NextParallelism>(std::forward<SoFar>(sofar),
                                           std::forward<NextTransformer>(next)),
      std::forward<MoreTransformers>(transformers)...);
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
auto setupParallelPipelineRecursive(SoFar&& sofar,
                                    std::tuple<NextTransformer...>&& next,
                                    MoreTransformers&&... transformers) {
  auto lambda = [&sofar](NextTransformer&&... transformers) {
    return makeBatchedPipeline<NextParallelism>(std::forward<SoFar>(sofar),
                                                std::move(transformers)...);
  };

  return setupParallelPipelineRecursive<Parallelisms...>(
      std::apply(lambda, std::move(next)),
      std::forward<MoreTransformers>(transformers)...);
}

class Interface;  // forward declaration needed below for friend declaration

}  // namespace detail

/* Holds a Pipeline (Object on which we can call "pickupBatch" and get a
 * Batch<ValueType> and extracts its elements one by one. Internally buffers one
 * batch and concurrently produces the nextBatch while it issues the element
 * from the buffer one at a time via the getNextValue() method.
 */
/**
 * @brief An instantiation of this templated class is created by the calls to
 * setupPipeline() and setupParallelPipeline() It supports the getNextValue()
 * interface that will return std::optional<ValueT> for each of the completely
 * transformed elements in a pipeline and std::nullopt once it is exhausted. See
 * the documentation of the setup* functions
 */
template <class Pipeline>
class BatchExtractor {
  using AtomicMs = detail::AtomicMs;

 public:
  /// The type of our elements after all transformations were applied
  using ValueT = std::decay_t<
      decltype(std::declval<Pipeline>().pickupBatch().content_[0])>;

  /// Get the next completely transformed value from the pipeline. std::nullopt
  /// means that all elements have been extracted and the pipeline is exhausted.
  /// Might block if the pipeline is currently busy and the internal buffer is
  /// empty
  std::optional<ValueT> getNextValue() {
    // we return the elements in order
    if (buffer_.size() == bufferPosition_ && isPipelineValid_) {
      // we have to wait for the next parallel batch to be completed.
      Timer timer{Timer::InitialStatus::Started};
      {
        auto res = fut_.get();
        isPipelineValid_ = res.isPipelineGood_;
        buffer_ = std::move(res.content_);
      }

      waitingTime_->fetch_add(timer.msecs().count());
      bufferPosition_ = 0;
      if (isPipelineValid_) {
        // if possible, directly submit the next parsing job
        fut_ = std::async(std::launch::async, [ptr = pipeline_.get()]() {
          return ptr->pickupBatch();
        });
      }
    }
    // we now should have some values in our buffer
    if (bufferPosition_ < buffer_.size()) {
      return std::move(buffer_[bufferPosition_++]);
    } else {
      // we can only reach this if the pipeline is exhausted and we have reached
      // past the last element.
      return std::nullopt;
    }
  }

  /**
   * @brief for all steps in the pipeline, report how long their calls to
   * pickupBatch were blocking. TODO<joka921>: how useful is this measure?
   */
  [[nodiscard]] std::vector<Timer::Duration> getWaitingTime() const {
    auto res = pipeline_->getWaitingTime();
    res.push_back(std::chrono::milliseconds(*waitingTime_));
    return res;
  }

  /// return the batchSize
  [[nodiscard]] size_t getBatchSize() const { return pipeline_.getBatchSize(); }

 private:
  std::unique_ptr<AtomicMs> waitingTime_{std::make_unique<AtomicMs>(0)};
  std::unique_ptr<Pipeline> pipeline_;
  std::future<detail::Batch<ValueT>> fut_;
  std::vector<ValueT, ad_utility::default_init_allocator<ValueT>> buffer_;
  size_t bufferPosition_ = 0;
  bool isPipelineValid_ = true;

  // Pipelines are move-only types
  explicit BatchExtractor(Pipeline&& pipeline)
      : pipeline_(std::make_unique<Pipeline>(std::move(pipeline))) {
    fut_ = std::async(std::launch::async,
                      [ptr = pipeline_.get()]() { return ptr->pickupBatch(); });
  }
  friend class detail::Interface;
};

namespace detail {
class Interface {
 public:
  // the actual implementation of setupPipeline, hidden in this class s.t.
  // we can make the BatchExtractor's constructor private and expose a minimal
  // interface
  template <typename Creator, typename... Ts>
  static auto setupPipeline(size_t batchSize, Creator&& creator,
                            Ts&&... transformers) {
    auto batcher = detail::Batcher(batchSize, std::forward<Creator>(creator));
    auto pipeline = detail::setupPipelineRecursive(
        std::move(batcher), std::forward<Ts>(transformers)...);
    return BatchExtractor(std::move(pipeline));
  }

  // the actual implementation of setupParallelPipeline, hidden in this class
  // s.t. we can make the BatchExtractor's constructor private and expose a
  // minimal interface
  template <size_t... Parallelisms, typename Creator, typename... Ts>
  static auto setupParallelPipeline(size_t batchSize, Creator&& creator,
                                    Ts&&... transformers) {
    static_assert(sizeof...(Parallelisms) == sizeof...(Ts),
                  "setupParallelPipeline needs same number of parallelism "
                  "specifiers and transformers");
    auto batcher = detail::Batcher(batchSize, std::forward<Creator>(creator));
    auto pipeline = detail::setupParallelPipelineRecursive<Parallelisms...>(
        std::move(batcher), std::forward<Ts>(transformers)...);
    return BatchExtractor(std::move(pipeline));
  }
};
}  // namespace detail

/**
 * @brief setup a pipeline that efficiently creates and transforms values.
 * Concurrency is used ONLY BETWEEN the different stages
 *
 * (if you want to use concurrency also within the stages see the
 * setupParallelPipeline() function below)
 *
 * Each element is created by the creator and then transformed by all of the
 * transformers one after another s.t. it becomes
 * ... TransformerN(....(Transformer1(creator()))
 * This is implemented by first creating a batch of values. Then this batch is
 * passed to the first transformer which transforms it while the creator creates
 * the nextBatch in Parallel. This principle applies to all the transformers.
 * This means that all the transformers/ different stages of the pipeline must
 * be able to run in Parallel without conflicts (but of course, no two
 * transformers access the same element at a time. This allows us to use
 * Transformers which might have sideeffects. For examples see the corresponding
 * unitTests.
 * @param batchSize the size of the batches. Might influence Performance (time
 * vs. memory consumption)
 * @param creator repeated calls to creator() must create the initial values of
 * type T_0 one at a time as std::optional<T_0> a return value of std::nullopt
 * means that no more elements are created
 * @param transformers Function objects that perform the different
 * transformations in the order they are listed. The i-th transformer takes a
 * T_i&& and returns a T_{i+1}; Currently the transformers must be
 * copy-constructible
 * @return a BatchExtractor on which we can call getNextValue() to get
 * std::optional<ReturnTypeOfLastTransformers> here also a std::nullopt
 * signalizes the last value.
 */
template <typename Creator, typename... Ts>
auto setupPipeline(size_t batchSize, Creator&& creator, Ts&&... transformers) {
  return detail::Interface::setupPipeline(batchSize,
                                          std::forward<Creator>(creator),
                                          std::forward<Ts>(transformers)...);
}

/**
 * @brief setup a pipeline that efficiently creates and transforms values.
 * Concurrency is used between and within the different stages
 *
 * In general this works similar to the setupPipeline() function template. Make
 * sure to read and understand the corresponding documentation first. It has the
 * following difference::
 *
 * With each Transformer there is a degree of Parallelism (a size_t) associated.
 * If this is degree is 1, this stage behaves exactly the same as in the
 * setupPipeline function above.
 *
 * Otherwise let p be the degree of Parallelism of the i-th Transformer. then
 * there are two possibilities:
 * - The transformer is a single function object. Then for each batch p threads
 * are started that each are assigned a part of the batch. Those are then
 * transformed concurrently by calls to the transformer. This means that the
 * transformer must be threadsafe.
 *
 * - The transformer is a std::tuple of p function objects. Then again the batch
 * is split into p parts where the first part is transformed by the first
 * element of the tuple, the second part by the second element... In this case
 * it is of course necessary, that the return types of the function objects in
 * the tuple are compatible, the return type of this stage is derived from the
 * first tuple element. The following guarantee holds: If two (not necessary
 * consecutive) stages i and j have the same degree of parallelism p > 1  and
 * both specify a tuple of p function objects then the elements that are
 * transformed by the k-th tuple element on stage i are also transformed by the
 * k-th tuple element on stage j.
 *
 * @tparam Parallelisms One size_t for each transformer, assigning it a degree
 * of parallelism
 * @param batchSize the size of the batches. Might influence Performance (time
 * vs. memory consumption)
 * @param creator repeated calls to creator() must create the initial values of
 * type T_0 one at a time as std::optional<T_0> a return value of std::nullopt
 * means that no more elements are created
 * @param transformers for each element either one FunctionObject or a tuple of
 * k function objects, where k is the corresponding entry in the Parallelisms.
 * @return a BatchExtractor on which we can call getNextValue() to get
 * std::optional<ReturnTypeOfLastTransformers> here also a std::nullopt
 * signalizes the last value.
 */
template <size_t... Parallelisms, typename Creator, typename... Ts>
static auto setupParallelPipeline(size_t batchSize, Creator&& creator,
                                  Ts&&... transformers) {
  return detail::Interface::setupParallelPipeline<Parallelisms...>(
      batchSize, std::forward<Creator>(creator),
      std::forward<Ts>(transformers)...);
}

}  // namespace ad_pipeline

#endif  // QLEVER_BATCHEDPIPELINE_H
