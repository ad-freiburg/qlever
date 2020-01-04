// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <johannes.kalmbach@gmail.com>

#ifndef QLEVER_BATCHEDPIPELINE_H
#define QLEVER_BATCHEDPIPELINE_H

#include <future>
#include <utility>
#include "./Timer.h"
#include "./TupleHelpers.h"

namespace ad_pipeline {

using namespace ad_tuple_helpers;

namespace detail {

/* This is used as a return value for the produceBatch calls of our pipeline
 * elements If the first element (the bool) is false this means that our
 * pipeline is exhausted and there is no point in asking it for further batches.
 */
template <class T>
using VecT = std::pair<bool, std::vector<T>>;

/*
 * The Batcher class takes a Creator (A Functor that returns a std::optional<T>
 * and will call the creator and store the results. A call to produceBatch will
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
      : _batchSize(batchSize),
        _creator(std::make_unique<Creator>(std::move(creator))) {
    orderNextBatch();
  }

  // construct from the batchSize and Creator const Reference
  /// @brief Don't use these, call setupPipeline or setupParallelPipeline
  /// instead
  Batcher(size_t batchSize, const Creator& creator)
      : _batchSize(batchSize), _creator(std::make_unique<Creator>(creator)) {
    orderNextBatch();
  }

  /* Get the next batch of values. A batch exactly batchSize values
   * unless this was the last batch. Then result.first will be false
   * and the batch might have < batchSize elements
   */
  detail::VecT<ValueT> produceBatch() {
    try {
      _timer.cont();
      auto res = _fut.get();
      _timer.stop();
      orderNextBatch();
      return res;
    } catch (const std::future_error& e) {
      throw std::runtime_error(
          "encountered std::future_error in the Batcher class. Should never "
          "happen, please report this");
    }
  }

  // get the accumulated time that calls to produceBatch had to block until they
  // could return the next batch
  std::vector<off_t> getWaitingTime() const { return {_timer.msecs()}; }

  // returns the batchSize. The last batch might be smaller
  [[nodiscard]] size_t getBatchSize() const { return _batchSize; }

 private:
  size_t _batchSize;
  std::unique_ptr<Creator> _creator;
  std::future<detail::VecT<ValueT>> _fut;
  ad_utility::Timer _timer;

  // start assembling the next batch in parallel
  void orderNextBatch() {
    // since the unique_ptr _creator owns the creator,
    // the captured pointer will stay valid even while this
    // class is moved.
    _fut = std::async(std::launch::async,
                      [bs = _batchSize, ptr = _creator.get()]() {
                        return produceBatchInternal(bs, ptr);
                      });
  }

  /* retrieve values from the creator and store them in the VecT result.
   * Once we have reached batchSize Elements or the creator returns std::nullopt
   * we return. In the latter case, result.first is false
   */
  static detail::VecT<ValueT> produceBatchInternal(size_t batchSize,
                                                   Creator* creator) {
    detail::VecT<ValueT> res;
    res.first = true;
    res.second.reserve(batchSize);
    for (size_t i = 0; i < batchSize; ++i) {
      auto opt = (*creator)();
      if (!opt) {
        res.first = false;
        return res;
      }
      res.second.push_back(std::move(opt.value()));
    }
    return res;
  }
};

/*
 * This class represents an intermediate step of the Pipeline.
 * It gets batches from the Producer and Applies the Transformer(s) (see below)
 * to each element of the batch and returns the produced batch via the
 * produceBatch() method. Each time a transformed batch is returned, we issue
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
template <size_t Parallelism, class Producer, class FirstTransformer,
          class... Transformers>
class BatchedPipeline {
 public:
  // either the same transformer is applied in all parallel Branches, or there
  // is exactly one transformer for all
  static_assert(sizeof...(Transformers) == 0 ||
                sizeof...(Transformers) + 1 == Parallelism);

  // the value type our producer delivers to us
  using InT = std::decay_t<decltype(
      std::declval<Producer&>().produceBatch().second[0])>;

  // the value type this BatchedPipeline produces
  using ResT = std::invoke_result_t<FirstTransformer, InT>;

  /* TODO<joka921>: Currently the Transformers have to be copy-constructible.
   * could be changed to some kind of perfect forwarding should this ever become
   * an issue, but copying is pretty much the default for Function objects in
   * C++
   */
  BatchedPipeline(Producer&& p, FirstTransformer t, Transformers... ts)
      : _transformers(toUniquePtrTuple(t, ts...)),
        _rawTransformers(toRawPtrTuple(_transformers)),
        _producer(std::make_unique<Producer>(std::move(p))) {
    orderNextBatch();
  }

  // _____________________________________________________________________
  VecT<ResT> produceBatch() {
    try {
      _timer.cont();
      auto res = _fut.get();
      _timer.stop();
      orderNextBatch();
      return res;
    } catch (std::future_error& e) {
      throw std::runtime_error(
          "encountered std::future_error in the BatchedPipeline class. Should "
          "never happen, please report this");
    }
  }

  // asynchronously prepare the next Batch in a different thread
  void orderNextBatch() {
    auto lambda = [p = _producer.get(), batchSize = _producer->getBatchSize()](
                      auto... transformerPtrs) {
      return std::async(
          std::launch::async, [p, batchSize, transformerPtrs...]() {
            return produceBatchInternal(p, batchSize, transformerPtrs...);
          });
    };
    _fut = std::apply(lambda, _rawTransformers);
  }

  // for this and all previous steps of the pipeline, get the total blocking
  // time in calls to produce batch
  std::vector<off_t> getWaitingTime() const {
    auto res = _producer->getWaitingTime();
    res.push_back(_timer.msecs());
    return res;
  }

  // Return the batch size
  [[nodiscard]] size_t getBatchSize() const {
    return _producer->getBatchSize();
  }

 private:
  /*  The actual transformation logic
   *  is a static function to easily pass it to a std::future
   *  takes raw pointers to the transformers and the producer, so the
   *  ownership remains with the unique_ptrs within the class.
   *  This implies that this class can be safely moved, even this method runs in
   * parallel also takes the actual batchSize of the pipeline. That way it can
   * apply the correct transformer to the correct element even for the last
   * (possibly incomplete batch)
   */
  template <typename FirstPtr, typename... OtherPtrs>
  static VecT<ResT> produceBatchInternal(Producer* producer, size_t inBatchSize,
                                         FirstPtr fst, OtherPtrs... other) {
    auto inBatch = producer->produceBatch();
    auto& in = inBatch.second;
    VecT<ResT> result;
    auto& out = result.second;
    result.first = inBatch.first;
    out.reserve(in.size());
    if constexpr (Parallelism == 1) {
      // simplest case, only one thread and one transformer
      std::transform(std::make_move_iterator(in.begin()),
                     std::make_move_iterator(in.end()), std::back_inserter(out),
                     [transformer = fst](
                         typename std::decay_t<decltype(in)>::value_type&& x) {
                       return (*transformer)(std::move(x));
                     });
    } else {
      // currently each of the <parallelism> threads first creates its own VecT
      // and later we merge doing this in place would require something like a
      // std::vector without default construction on insert.
      std::array<std::future<std::vector<ResT>>, Parallelism> futures;
      const size_t batchSize = inBatchSize / Parallelism;
      if constexpr (sizeof...(OtherPtrs) == 0) {
        // only one transformer, but multiple threads that all use the same
        // transformer
        for (size_t i = 0; i < Parallelism; ++i) {
          // correctly calculate the boundaries for the subbatch
          auto startIt = std::min(in.begin() + i * batchSize, in.end());
          auto endIt =
              i < Parallelism - 1
                  ? std::min(in.begin() + (i + 1) * batchSize, in.end())
                  : in.end();
          // start a thread returns a transformed version of all the elements in
          // [startIt, endIt)
          futures[i] = std::async(
              std::launch::async,
              [i, transformer = fst, batchSize, startIt, endIt] {
                std::vector<ResT> res;
                res.reserve(endIt - startIt);
                std::transform(
                    std::make_move_iterator(startIt),
                    std::make_move_iterator(endIt), std::back_inserter(res),
                    [transformer](
                        typename std::decay_t<decltype(in)>::value_type&& x) {
                      return (*transformer)(std::move(x));
                    });
                return res;
              });
        }
      } else {  // we have one transformer per parallel Branch
        // this was outsourced since we need template magic for the different
        // transformers
        createInternalParallelism(&futures, batchSize, 0, in, fst, other...);
      }
      // if we had multiple threads, we have to merge the partial results in the
      // correct order.
      for (size_t i = 0; i < Parallelism; ++i) {
        auto vec = futures[i].get();
        out.insert(out.end(), std::make_move_iterator(vec.begin()),
                   std::make_move_iterator(vec.end()));
      }
    }
    return result;
  }

  /**
   * @brief This function makes sure that each of the different transformers,
   * which possibly have different types (lambdas!) are applied to the correct
   * subbatch.
   * @tparam InVec std::vector of the Producer's value type
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
  template <typename InVec, typename TransformerPtr,
            typename... TransformerPtrs>
  static void createInternalParallelism(
      std::array<std::future<std::vector<ResT>>, Parallelism>* futuresPtr,
      size_t batchSize, size_t index, InVec& in, TransformerPtr transformer,
      TransformerPtrs... transformers) {
    auto& futures = *futuresPtr;
    // calculate the correct bounds for the next subbatch
    auto startIt = std::min(in.begin() + index * batchSize, in.end());
    auto endIt = index < Parallelism - 1
                     ? std::min(in.begin() + (index + 1) * batchSize, in.end())
                     : in.end();
    // start a thread for the first transformer. This function will
    // be called recursively s.t. the index-th transformer will always be
    // the first in the respective recursion step
    futures[index] = std::async(std::launch::async, [index, transformer,
                                                     batchSize, startIt,
                                                     endIt] {
      std::vector<ResT> res;
      res.reserve(endIt - startIt);
      std::transform(
          std::make_move_iterator(startIt), std::make_move_iterator(endIt),
          std::back_inserter(res),
          [transformer](typename std::decay_t<decltype(in)>::value_type&& x) {
            return (*transformer)(std::move(x));
          });
      return res;
    });

    if (index < Parallelism - 1) {
      // this was not the last transformer, recursive
      if constexpr (sizeof...(transformers) > 0) {
        createInternalParallelism(futuresPtr, batchSize, index + 1, in,
                                  transformers...);
      } else {
        // this can never happen by the static_asserted invariants of the class
        // but we need the constexpr if to make the instantiation for the last
        // transformer work
        throw std::runtime_error(
            "This path should never be called in createInternalParallelism. "
            "Please report to the developers.");
      }
    }
  }

 private:
  ad_utility::Timer _timer;
  // the unique_ptrs to our Transformers
  using uniquePtrTuple = toUniquePtrTuple_t<FirstTransformer, Transformers...>;
  // raw non-owning pointers to the transformers
  using rawPtrTuple = toRawPtrTuple_t<uniquePtrTuple>;
  uniquePtrTuple _transformers;
  rawPtrTuple _rawTransformers;
  std::unique_ptr<Producer> _producer;
  std::future<VecT<ResT>> _fut;
};

/*
 * Factory function for the BatchedPipeline class
 * That allows us to explicitly specify the Parallelism parameter
 * and implicitly specify the other template types by the constructor
 * arguments.
 */
template <size_t Parallelism, class Producer, class Transformer,
          class... OtherTransformers>
auto makeBatchedPipeline(Producer&& p, Transformer&& t,
                         OtherTransformers&&... other) {
  return BatchedPipeline<Parallelism, std::decay_t<Producer>,
                         std::decay_t<Transformer>,
                         std::decay_t<OtherTransformers>...>(
      std::forward<Producer>(p), std::forward<Transformer>(t),
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
    return makeBatchedPipeline<NextParallelism>(
        std::forward<SoFar>(sofar),
        std::forward<NextTransformer>(transformers)...);
  };

  return setupParallelPipelineRecursive<Parallelisms...>(
      std::apply(lambda, std::move(next)),
      std::forward<MoreTransformers>(transformers)...);
}

class Interface;  // forward declaration needed below for friend declaration

}  // namespace detail

/* Holds a Pipeline (Object on which we can call "produceBatch" and get a
 * VecT<ValueType> and extracts its elements one by one. Internally buffers one
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
 public:
  /// The type of our elements after all transformations were applied
  using ValueT =
      std::decay_t<decltype(std::declval<Pipeline>().produceBatch().second[0])>;

  // Retrieve and return the next triple from the internal buffer.
  // If the buffer is exhausted blocks
  // until the (asynchronous) call to _pipeline.produceBatch() has finished. A
  // nullopt signals the parser has completely parsed the file.
  /// get the next completely transformed value from the pipeline. std::nullopt
  /// means that all elements have been extracted
  std::optional<ValueT> getNextValue() {
    // we return the elements in order
    if (_buffer.size() == _bufferPosition && _isPipelineValid) {
      // we have to wait for the next parallel batch to be completed.
      _timer.cont();
      std::tie(_isPipelineValid, _buffer) = _fut.get();
      _timer.stop();
      _bufferPosition = 0;
      if (_isPipelineValid) {
        // if possible, directly submit the next parsing job
        _fut = std::async(std::launch::async, [ptr = _pipeline.get()]() {
          return ptr->produceBatch();
        });
      }
    }
    // we now should have some values in our buffer
    if (_bufferPosition < _buffer.size()) {
      return std::move(_buffer[_bufferPosition++]);
    } else {
      // we can only reach this if the buffer is exhausted and there is nothing
      // more to parse
      return std::nullopt;
    }
  }

  /**
   * @brief for all steps in the pipeline, report how long their calls to
   * produceBatch were blocking. TODO<joka921>: how useful is this measure?
   */
  [[nodiscard]] std::vector<off_t> getWaitingTime() const {
    auto res = _pipeline->getWaitingTime();
    res.push_back(_timer.msecs());
    return res;
  }

  /// return the batchSize
  [[nodiscard]] size_t getBatchSize() const { return _pipeline.getBatchSize(); }

 private:
  ad_utility::Timer _timer;
  std::unique_ptr<Pipeline> _pipeline;
  std::future<detail::VecT<ValueT>> _fut;
  std::vector<ValueT> _buffer;
  size_t _bufferPosition = 0;
  bool _isPipelineValid = true;

  // Pipelines are move-only types
  explicit BatchExtractor(Pipeline&& pipeline)
      : _pipeline(std::make_unique<Pipeline>(std::move(pipeline))) {
    _fut = std::async(std::launch::async, [ptr = _pipeline.get()]() {
      return ptr->produceBatch();
    });
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
 * @brief setup a pipeline that efficiently creates and transforms values. The
 * Concurrency is used between the different levels
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
 * Concurrency is used between and within the different levels
 *
 * In general this works similar to the setupPipeline() function template. Make
 * sure to read and understand the corresponding documentation first. It has the
 * following difference::
 *
 * With each Transformer there is a degree of Parallelism (a size_t) associated.
 * If this is degree is 1, this level behaves exactly the same as in the
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
 * the tuple are compatible, the return type of this level is derived from the
 * first tuple element. The following guarantee holds: If two (not necessary
 * consecutive) levels i and j have the same degree of parallelism p > 1  and
 * both specify a tuple of p function objects then the elements that are
 * transformed by the k-th tuple element on level i are also transformed by the
 * k-th tuple element on level j.
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
