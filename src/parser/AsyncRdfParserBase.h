// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_ASYNCRDFPARSERBASE_H
#define QLEVER_SRC_PARSER_ASYNCRDFPARSERBASE_H

#include <absl/functional/any_invocable.h>

#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/dispatch.hpp>
#include <exception>
#include <optional>
#include <utility>
#include <vector>

#include "backports/asio.h"
#include "parser/RdfParser.h"

// Abstract base class for RDF parsers that deliver their batches of triples
// asynchronously via `boost::asio`. This is the asynchronous counterpart of
// `RdfParserBase`: instead of a blocking `getBatch()`, it has an
// `asyncGetBatch()` operation that accepts an arbitrary Asio completion token.
// Derived classes own no threads of their own, but schedule all of their work
// on the executor that is passed to the constructor. The parallelism therefore
// comes entirely from the caller keeping several `asyncGetBatch()` calls in
// flight at once, see `IndexImpl::buildPartialVocabularies` for the main use
// case.
//
// NOTE: This class deliberately only uses features of `boost::asio` that are
// available in Boost 1.71 (in particular no coroutines and no
// `experimental::channel`), because it is also part of the
// `REDUCED_FEATURE_SET_FOR_CPP17` build, see `AsyncSerialParserAdapter`.
class AsyncRdfParserBase {
 public:
  // The result of a single `asyncGetBatch()` call: the triples of the next
  // batch, or `nullopt` at the end of the input (see `asyncGetBatch` below).
  using OptionalTriples = std::optional<std::vector<TurtleTriple>>;

  // The completion handler signature for `asyncGetBatchImpl`. It is called
  // exactly once, from any thread, see `asyncGetBatch` below for the meaning of
  // the arguments.
  using Handler = absl::AnyInvocable<void(std::exception_ptr, OptionalTriples)>;

 private:
  ql::any_io_executor executor_;

 public:
  // `executor` is the executor on which the derived class schedules its work,
  // and onto which the completion handlers are dispatched if the completion
  // token passed to `asyncGetBatch` has no executor of its own associated with
  // it.
  explicit AsyncRdfParserBase(const ql::any_io_executor& executor)
      : executor_{executor} {}
  virtual ~AsyncRdfParserBase() = default;

  // The executor that was passed to the constructor.
  const ql::any_io_executor& executor() const { return executor_; }

  // Asynchronously fetch and parse the next batch of triples. Accept any Asio
  // completion token (e.g. `boost::asio::use_future`, `use_awaitable`, or a
  // plain callable). The completion signature is
  // `void(std::exception_ptr, OptionalTriples)`: a null `exception_ptr`
  // together with a non-null optional signals success; a null `exception_ptr`
  // together with `nullopt` signals the end of the input; a non-null
  // `exception_ptr` signals that parsing this batch failed. After a failure,
  // all subsequent calls complete with `nullopt` (a clean end of the input) so
  // that the pipeline of the caller stops without reporting the same error
  // several times.
  //
  // Concurrent calls are allowed and are the intended way of using this
  // class; the derived classes take care of the necessary synchronization.
  // There is no guarantee about the order in which concurrent calls complete.
  // An instance must outlive all of its in-flight calls.
  template <typename CompletionToken>
  auto asyncGetBatch(CompletionToken&& token) {
    namespace net = boost::asio;
    return net::async_initiate<CompletionToken,
                               void(std::exception_ptr, OptionalTriples)>(
        [this](auto handler) mutable {
          auto ex = net::get_associated_executor(handler, executor_);
          asyncGetBatchImpl([h = std::move(handler), ex](
                                std::exception_ptr ep,
                                OptionalTriples batch) mutable {
            net::dispatch(
                ex, [h = std::move(h), ep, batch = std::move(batch)]() mutable {
                  std::move(h)(ep, std::move(batch));
                });
          });
        },
        // NOTE: `async_initiate` always takes its token as an lvalue, see the
        // corresponding comment in `AsyncBlockSource::asyncGetNextBlock`.
        token);
  }

 protected:
  // The single extension point required from every derived class. Must invoke
  // `handler` exactly once (asynchronously, from any thread), see
  // `asyncGetBatch` for the semantics of the arguments. Must never throw, but
  // report errors via the `exception_ptr` argument of the handler.
  virtual void asyncGetBatchImpl(Handler handler) = 0;
};

#endif  // QLEVER_SRC_PARSER_ASYNCRDFPARSERBASE_H
