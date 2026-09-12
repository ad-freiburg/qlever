// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_ASYNCSERIALPARSERADAPTER_H
#define QLEVER_SRC_PARSER_ASYNCSERIALPARSERADAPTER_H

#include <absl/functional/any_invocable.h>

#include <boost/asio/strand.hpp>
#include <memory>

#include "backports/asio.h"
#include "parser/AsyncRdfParserBase.h"
#include "parser/RdfParser.h"

// Adapt a synchronous `RdfParserBase` to the asynchronous `AsyncRdfParserBase`
// interface. The calls to `getBatch()` of the wrapped parser run one at a time
// on a strand of the executor, so concurrent `asyncGetBatch()` calls are safe
// even for parsers that are not thread-safe (e.g. `RdfStreamParser`), and a
// call that has to wait for a previous one is queued on the strand instead of
// blocking a thread. Note that the parsing itself still blocks the thread of
// the executor that happens to run it, so the parallelism of this class is
// limited to one call at a time; it is meant for inputs that cannot be parsed
// in parallel (see `RdfAsyncMultifileParser`), and as the fallback for the
// `REDUCED_FEATURE_SET_FOR_CPP17` build, which has no coroutines and hence
// cannot use `RdfAsyncMultifileParser`.
class AsyncSerialParserAdapter : public AsyncRdfParserBase {
 public:
  // A factory for the wrapped parser, see the constructor below. The `&&` in
  // the signature expresses that it is called (and thus destroyed) exactly
  // once.
  using ParserFactory = absl::AnyInvocable<std::unique_ptr<RdfParserBase>() &&>;

 private:
  boost::asio::strand<ql::any_io_executor> strand_;
  ParserFactory parserFactory_;
  // The wrapped parser, created by the first `asyncGetBatch()` call, see the
  // constructor below. Only accessed from within `strand_`, so no further
  // synchronization is needed.
  std::unique_ptr<RdfParserBase> parser_;
  // Set once the wrapped parser has reported the end of the input or has
  // thrown. All subsequent calls complete with `nullopt`. Only accessed from
  // within `strand_`, so no further synchronization is needed.
  bool finished_ = false;

 public:
  // Wrap the parser that `parserFactory` creates. All of its `getBatch()`
  // calls will be run on a strand of `executor`.
  //
  // NOTE: The parser is deliberately not passed in ready-made, but created
  // lazily on `strand_` by the first `asyncGetBatch()` call. The constructors
  // of the synchronous parsers are expensive (that of `RdfStreamParser` even
  // reads the first block of the input), which would make this constructor
  // expensive as well. Callers rely on it being cheap;
  // `RdfAsyncMultifileParser` for example creates its per-file parsers while
  // holding a lock. An exception from `parserFactory` is reported like a parse
  // error, that is, through the completion handler of that first call.
  AsyncSerialParserAdapter(const ql::any_io_executor& executor,
                           ParserFactory parserFactory);

 protected:
  void asyncGetBatchImpl(Handler handler) override;
};

#endif  // QLEVER_SRC_PARSER_ASYNCSERIALPARSERADAPTER_H
