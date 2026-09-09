// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/RdfAsyncParallelParser.h"

#include <string>

#include "parser/Tokenizer.h"
#include "parser/TokenizerCtre.h"

// ____________________________________________________________________________
template <typename Parser>
RdfAsyncParallelParser<Parser>::RdfAsyncParallelParser(
    const ql::any_io_executor& executor,
    const qlever::InputFileSpecification& spec,
    ad_utility::MemorySize blocksize,
    const EncodedIriManager* encodedIriManager,
    const TripleComponent& defaultGraphIri)
    : executor_{executor},
      state_{encodedIriManager, defaultGraphIri},
      blockSource_{executor, spec.makeAsyncBlockSource(executor, blocksize),
                   detail::findEndOfLastStatement,
                   std::string{detail::statementBoundaryDescription}},
      blockFetchPermit_{executor, 1} {
  parseHeaderAsync();
}

// ____________________________________________________________________________
template <typename Parser>
void RdfAsyncParallelParser<Parser>::parseHeaderAsync() {
  namespace net = boost::asio;
  blockFetchPermit_.asyncAcquire(net::bind_executor(
      executor_, [this](const boost::system::error_code& errorCode,
                        Permit permit) mutable {
        // Nothing ever cancels `blockFetchPermit_`, so acquiring a permit
        // cannot fail. In particular this is the very first acquisition.
        AD_CORRECTNESS_CHECK(!errorCode && permit.isValid());
        continueParsingHeader(std::move(permit));
      }));
}

// ____________________________________________________________________________
template <typename Parser>
void RdfAsyncParallelParser<Parser>::continueParsingHeader(Permit permit) {
  namespace net = boost::asio;
  blockSource_.asyncGetNextBlock(net::bind_executor(
      executor_, [this, permit = std::move(permit)](
                     std::exception_ptr fetchEptr,
                     std::optional<qlever::parser::ByteBlock> block) mutable {
        try {
          if (fetchEptr) {
            std::rethrow_exception(fetchEptr);
          }
          if (state_.parseHeaderStep(std::move(block))) {
            // The declarations span more than the blocks that we have seen so
            // far. Keep holding the permit (so that no `asyncGetBatch()` call
            // can interleave) and fetch the next block.
            continueParsingHeader(std::move(permit));
            return;
          }
        } catch (...) {
          initializationError_ = std::current_exception();
        }
        // The header is complete (or has failed); let the waiting
        // `asyncGetBatch()` calls proceed.
        permit.release();
      }));
}

template class RdfAsyncParallelParser<TurtleParser<Tokenizer>>;
template class RdfAsyncParallelParser<TurtleParser<TokenizerCtre>>;
template class RdfAsyncParallelParser<NQuadParser<Tokenizer>>;
template class RdfAsyncParallelParser<NQuadParser<TokenizerCtre>>;
