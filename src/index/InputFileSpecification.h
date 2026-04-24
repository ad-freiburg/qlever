// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_INPUTFILESPECIFICATION_H
#define QLEVER_SRC_INDEX_INPUTFILESPECIFICATION_H

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <variant>

#include "parser/ParallelBuffer.h"

namespace qlever {

// An enum to distinguish between `Turtle` and `NQuad` files.
enum class Filetype { Turtle, NQuad };

// Specify a single input file or stream for the index builder. The source of
// bytes is either a filename or a factory that produces a `ParallelBuffer`.
struct InputFileSpecification {
  // A factory that, when called, creates a ready-to-use `ParallelBuffer`.
  // The `size_t` is the preferred blocksize for the `ParallelBuffer`, and the
  // `string_view` is a descriptor of the resource, which can be used for
  // logging and debugging.
  using ParallelBufferFactory =
      std::function<std::unique_ptr<ParallelBuffer>(size_t, std::string_view)>;

  struct BufferFactoryAndDescription {
    ParallelBufferFactory bufferFactory_;
    std::string description_;
  };
  // The byte source: either a filename or a factory.
  std::variant<std::string, BufferFactoryAndDescription> source_;

  Filetype filetype_;
  // All triples that don't have a dedicated graph (either because the input
  // format is N-Triples or Turtle, or because the corresponding line in the
  // N-Quads format has no explicit graph) will be stored in this graph. The
  // graph has to be specified without angle brackets. If set to `nullopt`, the
  // global default graph will be used.
  std::optional<std::string> defaultGraph_;

  // If set to `true`, then the parallel RDF parser will be used for this file.
  // This always works for N-Triples and N-Quads files, and for well-behaved
  // Turtle files with all prefixes at the beginning and no multiline literals.
  bool parseInParallel_ = false;

  // Remember if the value for parallel parsing was set explicitly (via the
  // command line).
  bool parseInParallelSetExplicitly_ = false;

  // Return the filename/description for the `source_`.
  const std::string& filename() const {
    return std::visit(
        [](const auto& arg) -> const auto& {
          if constexpr (ad_utility::isSimilar<std::string, decltype(arg)>) {
            return arg;
          } else {
            return arg.description_;
          }
        },
        source_);
  }

  // Create and return a `ParallelBuffer` for this spec. For filename-based
  // specs, a `ParallelFileBuffer` with the given `blocksize` is returned. For
  // factory-based specs, the factory is called.
  std::unique_ptr<ParallelBuffer> getParallelBuffer(size_t blocksize) const {
    if (std::holds_alternative<std::string>(source_)) {
      return std::make_unique<ParallelFileBuffer>(
          blocksize, std::get<std::string>(source_));
    }
    auto& [factory, description] =
        std::get<BufferFactoryAndDescription>(source_);
    return factory(blocksize, description);
  }
};
}  // namespace qlever

#endif  // QLEVER_SRC_INDEX_INPUTFILESPECIFICATION_H
