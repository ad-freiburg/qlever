// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_INPUTFILESPECIFICATION_H
#define QLEVER_SRC_INDEX_INPUTFILESPECIFICATION_H

#include <optional>
#include <string>
namespace qlever {

// An enum to distinguish between `Turtle` and `NQuad` files.
enum class Filetype { Turtle, NQuad };

// Specify a single input file or stream for the index builder.
struct InputFileSpecification {
  std::string filename_;
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

  bool operator==(const InputFileSpecification&) const = default;
};
}  // namespace qlever

#endif  // QLEVER_SRC_INDEX_INPUTFILESPECIFICATION_H
