// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_COMBINEDRUNSINPUT_H
#define QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_COMBINEDRUNSINPUT_H

#include <cstddef>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/MemorySize/MemorySize.h"
#include "util/NoCopyNoMove.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

namespace ad_utility::parallelBlockMerge {

// The runs of several inputs of the same type (see the `InputConcept` in
// `RunsInputPolicy.h`), combined into a single input for the parallel block
// merge. The runs are numbered consecutively, in the order of the inputs. The
// output blocks are created by the first input.
template <typename Input>
class CombinedRunsInput : public ad_utility::NoCopy {
 public:
  using Block = typename Input::Block;
  using OutputBlock = detail::OutputBlockT<Input>;
  using Element = typename Input::Element;
  using value_type = typename Input::value_type;

 private:
  std::vector<Input> inputs_;
  // `runOffsets_[i]` is the number of runs of all the inputs before the `i`-th
  // one, so the last entry is the total number of runs.
  std::vector<size_t> runOffsets_;

 public:
  // Combine the `inputs`, of which there has to be at least one.
  explicit CombinedRunsInput(std::vector<Input> inputs)
      : inputs_{std::move(inputs)} {
    AD_CONTRACT_CHECK(!inputs_.empty());
    runOffsets_.push_back(0);
    for (const auto& input : inputs_) {
      runOffsets_.push_back(runOffsets_.back() + input.numRuns());
    }
  }

  // ___________________________________________________________________________
  size_t numRuns() const { return runOffsets_.back(); }

  // ___________________________________________________________________________
  size_t numBlocks(size_t run) const {
    auto [input, localRun] = locate(run);
    return input.numBlocks(localRun);
  }

  // ___________________________________________________________________________
  size_t numElementsInBlock(size_t run, size_t block) const {
    auto [input, localRun] = locate(run);
    return input.numElementsInBlock(localRun, block);
  }

  // ___________________________________________________________________________
  decltype(auto) firstElement(size_t run, size_t block) const {
    auto [input, localRun] = locate(run);
    return input.firstElement(localRun, block);
  }

  // ___________________________________________________________________________
  decltype(auto) lastElement(size_t run, size_t block) const {
    auto [input, localRun] = locate(run);
    return input.lastElement(localRun, block);
  }

  // ___________________________________________________________________________
  Block getBlock(size_t run, size_t block) const {
    auto [input, localRun] = locate(run);
    return input.getBlock(localRun, block);
  }

  // ___________________________________________________________________________
  OutputBlock makeEmptyBlock() const {
    return inputs_.front().makeEmptyBlock();
  }

  // ___________________________________________________________________________
  template <typename R>
  void appendToBlock(OutputBlock& block, R&& row) const {
    inputs_.front().appendToBlock(block, AD_FWD(row));
  }

  // ___________________________________________________________________________
  template <typename R>
  ad_utility::MemorySize memorySizeOfElement(const R& row) const {
    return inputs_.front().memorySizeOfElement(row);
  }

 private:
  // Return the input that the `run` belongs to, and the index of the run
  // within that input.
  std::pair<const Input&, size_t> locate(size_t run) const {
    AD_CORRECTNESS_CHECK(run < numRuns());
    auto it = ql::ranges::upper_bound(runOffsets_, run);
    size_t inputIdx = static_cast<size_t>(it - runOffsets_.begin()) - 1;
    return {inputs_.at(inputIdx), run - runOffsets_.at(inputIdx)};
  }
};

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_COMBINEDRUNSINPUT_H
