//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "./Index.h"
#include "./IndexImpl.h"
#include "./TriplesView.h"

/// Dump a  certain permutation to stdout in a human-readable way as IDs, and
/// in deterministic order
template <typename Permutation>
void dumpToStdout(const Permutation& permutation) {
  ad_utility::AllocatorWithLimit<Id> allocator{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(
          std::numeric_limits<uint64_t>::max())};
  auto triples = TriplesView(permutation, allocator);
  size_t i = 0;
  for (auto triple : triples) {
    std::cout << triple[0] << " " << triple[1] << " " << triple[2] << std::endl;
    ++i;
    if (i % (1ul << 20) == 0) {
      LOG(INFO) << "Exported " << i << " relations" << std::endl;
    }
  }
}

/// Load a certain permutation from a certain index, and dump it to stdout
/// in a human-readable and deterministic way. This can be used for large
/// regression tests, when the index format or the index building procedure
/// changes.
/// Args: ./PermutationExporterMain <indexBasename> <permutation>
///  (<permutation>  must be one of pso, pos, spo, sop, osp, ops
int main(int argc, char** argv) {
  // Actual output goes to std::cout,output of LOG(...) to std::cerr
  ad_utility::setGlobalLoggingStream(&std::cerr);

  if (argc != 3) {
    LOG(ERROR) << "Usage: PermutationExporterMain <indexBasename> <permutation "
                  "to dump>"
               << std::endl;
    return EXIT_FAILURE;
  }

  Index i{ad_utility::makeUnlimitedAllocator<Id>()};
  IndexImpl& impl = i.getImpl();
  std::string indexName{argv[1]};
  std::string p{argv[2]};

  if (p == "sop") {
    impl.SOP().loadFromDisk(indexName);
    dumpToStdout(impl.SOP());
    return EXIT_SUCCESS;
  }
  if (p == "spo") {
    impl.SPO().loadFromDisk(indexName);
    dumpToStdout(impl.SPO());
    return EXIT_SUCCESS;
  }
  if (p == "osp") {
    impl.OSP().loadFromDisk(indexName);
    dumpToStdout(impl.OSP());
    return EXIT_SUCCESS;
  }
  if (p == "ops") {
    impl.OPS().loadFromDisk(indexName);
    dumpToStdout(impl.OPS());
    return EXIT_SUCCESS;
  }

  if (p == "pos") {
    impl.POS().loadFromDisk(indexName);
    dumpToStdout(impl.POS());
    return EXIT_SUCCESS;
  }

  if (p == "pso") {
    impl.PSO().loadFromDisk(indexName);
    dumpToStdout(impl.PSO());
    return EXIT_SUCCESS;
  }

  LOG(ERROR)
      << "<permutation> must be one of pso, pos, spo, sop, osp, ops, but was \""
      << p << "\"" << std::endl;
  return EXIT_FAILURE;
}
