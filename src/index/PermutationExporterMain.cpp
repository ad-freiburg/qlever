//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "./Index.h"
#include "./MetaDataIterator.h"

/// Dump a  certain permutation to stdout in a human-readable way as IDs, and
/// in deterministic order
template <typename Permutation>
void dumpToStdout(const Permutation& permutation) {
  MetaDataIterator it{permutation._meta, permutation._file};
  while (!it.empty()) {
    auto triple = *it;
    std::cout << triple[0] << " " << triple[1] << " " << triple[2] << std::endl;
    ++it;
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
  ad_utility::setGlobalLogginStream(&std::cerr);

  if (argc != 3) {
    LOG(ERROR) << "Usage: PermutationExporterMain <indexBasename> <permutation "
                  "to dump>"
               << std::endl;
    return EXIT_FAILURE;
  }

  Index i;
  std::string indexName{argv[1]};
  std::string p{argv[2]};
  if (p == "sop") {
    i.SOP().loadFromDisk(indexName);
    dumpToStdout(i.SOP());
    return EXIT_SUCCESS;
  }
  if (p == "spo") {
    i.SPO().loadFromDisk(indexName);
    dumpToStdout(i.SPO());
    return EXIT_SUCCESS;
  }
  if (p == "osp") {
    i.OSP().loadFromDisk(indexName);
    dumpToStdout(i.OSP());
    return EXIT_SUCCESS;
  }
  if (p == "ops") {
    i.OPS().loadFromDisk(indexName);
    dumpToStdout(i.OPS());
    return EXIT_SUCCESS;
  }

  if (p == "pos") {
    i.POS().loadFromDisk(indexName);
    dumpToStdout(i.POS());
    return EXIT_SUCCESS;
  }

  if (p == "pso") {
    i.PSO().loadFromDisk(indexName);
    dumpToStdout(i.PSO());
    return EXIT_SUCCESS;
  }

  LOG(ERROR)
      << "<permutation> must be one of pso, pos, spo, sop, osp, ops, but was \""
      << p << "\"" << std::endl;
  return EXIT_FAILURE;
}
