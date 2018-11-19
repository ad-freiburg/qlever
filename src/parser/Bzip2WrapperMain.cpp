// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//
//
#include <fstream>
#include <iostream>
#include "./Bzip2Wrapper.h"

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "Usage: ./Bzip2WrapperMain <compressedFile> <outfile>";
    exit(1);
  }
  std::ofstream ofs(argv[2]);
  Bzip2Wrapper w;
  w.open(argv[1]);
  while (auto data = w.decompressBlock()) {
    ofs.write(data.value().data(), data.value().size());
  }
}
