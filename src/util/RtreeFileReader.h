//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>

#ifndef QLEVER_RTREEFILEREADER_H
#define QLEVER_RTREEFILEREADER_H

#include <vector>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <optional>
#include <limits>
#include <boost/geometry.hpp>
#include <boost/serialization/version.hpp>
#include <boost/serialization/split_free.hpp>
#include <util/Rtree.h>

class FileReader {
 private:
  std::string filePath;
  std::ifstream file;
  uint64_t fileLength;
 public:
  explicit FileReader(const std::string& filePath);
  std::optional<RTreeValueWithOrderIndex> GetNextElement();
  void Close();
};

class FileReaderWithoutIndex {
 private:
  std::string filePath;
  std::ifstream file;
  uint64_t fileLength;
 public:
  explicit FileReaderWithoutIndex(const std::string& filePath);
  std::optional<RTreeValue> GetNextElement();
  void Close();
};

#endif //QLEVER_RTREEFILEREADER_H
