//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>

#ifndef QLEVER_RTREEFILEREADER_H
#define QLEVER_RTREEFILEREADER_H

#include <util/Rtree.h>

#include <boost/geometry.hpp>
#include <boost/serialization/split_free.hpp>
#include <boost/serialization/version.hpp>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <optional>
#include <vector>

class FileReader {
 public:
  explicit FileReader(const std::string& filename) : file(filename) {}

  class iterator : public std::iterator<std::input_iterator_tag, std::string> {
   public:
    explicit iterator(std::ifstream& in) : input(in) {
      ++(*this);  // Read the first element
    }

    iterator() : input(nullstream) {}  // End iterator constructor

    iterator& operator++() {
      double minX;
      double minY;
      double maxX;
      double maxY;
      uint64_t id;
      uint64_t orderX;
      uint64_t orderY;
      if (input && input.read(reinterpret_cast<char*>(&minX), sizeof(double)) &&
          input.read(reinterpret_cast<char*>(&minY), sizeof(double)) &&
          input.read(reinterpret_cast<char*>(&maxX), sizeof(double)) &&
          input.read(reinterpret_cast<char*>(&maxY), sizeof(double)) &&
          input.read(reinterpret_cast<char*>(&id), sizeof(uint64_t)) &&
          input.read(reinterpret_cast<char*>(&orderX), sizeof(uint64_t)) &&
          input.read(reinterpret_cast<char*>(&orderY), sizeof(uint64_t))) {
        Rtree::BoundingBox box =
            Rtree::createBoundingBox(minX, minY, maxX, maxY);
        currentElement = {{box, id}, orderX, orderY};
        valid = true;
      } else {
        valid = false;
      }
      return *this;
    }

    const RTreeValueWithOrderIndex& operator*() const { return currentElement; }

    bool operator!=(const iterator& other) const {
      return valid != other.valid;
    }

   private:
    std::ifstream& input;
    std::ifstream nullstream;  // A dummy stream for the end iterator
    RTreeValueWithOrderIndex currentElement;
    bool valid{};
  };

  iterator begin() { return iterator(file); }

  static iterator end() { return {}; }

 private:
  std::ifstream file;
};

class FileReaderWithoutIndex {
 public:
  explicit FileReaderWithoutIndex(const std::string& filename)
      : file(filename) {}

  class iterator : public std::iterator<std::input_iterator_tag, std::string> {
   public:
    explicit iterator(std::ifstream& in) : input(in) {
      ++(*this);  // Read the first element
    }

    iterator() : input(nullstream) {}  // End iterator constructor

    iterator& operator++() {
      double minX;
      double minY;
      double maxX;
      double maxY;
      uint64_t id;
      if (input && input.read(reinterpret_cast<char*>(&minX), sizeof(double)) &&
          input.read(reinterpret_cast<char*>(&minY), sizeof(double)) &&
          input.read(reinterpret_cast<char*>(&maxX), sizeof(double)) &&
          input.read(reinterpret_cast<char*>(&maxY), sizeof(double)) &&
          input.read(reinterpret_cast<char*>(&id), sizeof(uint64_t))) {
        Rtree::BoundingBox box =
            Rtree::createBoundingBox(minX, minY, maxX, maxY);
        currentElement = {box, id};
        valid = true;
      } else {
        valid = false;
      }
      return *this;
    }

    const RTreeValue& operator*() const { return currentElement; }

    bool operator!=(const iterator& other) const {
      return valid != other.valid;
    }

   private:
    std::ifstream& input;
    std::ifstream nullstream;  // A dummy stream for the end iterator
    RTreeValue currentElement;
    bool valid{};
  };

  iterator begin() { return iterator(file); }

  static iterator end() { return {}; }

 private:
  std::ifstream file;
};

#endif  // QLEVER_RTREEFILEREADER_H
