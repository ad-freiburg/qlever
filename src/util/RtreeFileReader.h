//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>

#ifndef QLEVER_RTREEFILEREADER_H
#define QLEVER_RTREEFILEREADER_H

#include "./Rtree.h"
#include "./RtreeNode.h"

class FileReader {
 public:
  // ___________________________________________________________________________
  // Save a single datapoint of the Rtree, together with its position in the x
  // and y sorting to disk
  static void SaveEntryWithOrderIndex(RTreeValueWithOrderIndex treeValue,
                                      std::ofstream& convertOfs);
  // ___________________________________________________________________________
  // Load all datapoints of the Rtree, together with its x and y sorting into
  // ram
  static multiBoxWithOrderIndex LoadEntriesWithOrderIndex(
      const std::filesystem::path& file);
  // ___________________________________________________________________________
  // Save the current node in the building process to disk and return the
  // position of the node in the file
  static uint64_t SaveNode(RtreeNode& node, std::ofstream& nodesOfs);
  // ___________________________________________________________________________
  // Load a specific RtreeNode to query in its children
  static RtreeNode LoadNode(uint64_t id, std::ifstream& lookupIfs,
                            std::ifstream& nodesIfs);

  explicit FileReader(const std::filesystem::path& filename)
      : file_(filename) {}

  class iterator
      : public std::iterator<std::input_iterator_tag, std::filesystem::path> {
   public:
    explicit iterator(std::ifstream& in) : input_(in) {
      ++(*this);  // Read the first element
    }

    iterator() : input_(nullstream_) {}  // End iterator constructor

    iterator& operator++();

    const RTreeValueWithOrderIndex& operator*() const {
      return currentElement_;
    }

    bool operator!=(const iterator& other) const {
      return valid_ != other.valid_;
    }

   private:
    std::ifstream& input_;
    std::ifstream nullstream_;  // A dummy stream for the end iterator
    RTreeValueWithOrderIndex currentElement_;
    bool valid_{};
  };

  iterator begin() { return iterator(file_); }

  static iterator end() { return {}; }

 private:
  std::ifstream file_;
};

class FileReaderWithoutIndex {
 public:
  // ___________________________________________________________________________
  // Save a single datapoint for the Rtree to disk
  static void SaveEntry(BasicGeometry::BoundingBox boundingBox, uint64_t index,
                        std::ofstream& convertOfs);
  // ___________________________________________________________________________
  // Load all datapoints of the Rtree in file into ram
  static multiBoxGeo LoadEntries(const std::filesystem::path& file);

  explicit FileReaderWithoutIndex(const std::filesystem::path& filename)
      : file_(filename) {}

  class iterator
      : public std::iterator<std::input_iterator_tag, std::filesystem::path> {
   public:
    explicit iterator(std::ifstream& in) : input_(in) {
      ++(*this);  // Read the first element
    }

    iterator() : input_(nullstream_) {}  // End iterator constructor

    iterator& operator++();

    const RTreeValue& operator*() const { return currentElement_; }

    bool operator!=(const iterator& other) const {
      return valid_ != other.valid_;
    }

   private:
    std::ifstream& input_;
    std::ifstream nullstream_;  // A dummy stream for the end iterator
    RTreeValue currentElement_;
    bool valid_{};
  };

  iterator begin() { return iterator(file_); }

  static iterator end() { return {}; }

 private:
  std::ifstream file_;
};

#endif  // QLEVER_RTREEFILEREADER_H
