//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>

#include "./RtreeFileReader.h"

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/vector.hpp>

#include "./Rtree.h"

FileReader::iterator& FileReader::iterator::operator++() {
  BasicGeometry::BoundingBox box;
  uint64_t id;
  uint64_t orderX;
  uint64_t orderY;
  if (input_ &&
      input_.read(reinterpret_cast<char*>(&box),
                  sizeof(BasicGeometry::BoundingBox)) &&
      input_.read(reinterpret_cast<char*>(&id), sizeof(uint64_t)) &&
      input_.read(reinterpret_cast<char*>(&orderX), sizeof(uint64_t)) &&
      input_.read(reinterpret_cast<char*>(&orderY), sizeof(uint64_t))) {
    currentElement_ = {{box, id}, orderX, orderY};
    valid_ = true;
  } else {
    valid_ = false;
  }
  return *this;
}

FileReaderWithoutIndex::iterator&
FileReaderWithoutIndex::iterator::operator++() {
  BasicGeometry::BoundingBox box;
  uint64_t id;
  if (input_ &&
      input_.read(reinterpret_cast<char*>(&box),
                  sizeof(BasicGeometry::BoundingBox)) &&
      input_.read(reinterpret_cast<char*>(&id), sizeof(uint64_t))) {
    currentElement_ = {box, id};
    valid_ = true;
  } else {
    valid_ = false;
  }
  return *this;
}

uint64_t FileReader::SaveNode(RtreeNode& node, std::ofstream& nodesOfs) {
  uint64_t pos = static_cast<uint64_t>(nodesOfs.tellp());
  boost::archive::binary_oarchive archive(nodesOfs);
  archive << node;
  nodesOfs.write(" ", 1);

  return pos;
}

RtreeNode FileReader::LoadNode(uint64_t id, std::ifstream& lookupIfs,
                               std::ifstream& nodesIfs) {
  RtreeNode newNode;

  uint64_t offset = id * (uint64_t)sizeof(uint64_t);
  lookupIfs.seekg((long long)offset, std::ios::beg);

  uint64_t nodePtr;
  lookupIfs.read(reinterpret_cast<char*>(&nodePtr), sizeof(uint64_t));

  nodesIfs.seekg((long long)nodePtr);
  boost::archive::binary_iarchive ia(nodesIfs);
  ia >> newNode;

  return newNode;
}

void FileReaderWithoutIndex::SaveEntry(BasicGeometry::BoundingBox boundingBox,
                                       uint64_t index,
                                       std::ofstream& convertOfs) {
  /**
   * Save a single entry (which was e.g. converted by ConvertWordToRtreeEntry)
   * to the disk
   */
  static_assert(std::is_trivially_copyable_v<BasicGeometry::BoundingBox>);
  convertOfs.write(reinterpret_cast<const char*>(&boundingBox),
                   sizeof(BasicGeometry::BoundingBox));
  convertOfs.write(reinterpret_cast<const char*>(&index), sizeof(uint64_t));
}

void FileReader::SaveEntryWithOrderIndex(RTreeValueWithOrderIndex treeValue,
                                         std::ofstream& convertOfs) {
  /**
   * Save a single entry, containing its postion in the x- and y-sorting
   */

  static_assert(std::is_trivially_copyable_v<BasicGeometry::BoundingBox>);
  convertOfs.write(reinterpret_cast<const char*>(&treeValue.box),
                   sizeof(BasicGeometry::BoundingBox));
  convertOfs.write(reinterpret_cast<const char*>(&treeValue.id),
                   sizeof(uint64_t));
  convertOfs.write(reinterpret_cast<const char*>(&treeValue.orderX),
                   sizeof(uint64_t));
  convertOfs.write(reinterpret_cast<const char*>(&treeValue.orderY),
                   sizeof(uint64_t));
}

multiBoxGeo FileReaderWithoutIndex::LoadEntries(
    const std::filesystem::path& file) {
  multiBoxGeo boxes;

  for (const RTreeValue& element : FileReaderWithoutIndex(file)) {
    boxes.push_back(element);
  }

  return boxes;
}

multiBoxWithOrderIndex FileReader::LoadEntriesWithOrderIndex(
    const std::filesystem::path& file) {
  multiBoxWithOrderIndex boxes;

  for (const RTreeValueWithOrderIndex& element : FileReader(file)) {
    boxes.push_back(element);
  }

  return boxes;
}
