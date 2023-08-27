//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>

#include <util/Rtree.h>
#include <util/RtreeFileReader.h>

FileReader::FileReader(const std::string& filePath) {
  this->filePath = filePath;

  this->file = std::ifstream(this->filePath, std::ios::binary);
  this->file.seekg (0, std::ifstream::end);
  this->fileLength = this->file.tellg();
  this->file.seekg (0, std::ifstream::beg);
}

std::optional<RTreeValueWithOrderIndex> FileReader::GetNextElement() {
  if (static_cast<uint64_t>(this->file.tellg()) < this->fileLength) {
    double minX;
    double minY;
    double maxX;
    double maxY;
    uint64_t id;
    uint64_t orderX;
    uint64_t orderY;

    this->file.read(reinterpret_cast<char*>(&minX), sizeof(double));
    this->file.read(reinterpret_cast<char*>(&minY), sizeof(double));
    this->file.read(reinterpret_cast<char*>(&maxX), sizeof(double));
    this->file.read(reinterpret_cast<char*>(&maxY), sizeof(double));
    this->file.read(reinterpret_cast<char*>(&id), sizeof(uint64_t));
    this->file.read(reinterpret_cast<char*>(&orderX), sizeof(uint64_t));
    this->file.read(reinterpret_cast<char*>(&orderY), sizeof(uint64_t));

    Rtree::BoundingBox box = Rtree::createBoundingBox(minX, minY, maxX, maxY);
    RTreeValueWithOrderIndex element = {box, id, orderX, orderY};

    return { element };
  } else {
    return {};
  }
}

void FileReader::Close() {
  this->file.close();
}

FileReaderWithoutIndex::FileReaderWithoutIndex(const std::string& filePath) {
  this->filePath = filePath;

  this->file = std::ifstream(this->filePath, std::ios::binary);
  this->file.seekg (0, std::ifstream::end);
  this->fileLength = this->file.tellg();
  this->file.seekg (0, std::ifstream::beg);
}

std::optional<RTreeValue> FileReaderWithoutIndex::GetNextElement() {
  if (static_cast<uint64_t>(this->file.tellg()) < this->fileLength) {
    double minX;
    double minY;
    double maxX;
    double maxY;
    uint64_t id;

    this->file.read(reinterpret_cast<char*>(&minX), sizeof(double));
    this->file.read(reinterpret_cast<char*>(&minY), sizeof(double));
    this->file.read(reinterpret_cast<char*>(&maxX), sizeof(double));
    this->file.read(reinterpret_cast<char*>(&maxY), sizeof(double));
    this->file.read(reinterpret_cast<char*>(&id), sizeof(uint64_t));

    Rtree::BoundingBox box = Rtree::createBoundingBox(minX, minY, maxX, maxY);
    RTreeValue boxWithId = {box, id};

    return { boxWithId };
  } else {
    return {};
  }
}

void FileReaderWithoutIndex::Close() {
  this->file.close();
}
