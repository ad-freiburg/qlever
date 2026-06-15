//  Copyright 2015 - 2026 The QLever Authors, in particular:
//
//  2015 Björn Buchhold <buchhold@cs.uni-freiburg.de>, UFR
//  2015 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/IndexMetaData.h"

#include "global/Constants.h"
#include "util/File.h"
#include "util/ReadableNumberFacet.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeString.h"

// _____________________________________________________________________________
void IndexMetaData::add(AddType addedValue) {
  totalElements_ += addedValue.getNofElements();
  data_.add(addedValue);
}

// _____________________________________________________________________________
off_t IndexMetaData::getOffsetAfter() const { return offsetAfter_; }

// _____________________________________________________________________________
IndexMetaData::GetType IndexMetaData::getMetaData(Id col0Id) const {
  auto metaData = data_.getIfPresent(col0Id);
  AD_CONTRACT_CHECK(metaData.has_value());
  return metaData.value();
}

// _____________________________________________________________________________
bool IndexMetaData::col0IdExists(Id col0Id) const {
  return data_.getIfPresent(col0Id).has_value();
}

// ____________________________________________________________________________
void IndexMetaData::writeToFile(const std::string& filename) const {
  ad_utility::File permutationFile{filename, "w"};
  ad_utility::File metaFile{filename + META_FILE_SUFFIX, "w"};
  appendToFile(&permutationFile, &metaFile);
}

// ____________________________________________________________________________
void IndexMetaData::appendToFile(ad_utility::File* permutationFile,
                                 ad_utility::File* metaFile) const {
  AD_CONTRACT_CHECK(permutationFile->isOpen());
  // Write the per-relation metadata to its own file.
  data_.writeToFile(*metaFile);
  // Append the per-block metadata to the end of the permutation file.
  permutationFile->seek(0, SEEK_END);
  off_t startOfMeta = permutationFile->tell();
  ad_utility::serialization::FileWriteSerializer serializer{
      std::move(*permutationFile)};
  serializer << (*this);
  *permutationFile = std::move(serializer).file();
  permutationFile->write(&startOfMeta, sizeof(startOfMeta));
}

// _________________________________________________________________________
void IndexMetaData::readFromFile(const std::string& filename) {
  ad_utility::File permutationFile{filename, "r"};
  ad_utility::File metaFile{filename + META_FILE_SUFFIX, "r"};
  readFromFile(&permutationFile, &metaFile);
}

// _________________________________________________________________________
void IndexMetaData::readFromFile(ad_utility::File* permutationFile,
                                 ad_utility::File* metaFile) {
  // Read the per-relation metadata from its own file.
  data_.readFromFile(*metaFile);
  // Read the per-block metadata from the end of the permutation file.
  off_t metaFrom;
  off_t metaTo = permutationFile->getLastOffset(&metaFrom);
  std::vector<char> buf(metaTo - metaFrom);
  permutationFile->read(buf.data(), static_cast<size_t>(metaTo - metaFrom),
                        metaFrom);

  ad_utility::serialization::ByteBufferReadSerializer serializer{
      std::move(buf)};

  serializer >> (*this);
}

// _____________________________________________________________________________
std::string IndexMetaData::statistics() const {
  std::ostringstream os;
  std::locale loc;
  ad_utility::ReadableNumberFacet facet(1);
  std::locale locWithNumberGrouping(loc, &facet);
  os.imbue(locWithNumberGrouping);
  os << "#relations = " << numDistinctCol0_
     << ", #blocks = " << blockData().size()
     << ", #triples = " << totalElements_;
  return std::move(os).str();
}

// __________________________________________________________________
void IndexMetaData::calculateStatistics(size_t numDistinctCol0) {
  totalElements_ = 0;
  numDistinctCol0_ = numDistinctCol0;
  for (const auto& block : blockData()) {
    totalElements_ += block.numRows_;
  }
}

// _____________________________________________________________________________
void IndexMetaData::exchangeMultiplicities(IndexMetaData& other) {
  data_.exchangeMultiplicities(other.data_);
}
