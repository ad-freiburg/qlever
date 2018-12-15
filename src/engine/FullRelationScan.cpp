// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include "FullRelationScan.h"

FullRelationScan::FullRelationScan(QueryExecutionContext* qec, ScanType type,
                                   const std::string& entity_var_name,
                                   const std::string& count_var_name)
    : Operation(qec),
      _type(type),
      _entity_var_name(entity_var_name),
      _count_var_name(count_var_name) {}

string FullRelationScan::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  switch (_type) {
    case ScanType::SUBJECT:
      os << "FULL_RELATION_SCAN for subjects.";
      break;
    case ScanType::PREDICATE:
      os << "FULL_RELATION_SCAN for predicates.";
      break;
    case ScanType::OBJECT:
      os << "FULL_RELATION_SCAN for objects.";
      break;
  }
  return os.str();
}

size_t FullRelationScan::getResultWidth() const { return 2; }

vector<size_t> FullRelationScan::resultSortedOn() const { return {0}; }

ad_utility::HashMap<string, size_t> FullRelationScan::getVariableColumns()
    const {
  ad_utility::HashMap<string, size_t> varCols;
  varCols[_entity_var_name] = 0;
  varCols[_count_var_name] = 1;
  return varCols;
}

void FullRelationScan::setTextLimit(size_t limit) {}

bool FullRelationScan::knownEmptyResult() { return false; }

float FullRelationScan::getMultiplicity(size_t col) { return 1; }

size_t FullRelationScan::getSizeEstimate() {
  switch (_type) {
    case ScanType::SUBJECT:
      return getIndex().getNofSubjects();
    case ScanType::PREDICATE:
      return getIndex().getNofPredicates();
    case ScanType::OBJECT:
      return getIndex().getNofObjects();
  }
  return 0;
}

size_t FullRelationScan::getCostEstimate() { return getSizeEstimate(); }

void FullRelationScan::computeResult(ResultTable* result) const {
  result->_nofColumns = getResultWidth();
  result->_sortedBy = resultSortedOn();
  computeFullScan(result, getIndex(), _type);
  result->finish();
}

void FullRelationScan::computeFullScan(ResultTable* result, const Index& index,
                                       FullRelationScan::ScanType type) {
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);
  std::vector<std::array<Id, 2>>* fixedSizeData =
      new std::vector<std::array<Id, 2>>();
  result->_fixedSizeData = fixedSizeData;
  if (type == FullRelationScan::ScanType::PREDICATE) {
    fixedSizeData->reserve(index.getNofPredicates());
    const IndexMetaDataHmap& meta = index.getPsoMeta();
    for (const std::pair<size_t, const FullRelationMetaData&> elem :
         meta.data()) {
      fixedSizeData->push_back({elem.first, elem.second.getNofElements()});
    }
  } else {
    const IndexMetaDataMmapView* meta;
    if (type == FullRelationScan::ScanType::SUBJECT) {
      fixedSizeData->reserve(index.getNofSubjects());
      meta = &index.getSpoMeta();
    } else if (type == FullRelationScan::ScanType::OBJECT) {
      fixedSizeData->reserve(index.getNofObjects());
      meta = &index.getOpsMeta();
    } else {
      AD_THROW(ad_semsearch::Exception::INVALID_PARAMETER_VALUE,
               "FullRelationScan called with an unsupported scan type : " +
                   std::to_string(static_cast<int>(type)));
    }

    for (const std::pair<size_t, const FullRelationMetaData&> elem :
         meta->data()) {
      fixedSizeData->push_back({elem.first, elem.second.getNofElements()});
    }
  }
}

FullRelationScan::ScanType FullRelationScan::getType() const { return _type; }
