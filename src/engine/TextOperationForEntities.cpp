// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <sstream>
#include <unordered_map>
#include "./QueryExecutionTree.h"
#include "TextOperationForEntities.h"


using std::string;
using std::unordered_map;

// _____________________________________________________________________________
size_t TextOperationForEntities::getResultWidth() const {
  size_t width = 3 + _freeVars;
  for (size_t i = 0; i < _subtrees.size(); ++i) {
    width += _subtrees[i].first.getRootOperation()->getResultWidth();
  }
  return width;
}

// _____________________________________________________________________________
TextOperationForEntities::TextOperationForEntities(
    QueryExecutionContext *qec,
    const string& words,
    const vector<pair<QueryExecutionTree, size_t>>& subtrees,
    size_t textLimit,
    size_t nofFreeVars) :
    Operation(qec), _words(words), _subtrees(subtrees), _freeVars(nofFreeVars) {
  setTextLimit(textLimit);
}

// _____________________________________________________________________________
string TextOperationForEntities::asString() const {
  std::ostringstream os;
  os << "TEXT OPERATION FOR ENTITIES:" << " co-occurrence with words: \"" <<
  _words << "\"";
  for (size_t i = 0; i < _subtrees.size(); ++i) {
    os << "\n\tand " << _subtrees[i].first.asString() << " [" <<
    _subtrees[i].second << "]";
  }
  os << " with textLimit = " << _textLimit;
  if (_freeVars > 0) {
    os << " and " << _freeVars << " free variables";
  }
  return os.str();
}

// _____________________________________________________________________________
void TextOperationForEntities::computeResult(ResultTable *result) const {
  LOG(DEBUG) << "TextOperationForEntities result computation..." << endl;
  if (_subtrees.size() == 0) {
    computeResultNoSubtrees(result);
  } else if (_subtrees.size() == 1) {
    computeResultOneSubtree(result);
  } else {
    computeResultMultSubtrees(result);
  }
  result->_status = ResultTable::FINISHED;
  LOG(DEBUG) << "TextOperationForEntities result computation done." << endl;
}

// _____________________________________________________________________________
void TextOperationForEntities::computeResultNoSubtrees(
    ResultTable *result) const {
  result->_nofColumns = 3 + _freeVars;
  if (_freeVars == 0) {
    result->_fixedSizeData = new vector<array<Id, 3>>;
    getExecutionContext()->getIndex().getECListForWords(
        _words,
        _textLimit,
        reinterpret_cast<vector<array<Id, 3>> *>(result->_fixedSizeData));
  } else if (_freeVars == 1) {
    result->_fixedSizeData = new vector<array<Id, 4>>;
    getExecutionContext()->getIndex().getECListForWords(
        _words,
        _textLimit,
        reinterpret_cast<vector<array<Id, 4>> *>(result->_fixedSizeData));
  } else if (_freeVars == 2) {
    result->_fixedSizeData = new vector<array<Id, 5>>;
    getExecutionContext()->getIndex().getECListForWords(
        _words,
        _textLimit,
        reinterpret_cast<vector<array<Id, 5>> *>(result->_fixedSizeData));
  } else {
    getExecutionContext()->getIndex().getECListForWords(
        _words,
        _textLimit,
        _freeVars,
        result->_varSizeData);
  }
}

// _____________________________________________________________________________
void TextOperationForEntities::computeResultOneSubtree(
    ResultTable *result) const {
  AD_CHECK(_subtrees.size() == 1);
  result->_nofColumns = 3 + _freeVars;
  for (size_t i = 0; i < _subtrees.size(); ++i) {
    result->_nofColumns +=
        _subtrees[i].first.getRootOperation()->getResultWidth();
  }
  if (result->_nofColumns == 4) {
    result->_fixedSizeData = new vector<array<Id, 4>>;
    AD_CHECK(_subtrees[0].first.getResult()._nofColumns == 1);
    AD_CHECK(_freeVars == 0);
    const Index::WidthOneList& subres =
        *static_cast<Index::WidthOneList *>(
            _subtrees[0].first.getResult()._fixedSizeData);
    getExecutionContext()->getIndex()
        .getECListForWordsAndSingleSub(_words,
                                       subres,
                                       _subtrees[0].second,
                                       _textLimit,
                                       *static_cast<vector<array<Id, 4>> *>(
                                           result->_fixedSizeData));
  } else if (result->_nofColumns == 5) {
    result->_fixedSizeData = new vector<array<Id, 5>>;
    if (_freeVars == 1) {
      const Index::WidthOneList& subres =
          *static_cast<Index::WidthOneList *>(
              _subtrees[0].first.getResult()._fixedSizeData);
      getExecutionContext()->getIndex()
          .getECListForWordsAndSingleSub(_words,
                                         subres,
                                         _subtrees[0].second,
                                         *static_cast<vector<array<Id, 5>> *>(
                                             result->_fixedSizeData));
    } else {
      const Index::WidthTwoList& subres =
          *static_cast<Index::WidthTwoList *>(
              _subtrees[0].first.getResult()._fixedSizeData);
      getExecutionContext()->getIndex()
          .getECListForWordsAndSingleSub(_words,
                                         subres,
                                         _subtrees[0].second,
                                         _textLimit,
                                         *static_cast<vector<array<Id, 5>> *>(
                                             result->_fixedSizeData));
    }
  } else {
    // Var size result.
    LOG(DEBUG) << "Transforming sub results into maps...\n";
    vector<unordered_map<Id, vector<vector<Id>>>> subResMaps;
    subResMaps.resize(_subtrees.size());
    for (size_t i = 0; i < _subtrees.size(); ++i) {
      const ResultTable& r = _subtrees[i].first.getResult();
      if (r._nofColumns > 5) {
        for (size_t j = 0; i < r._varSizeData.size(); ++j) {
          subResMaps[i][r._varSizeData[j][_subtrees[i].second]]
              .push_back(r._varSizeData[j]);
        }
      } else {
        const vector<vector<Id>> varR = r.getDataAsVarSize();
        for (size_t j = 0; j < varR.size(); ++j) {
          subResMaps[i][varR[j][_subtrees[i].second]].push_back(varR[j]);
        }
      }
    }
    LOG(DEBUG) << "Transformation into maps done.\n";
    getExecutionContext()->getIndex()
        .getECListForWordsAndSubtrees(_words,
                                      subResMaps,
                                      _textLimit,
                                      _freeVars,
                                      result->_varSizeData);
  }
}

// _____________________________________________________________________________
void TextOperationForEntities::computeResultMultSubtrees(
    ResultTable *result) const {
  result->_nofColumns = 3 + _freeVars;
  for (size_t i = 0; i < _subtrees.size(); ++i) {
    result->_nofColumns +=
        _subtrees[i].first.getRootOperation()->getResultWidth();
  }
  if (result->_nofColumns == 5) {
    AD_CHECK(_subtrees.size() == 2);
    AD_CHECK(_freeVars == 0);
    AD_CHECK(_subtrees[0].first.getResult()._nofColumns == 1);
    AD_CHECK(_subtrees[0].second == 0);
    AD_CHECK(_subtrees[1].first.getResult()._nofColumns == 1);
    AD_CHECK(_subtrees[1].second == 0);
    const Index::WidthOneList& subres1 =
        *static_cast<Index::WidthOneList *>(
            _subtrees[0].first.getResult()._fixedSizeData);
    const Index::WidthOneList& subres2 =
        *static_cast<Index::WidthOneList *>(
            _subtrees[1].first.getResult()._fixedSizeData);
    getExecutionContext()->getIndex()
        .getECListForWordsAndTwoW1Subs(_words,
                                       subres1,
                                       subres2,
                                       _textLimit,
                                       *static_cast<vector<array<Id, 5>> *>(
                                           result->_fixedSizeData));
  } else {
    // Var size result.
    LOG(DEBUG) << "Transforming sub results into maps...\n";
    vector<unordered_map<Id, vector<vector<Id>>>> subResMaps;
    subResMaps.resize(_subtrees.size());
    for (size_t i = 0; i < _subtrees.size(); ++i) {
      const ResultTable& r = _subtrees[i].first.getResult();
      if (r._nofColumns > 5) {
        for (size_t j = 0; i < r._varSizeData.size(); ++j) {
          subResMaps[i][r._varSizeData[j][_subtrees[i].second]]
              .push_back(r._varSizeData[j]);
        }
      } else {
        const vector<vector<Id>> varR = r.getDataAsVarSize();
        for (size_t j = 0; j < varR.size(); ++j) {
          subResMaps[i][varR[j][_subtrees[i].second]].push_back(varR[j]);
        }
      }
    }
    LOG(DEBUG) << "Transformation into maps done.\n";
    getExecutionContext()->getIndex()
        .getECListForWordsAndSubtrees(_words,
                                      subResMaps,
                                      _textLimit,
                                      _freeVars,
                                      result->_varSizeData);
  }
}
