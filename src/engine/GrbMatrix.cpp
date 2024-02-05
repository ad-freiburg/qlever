// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.roland.herrmann@mars.uni-freiburg.de)

#include "GrbMatrix.h"

#include <GraphBLAS.h>

#include <memory>
#include <vector>

#include "util/Exception.h"

// _____________________________________________________________________________
GrbMatrix::GrbMatrix(size_t numRows, size_t numCols) {
  auto info = GrB_Matrix_new(rawMatrix(), GrB_BOOL, numRows, numCols);
  handleError(info);
}

// _____________________________________________________________________________
GrbMatrix GrbMatrix::clone() const {
  GrbMatrix matrixCopy;
  auto info =
      GrB_Matrix_new(matrixCopy.rawMatrix(), GrB_BOOL, numRows(), numCols());
  handleError(info);
  info = GrB_Matrix_dup(matrixCopy.rawMatrix(), matrix());
  handleError(info);

  return matrixCopy;
}

// _____________________________________________________________________________
void GrbMatrix::setElement(size_t row, size_t col, bool value) {
  auto info = GrB_Matrix_setElement_BOOL(matrix(), value, row, col);
  handleError(info);
}

// _____________________________________________________________________________
bool GrbMatrix::getElement(size_t row, size_t col) const {
  bool result;
  auto info = GrB_Matrix_extractElement_BOOL(&result, matrix(), row, col);
  handleError(info);
  if (info == GrB_NO_VALUE) {
    return false;
  }
  return result;
}

// _____________________________________________________________________________
GrbMatrix GrbMatrix::build(const std::vector<size_t>& rowIndices,
                           const std::vector<size_t>& colIndices,
                           size_t numRows, size_t numCols) {
  auto matrix = GrbMatrix(numRows, numCols);
  GrB_Index nvals = rowIndices.size();
  if (nvals == 0) {
    return matrix;
  }

  std::unique_ptr<bool[]> values{new bool[nvals]()};
  for (size_t i = 0; i < nvals; i++) {
    values[i] = true;
  }
  auto info = GrB_Matrix_build_BOOL(matrix.matrix(), rowIndices.data(),
                                    colIndices.data(), values.get(), nvals,
                                    GxB_IGNORE_DUP);
  GrbMatrix::handleError(info);
  return matrix;
}

// _____________________________________________________________________________
GrbMatrix GrbMatrix::diag(size_t nvals) {
  auto result = GrbMatrix(nvals, nvals);

  for (size_t i = 0; i < nvals; i++) {
    result.setElement(i, i, true);
  }

  return result;
}

// _____________________________________________________________________________
std::pair<std::vector<size_t>, std::vector<size_t>> GrbMatrix::extractTuples()
    const {
  size_t nvals = numNonZero();
  std::vector<size_t> rowIndices;
  rowIndices.resize(nvals);
  std::vector<size_t> colIndices;
  colIndices.resize(nvals);
  std::unique_ptr<bool[]> values{new bool[nvals]()};
  auto info = GrB_Matrix_extractTuples_BOOL(
      rowIndices.data(), colIndices.data(), values.get(), &nvals, matrix());
  handleError(info);

  return {rowIndices, colIndices};
}

// _____________________________________________________________________________
std::vector<size_t> GrbMatrix::extractColumn(size_t colIndex) const {
  return extract(colIndex, GrB_NULL);
}

// _____________________________________________________________________________
std::vector<size_t> GrbMatrix::extractRow(size_t rowIndex) const {
  // The descriptor GrB_DESC_T0 transposes the second input, which is the matrix
  return extract(rowIndex, GrB_DESC_T0);
}

// _____________________________________________________________________________
size_t GrbMatrix::numNonZero() const {
  size_t nvals;
  auto info = GrB_Matrix_nvals(&nvals, matrix());
  handleError(info);
  return nvals;
}

// _____________________________________________________________________________
size_t GrbMatrix::numRows() const {
  size_t nrows;
  auto info = GrB_Matrix_nrows(&nrows, matrix());
  handleError(info);
  return nrows;
}

// _____________________________________________________________________________
size_t GrbMatrix::numCols() const {
  size_t ncols;
  auto info = GrB_Matrix_ncols(&ncols, matrix());
  handleError(info);
  return ncols;
}

// _____________________________________________________________________________
GrbMatrix GrbMatrix::transpose() const {
  GrbMatrix transposed;
  auto info =
      GrB_Matrix_new(transposed.rawMatrix(), GrB_BOOL, numCols(), numRows());
  handleError(info);
  info = GrB_transpose(transposed.matrix(), GrB_NULL, GrB_NULL, matrix(),
                       GrB_NULL);
  handleError(info);

  return transposed;
}

// _____________________________________________________________________________
void GrbMatrix::accumulateMultiply(const GrbMatrix& otherMatrix) const {
  auto info = GrB_mxm(matrix(), GrB_NULL, GrB_LOR, GrB_LOR_LAND_SEMIRING_BOOL,
                      matrix(), otherMatrix.matrix(), GrB_NULL);
  handleError(info);
}

// _____________________________________________________________________________
GrbMatrix GrbMatrix::multiply(const GrbMatrix& otherMatrix) const {
  size_t resultNumRows = numRows();
  size_t resultNumCols = otherMatrix.numCols();
  GrbMatrix result;
  auto info = GrB_Matrix_new(result.rawMatrix(), GrB_BOOL, resultNumRows,
                             resultNumCols);

  info =
      GrB_mxm(result.matrix(), GrB_NULL, GrB_NULL, GrB_LOR_LAND_SEMIRING_BOOL,
              matrix(), otherMatrix.matrix(), GrB_NULL);
  handleError(info);

  return result;
}

// _____________________________________________________________________________
GrB_Matrix* GrbMatrix::rawMatrix() const {
  if (matrix_.get() != nullptr) {
    return matrix_.get();
  }
  AD_THROW("GrbMatrix error: internal GrB_Matrix is null");
}

// _____________________________________________________________________________
std::vector<size_t> GrbMatrix::extract(size_t index,
                                       GrB_Descriptor desc) const {
  GrB_Vector vector;
  size_t vectorSize;
  if (desc == GrB_NULL) {
    vectorSize = numRows();
  } else {
    vectorSize = numCols();
  }
  auto info = GrB_Vector_new(&vector, GrB_BOOL, vectorSize);
  handleError(info);

  info = GrB_Col_extract(vector, GrB_NULL, GrB_NULL, matrix(), GrB_ALL,
                         vectorSize, index, desc);
  handleError(info);

  size_t vectorNvals;
  info = GrB_Vector_nvals(&vectorNvals, vector);
  handleError(info);

  std::vector<size_t> indices;
  indices.resize(vectorNvals);
  info = GrB_Vector_extractTuples_BOOL(indices.data(), nullptr, &vectorNvals,
                                       vector);
  handleError(info);

  info = GrB_Vector_free(&vector);
  handleError(info);

  return indices;
}

// _____________________________________________________________________________
void GrbMatrix::handleError(GrB_Info info) {
  switch (info) {
    case GrB_SUCCESS:
      return;
    case GrB_NO_VALUE:
      return;
    case GrB_UNINITIALIZED_OBJECT:
      AD_THROW("GraphBLAS error: object has not been initialized");
    case GrB_NULL_POINTER:
      AD_THROW("GraphBLAS error: input pointer is NULL");
    case GrB_INVALID_VALUE:
      AD_THROW("GraphBLAS error: generic error code; some value is bad");
    case GrB_INVALID_INDEX:
      AD_THROW("GraphBLAS error: a row or column index is out of bounds");
    case GrB_DOMAIN_MISMATCH:
      AD_THROW("GraphBLAS error: object domains are not compatible");
    case GrB_DIMENSION_MISMATCH:
      AD_THROW("GraphBLAS error: matrix dimensions do not match");
    case GrB_OUTPUT_NOT_EMPTY:
      AD_THROW("GraphBLAS error: output matrix already has values in it");
    case GrB_NOT_IMPLEMENTED:
      AD_THROW("GraphBLAS error: not implemented in SuiteSparse:GraphBLAS");
    case GrB_PANIC:
      AD_THROW("GraphBLAS error: unrecoverable error");
    case GrB_OUT_OF_MEMORY:
      AD_THROW("GraphBLAS error: out of memory");
    case GrB_INSUFFICIENT_SPACE:
      AD_THROW("GraphBLAS error: output array not large enough");
    case GrB_INVALID_OBJECT:
      AD_THROW("GraphBLAS error: object is corrupted");
    case GrB_INDEX_OUT_OF_BOUNDS:
      AD_THROW("GraphBLAS error: a row or column is out of bounds");
    case GrB_EMPTY_OBJECT:
      AD_THROW("GraphBLAS error: an input scalar has no entry");
  }
  AD_FAIL();
}

bool GrbMatrix::isInitialized_ = false;

// _____________________________________________________________________________
void GrbMatrix::initialize() {
  if (!GrbMatrix::isInitialized_) {
    GrB_init(GrB_NONBLOCKING);
    GrbMatrix::isInitialized_ = true;
  }
}

// _____________________________________________________________________________
void GrbMatrix::finalize() {
  if (GrbMatrix::isInitialized_) {
    GrB_finalize();
    GrbMatrix::isInitialized_ = false;
  }
}
