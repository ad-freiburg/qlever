// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.roland.herrmann@mars.uni-freiburg.de)

#include "GrbMatrix.h"

#include <GraphBLAS.h>

#include <memory>
#include <vector>

#include "util/Exception.h"

// _____________________________________________________________________________
GrbMatrix GrbMatrix::copy() const {
  GrB_Matrix matrixCopy;
  auto info = GrB_Matrix_new(&matrixCopy, GrB_BOOL, nrows(), ncols());
  handleError(info);
  info = GrB_Matrix_dup(&matrixCopy, *matrix_);
  handleError(info);

  auto returnMatrix = GrbMatrix();
  returnMatrix.matrix_ = std::make_unique<GrB_Matrix>(matrixCopy);

  return returnMatrix;
}

// _____________________________________________________________________________
void GrbMatrix::setElement(size_t row, size_t col, bool value) {
  auto info = GrB_Matrix_setElement_BOOL(*matrix_, value, row, col);
  handleError(info);
}

// _____________________________________________________________________________
bool GrbMatrix::getElement(size_t row, size_t col) const {
  bool result;
  auto info = GrB_Matrix_extractElement_BOOL(&result, *matrix_, row, col);
  handleError(info);
  if (info == GrB_NO_VALUE) {
    return false;
  }
  return result;
}

// _____________________________________________________________________________
GrbMatrix GrbMatrix::build(const std::vector<size_t> rowIndices,
                           const std::vector<size_t> colIndices, size_t numRows,
                           size_t numCols) {
  auto matrix = GrbMatrix(numRows, numCols);
  GrB_Index nvals = rowIndices.size();
  if (nvals == 0) {
    return matrix;
  }

  bool values[nvals];
  for (size_t i = 0; i < nvals; i++) {
    values[i] = true;
  }
  auto info =
      GrB_Matrix_build_BOOL(matrix.getMatrix(), &rowIndices[0], &colIndices[0],
                            values, nvals, GxB_IGNORE_DUP);
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
std::vector<std::pair<size_t, size_t>> GrbMatrix::extractTuples() const {
  size_t n = nvals();
  size_t rowIndices[n];
  size_t colIndices[n];
  bool values[n];
  auto info = GrB_Matrix_extractTuples_BOOL(rowIndices, colIndices, values, &n,
                                            *matrix_);
  GrbMatrix::handleError(info);

  std::vector<std::pair<size_t, size_t>> result;
  for (size_t i = 0; i < n; i++) {
    if (values[i]) {
      result.push_back(std::make_pair(rowIndices[i], colIndices[i]));
    }
  }
  return result;
}

// _____________________________________________________________________________
std::vector<size_t> GrbMatrix::extractColumn(size_t colIndex) const {
  std::unique_ptr<GrB_Vector> columnVector = std::make_unique<GrB_Vector>();
  size_t numRows = nrows();
  auto info = GrB_Vector_new(columnVector.get(), GrB_BOOL, numRows);
  handleError(info);

  info = GrB_Col_extract(*columnVector, GrB_NULL, GrB_NULL, *matrix_, GrB_ALL,
                         numRows, colIndex, GrB_NULL);
  handleError(info);

  size_t indices[numRows];
  bool values[numRows];
  std::unique_ptr<size_t> nvals = std::make_unique<size_t>(numRows);
  info = GrB_Vector_extractTuples_BOOL(indices, values, nvals.get(),
                                       *columnVector);
  handleError(info);

  info = GrB_Vector_free(columnVector.get());
  handleError(info);

  std::vector<size_t> vec;
  vec.insert(vec.begin(), indices, indices + *nvals);
  return vec;
}

// _____________________________________________________________________________
std::vector<size_t> GrbMatrix::extractRow(size_t rowIndex) const {
  GrbMatrix transposed = transpose();
  return transposed.extractColumn(rowIndex);
}

// _____________________________________________________________________________
size_t GrbMatrix::nvals() const {
  size_t nvals;
  auto info = GrB_Matrix_nvals(&nvals, *matrix_);
  GrbMatrix::handleError(info);
  return nvals;
}

// _____________________________________________________________________________
size_t GrbMatrix::nrows() const {
  size_t nrows;
  auto info = GrB_Matrix_nrows(&nrows, *matrix_);
  GrbMatrix::handleError(info);
  return nrows;
}

// _____________________________________________________________________________
size_t GrbMatrix::ncols() const {
  size_t ncols;
  auto info = GrB_Matrix_ncols(&ncols, *matrix_);
  GrbMatrix::handleError(info);
  return ncols;
}

// _____________________________________________________________________________
GrbMatrix GrbMatrix::transpose() const {
  GrB_Matrix transposed;
  auto info = GrB_Matrix_new(&transposed, GrB_BOOL, ncols(), nrows());
  handleError(info);
  info = GrB_transpose(transposed, GrB_NULL, GrB_NULL, *matrix_, GrB_NULL);
  handleError(info);

  GrbMatrix result = GrbMatrix();
  result.matrix_ = std::make_unique<GrB_Matrix>(transposed);

  return result;
}

// _____________________________________________________________________________
void GrbMatrix::accumulateMultiply(const GrbMatrix& otherMatrix) const {
  auto info = GrB_mxm(*matrix_, GrB_NULL, GrB_LOR, GrB_LOR_LAND_SEMIRING_BOOL,
                      *matrix_, otherMatrix.getMatrix(), GrB_NULL);
  handleError(info);
}

// _____________________________________________________________________________
GrbMatrix GrbMatrix::multiply(const GrbMatrix& otherMatrix) const {
  size_t resultNumRows = nrows();
  size_t resultNumCols = otherMatrix.ncols();
  GrB_Matrix resultMatrix;
  auto info =
      GrB_Matrix_new(&resultMatrix, GrB_BOOL, resultNumRows, resultNumCols);

  info = GrB_mxm(resultMatrix, GrB_NULL, GrB_NULL, GrB_LOR_LAND_SEMIRING_BOOL,
                 *matrix_, otherMatrix.getMatrix(), GrB_NULL);
  handleError(info);

  GrbMatrix result = GrbMatrix(resultNumRows, resultNumCols);
  result.matrix_ = std::make_unique<GrB_Matrix>(resultMatrix);
  return result;
}

// _____________________________________________________________________________
void GrbMatrix::handleError(GrB_Info info) {
  switch (info) {
    case GrB_SUCCESS:
      return;
    case GrB_NO_VALUE:
      return;
    // case GxB_EXHAUSTED:
    //   return;
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
