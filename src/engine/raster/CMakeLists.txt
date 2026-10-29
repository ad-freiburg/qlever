add_library(raster RasterFile.cpp)
qlever_target_link_libraries(raster rdfTypes)
# `PRIVATE`, so that the GDAL headers are only visible inside this library.
target_link_libraries(raster PRIVATE GDAL::GDAL)
