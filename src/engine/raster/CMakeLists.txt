add_library(raster RasterFile.cpp)
qlever_target_link_libraries(raster rdfTypes)
# `PRIVATE`, so that the GDAL headers are only visible inside this library.
target_link_libraries(raster PRIVATE GDAL::GDAL)
# The folder with the raster files, fixed to `rasters/` in the source tree.
target_compile_definitions(raster PRIVATE
        QLEVER_RASTER_DIR="${PROJECT_SOURCE_DIR}/rasters")
