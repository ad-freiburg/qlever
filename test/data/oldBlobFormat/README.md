# Blobs in the legacy format of the `qlever-bmw` fork

This directory contains four blobs (a vocabulary together with seven named
cached queries each) that were written by `Qlever::serializeToUncompressedBlob`
of the `demo-v1-c++17_unimodel` branch of the `qlever-bmw` fork
(`https://github.com/qlever-dev/qlever-bmw`). Their format is documented in
`src/blobConverter/LegacyBlobReader.h`, and they are used by
`test/blobConverter/BlobConverterTest.cpp` to test the conversion to the current
blob format of `Qlever::serializeVocabAndNamedCacheToCompressedBlob` (see
`src/blobConverter/BlobConverter.h` and the `qlever-blob-converter` binary).

The blobs have to be checked in, because the current code can no longer write
the legacy format. They were created by the `qlever-bmw` fork from four small
map tiles of the BMW unified map model; the numbers in the file names are the
tile ids. Each blob contains the named cached queries `boundaries`,
`dp-payload`, `geos` (with a cached geo index), `lane-payload`,
`road-ref-to-lane`, `roadrefs`, and `speed`, and `Id`s of the datatypes
`Undefined`, `Bool`, `Int`, `Double`, `VocabIndex`, `BlankNodeIndex`, and
`EncodedVal`. The metadata of the blobs configures all nine legacy encoding
schemes of the `qlever-bmw` fork (see
`src/blobConverter/LegacyEncodedIriManager.h`), but only four of them occur as
encoded IRIs in the cached results: `speedprofile_` (a hardcoded scheme) and
`lane_`, `roadPart_`, `dp_` (bit range constraint schemes). The other five
(`range_`, `valRange_`, `laneRef_`, `roadRef_`, `stopLoc_`) are covered by the
unit tests in `test/blobConverter/LegacyEncodedIriManagerTest.cpp` only.
