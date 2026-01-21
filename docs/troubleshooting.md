# Troubleshooting

If you have problems, try rebuilding QLever in debug mode (e.g. by changing the
`cmake` line in the `Dockerfile` to `cmake -DCMAKE_BUILD_TYPE=Debug
-DLOGLEVEL=DEBUG` or [building natively](docs/native_setup.md)),
Then rebuild your index with the newly build docker container and
`qlever-index` executable.
The release build assumes machine written words- and docsfiles and omits sanity
checks for the sake of speed.

## Run End-to-End Tests

QLever includes a simple mechanism for testing the entire system,
from building an index to running queries.

In fact the End-to-End Test is run on Travis CI for every commit and Pull
Request. It is also useful for local development, as it allows you to
quickly test if something is horribly broken.

Just like QLever itself the End-to-End Tests can be executed from a previously
built QLever docker container.

**Note**: If you encounter permission issues e.g. if your UID is not 1000 or you
are using docker with user namespaces, add the flag `-u root` to your `docker
run` command or do a `chmod -R o+rw e2e_data`

    docker build -t qlever .
    docker run -it --rm -v "$(pwd)/e2e_data:/app/e2e_data/" --name qlever-e2e --entrypoint e2e/e2e.sh qlever

## Converting old Indices For current QLever Versions

When we make changes to the way the index meta data (e.g. offsets of relations
within the permutations) is stored, old indices may become incompatible with new
executables.
For example, some old index builds with 6 permutations will not work directly
with newer QLever versions, while 2 permutation indices do work.
In either case incompatible versions are detected during startup.

For these cases, we provide a converter which only modifies the
meta data without having to rebuild the index. Run `MetaDataConverterMain
<index-prefix>` in the same way as as running `qlever-index`.

This will not automatically overwrite the old index but copy the permutations
and create new files with the suffix `.converted` (e.g.
`<index-prefix>.index.ops.converted` These suffixes have to be removed manually
in order to use the converted index (rename to `<index-prefix>.index.ops` in our
example).

**Please consider creating backups of the "original" index files before
overwriting them**.

Please note that for 6 permutations the converter also builds new files
`<index-prefix>.index.xxx.meta-mmap` where parts of the meta data of OPS and OSP
permutations will be stored.


## High RAM Usage During Runtime

QLever uses an on-disk index and usually requires only a moderate amount of RAM.
For example, it can handle the full Wikidata KB + Wikipedia, which is about 1.5 TB
of index with less than 46 GB of RAM

However, there are data layouts that currently lead to an excessive
amount of memory being used:

* **The size of the KB vocabulary**. Using the -l flag while building the index
causes long and rarely used strings to be externalized to
disk. This saves a significant amount of memory at little to no time cost for
typical queries. The strategy can be modified to be more aggressive.

* **Building all 6 permutations over large KBs** (or generally having a
permutation, where the primary ordering element takes many different values).
To handle this issue, the meta data of OPS and OSP are not loaded into RAM but
read from disk. This saves a lot of RAM with only little impact on the speed of
the query execution. We will evaluate if it's  worth also externalizing SPO and
SOP permutations in this way to further reduce the RAM usage, or to let the user
decide which permutations shall be stored in which format.

## Note when using Docker on Mac or Windows

When building an index, QLever will create scratch space for on-disc sorting.
This space is allocated as a large 1 TB sparse file. On standard Linux file
systems such as Ext4 sparse files are optimized and this will only take as much
disk space as is actually used.

With some systems such as Docker on Mac or when using unsupported file
systems such as NTFS or APFS, this may lead to problems as these do not
properly support sparse files.
