# Troubleshooting

If you have problems, try to rebuild when compiling with `-DCMAKE_BUILD_TYPE=Debug`.
In particular also rebuild the index.
The release build assumes machine written words- and docsfiles and omits sanity checks for the sake of speed.

## Run End-to-End Tests

QLever includes a simple mechanism for testing the entire system from
from building an index to running queries in a single command.

In fact this End-to-End Test is run on Travis CI for every commit and Pull
Request. It is also useful for local development however since it allows you to
quickly test if something is horribly broken.

Just like QLever itself the End-to-End Tests can be executed from a previously
build QLever container

**Note**: If you encounter permission issues e.g. if your UID is not 1000 or you
are using docker with user namespaces, add the flag `-u root` to your `docker
run` command or do a `chmod -R o+rw e2e_data`

    docker build -t qlever .
    docker run -it --rm -v "$(pwd)/e2e_data:/app/e2e_data/" --name qlever-e2e --entrypoint e2e/e2e.sh qlever

## Converting old Indices For Current QLever Versions

We have recently updated the way the index meta data (offsets of relations
within the permutations) is stored. Old index builds with 6 permutations will
not work directly with the recent QLever version while 2 permutation indices
will work but throw a warning at runtime.

We have provided a converter which
allows to only modify the meta data without having to rebuild the index. Just
run `MetaDataConverterMain <index-prefix>` in the same way as the
`IndexBuilderMain`.

This will not automatically overwrite the old index but copy the permutations
and create new files with the suffix `.converted` (e.g.
`<index-prefix>.index.ops.converted` These suffixes have to be removed manually
in order to use the converted index (rename to `<index-prefix>.index.ops` in our
example).
**Please consider creating backups of
the "original" index files before overwriting them like this**.

Please note that for 6 permutations the converter also builds new files
`<index-prefix>.index.xxx.meta-mmap` where parts of the meta data of OPS and OSP
permutations will be stored.


## High RAM Usage During Runtime

QLever uses an on-disk index and is usually able to operate with pretty low RAM
usage. For example it can handle the full Wikidata KB + Wikipedia which is about
1.5 TB of index with less than 46 GB of RAM

However, there are data layouts that currently lead to an excessive
amount of memory being used.

* The size of the KB vocabulary. Using the -l flag while building the index
causes long and rarely used strings to be externalized to
disk. This saves a significant amount of memory at little to no time cost for
typical queries. The strategy can be modified to be more aggressive (currently
by editing directly in the code during index construction)

* Building all 6 permutations over large KBs (or generelly having a
permutation, where the primary ordering element takes many different values).
To handle this issue, the meta data of OPS and OSP are not loaded into RAM but
read from disk. This saves a lot of RAM with only little impact on the speed of
the query execution. We will evaluate if it's  worth to also externalize SPO and
SOP permutations in this way to further reduce the RAM usage or to let the user
decide which permutations shall be stored in which format.

