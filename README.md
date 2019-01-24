# QLever

[![Build
Status](https://travis-ci.org/ad-freiburg/QLever.svg?branch=master)](https://travis-ci.org/ad-freiburg/QLever)

QLever (pronounced "clever") is an efficient SPARQL engine which can handle very large datasets.
For example, QLever can index the complete Wikidata (~ 7 billion triples) in less than 12 hours
on a standard Linux machine using around 40 GB of RAM, with subsequent query times below 1 second
even for relatively complex queries with large result sets.
On top of the standard SPARQL functionality, QLever also supports SPARQL+Text search and SPARQL autocompletion;
these are described in the next section.

A demo of QLever on a variety of large datasets, including the complete Wikidata, can be found under http://qlever.cs.uni-freiburg.de 

The basic design behind QLever is described in this [CIKM'17 paper](http://ad-publications.informatik.uni-freiburg.de/CIKM_qlever_BB_2017.pdf). If you use QLever, please cite this paper.
We are working on a follow-up publication that describes and evaluates the many additional features
and performance improvements to QLever since 2017.

# SPARQL+Text and SPARQL Autocompletion

On top of the vanilla SPARQL functionality,
QLever allows so-called SPARQL+Text queries on a text corpus linked to a knowledge base via entity recognition.
For example, the following query find all mentions of astronauts next to the words "moon" and "walk*" in the text corpus:

    SELECT ?a TEXT(?t) SCORE(?t) WHERE {
        ?a <is-a> <Astronaut> .
        ?t ql:contains-entity ?a .
        ?t ql:contains-word "walk* moon"
    } ORDER BY DESC(SCORE(?t))

Such queries can be simulated in standard SPARQL, but only with poor performance, see the CIKM'17 paper above.
Details about the required input data and the SPARQL+text query syntax and semantics can be
found [here](docs/sparql_plus_text.md).

QLever also supports efficient SPARQL autocompletion.
For example, the following query yields a list of all predicates associated with persons
in the knowledge base, ordered by the number of persons which have that predicate.

    SELECT ?predicate (COUNT(?predicate) as ?count) WHERE {
      ?x <is-a> <Person> .
      ?x ql:has-predicate ?predicate
    }
    GROUP BY ?predicate
    ORDER BY DESC(?count)

Note that this query could also be processed by standard SPARQL simply by replacing the second
triple by ?x ?predicate ?object. However, that query is bound to produce a very large intermediate
result (all triples of all persons) with a correspondingly huge query time.
In contrast, the query above takes only ~ 100ms on a standard Linux machine for a dataset with ~ 360 million triples and ~ 530 million text records.
More details on this feature set will be provided here soon.

# How to use

We recommend using QLever with `docker` as this alleviates the need for
installing dependencies. If you want to build QLever natively for your host or
are targeting a non Linux Unix-like system see [here](docs/native_setup.md).

The installation requires a 64-bit system (32 bit systems can't deal with `mmap`
on > 4 GB files or allocate enough RAM for larger KBs), docker version 18.05 or newer
(needs multi-stage builds without leaking files (for End-to-End Tests)) and `git`.
The you can simply do the following:

    git clone --recursive https://github.com/Buchhold/QLever.git qlever
    cd qlever
    docker build -t qlever .

This creates a docker image named "qlever" which contains all the libraries and
dependencies necessary to use QLever. If you want to be sure that everything is
working as it should before proceeding, you can run the [end-to-end
tests](#run-end-to-end-tests)

# Create an Index

## Provide QLever with access to the `/index` and `/input` volumes

When running **without user namespaces**, the container will use a user with UID
1000 which on desktop Linux is almost always the first real user.  If your UID
is not 1000 add `-u "$(id -u):$(id -g)"` to `docker run` to let QLever
execute as the current user.

When running **with user namespaces** you may need to make the index folder
accessible to the user the QLever process is mapped to on the host (e.g. nobody,
see `/etc/subuid`)

    chmod -R o+rw ./index

## Create an Index

First make sure that you have the your input data ready and accessible on your
machine. If you have no input data yet obtain it from one of our [recommended
sources](docs/obtaining_data.md) or create your own knowledge base in standard
*NTriple* or *Turtle* formats and (obtionally) add a [text
corpus](docs/sparql_plus_text.md).

Note that QLever only accepts UTF-8 encoded input files, then again [you should
be using UTF-8 anyway](http://utf8everywhere.org/)

Then proceed with creating an index.

**Important: Ensure that you have enough disk space where your `./index`
folder resides or see below for using a separate path**

To build a new index run a bash inside the QLever container as follows

    docker run -it --rm \
               -v "<absolute_path_to_input>:/input" \
               -v "$(pwd)/index:/index" --entrypoint "bash" qlever

If you want to use a *separate path* you **MUST** change the `"$(pwd)/index`
part in all `docker …` commands and replace it with the absolute path where your
index will reside.

From now on we are inside the container, make sure you follow all the coming instructions
for creating an index and **only then** proceed to the [next
section](#run-the-qlever-server).

If your input knowledge base is in the standard *NTriple* or *Turtle* format
create the index with the following command

    IndexBuilderMain -a -i /index/<prefix> -n /input/knowledge_base.ttl

Where `<perfix>` is the base name for all index files and `-a` enables certain
queries using predicate variables. If you use `index` as the prefix you can
later skip the `-e INDEX_PREFIX=<prefix>` flag.

If instead you are using a *non standard* TSV knowledge base file use the `-t`
flag instead of `-n`. This is *not recommended* but supported for legacy
reasons.

To include a text collection, the wordsfile and docsfiles (see
[here](docs/sparql_plus_text.md) for the required format) is provided with the
`-w` and `-d` flags respectively.

Then the full command will look like this:

    IndexBuilderMain -a -i /index/<prefix> -n /input/knowledge_base.ttl \
      -w /input/wordsfile.tsv -d /input/docsfile.tsv

If you want some literals to be written to an on disk vocabulary (by default
this concerns literals longer than 50 chars and literals in less frequent
lagnuages), add the optional `-l` flag.
This is useful for large knowledge bases that includ texts (descriptions etc) as
literals and thus consume lots of memory on startup without this option.

You can also add a text index to an existing knowledge base index by ommitting
`-n` (or `-t`) parameter.

# Run the QLever Server

To run a QLever server container use the following command.

    docker run -it -p 7001:7001 \
      -v "$(pwd)/index:/index" \
      -e INDEX_PREFIX=<prefix> \
      --name qlever \
      qlever <ServerMain args>

where `<ServerMain args>` are arguments (except for port and index prefix)
which are always included. If none are supplied `-t -a` is used. If you want
the container to run in the background and restart automatically replace `-it`
with `-d --restart=unless-stopped`

## Executing queries:
Queries can be executed from the command line using `curl`

    curl 'http://localhost:7001/?query=SELECT ?x WHERE {?x <rel> ?y}'

or with a minimal web UI accessible with the browser under:

    http://localhost:7001/index.html

## Convenience features

### Statistics:


You can get stats for the currently active index in the following way:

    <server>:<port>/?cmd=stats

This query will yield a JSON response that features:

* The name of the KB index
* The number of triples in the KB index
* The number of index permutations build (usually 2 or 6)
* The numbers of distinct subjects, predicates and objects (only available if 6 permutations are built)
* The name of the text index (if one is present)
* The number of text records in the text index (if text index present)
* The number of word occurrences/postings in the text index (if text index present)
* The number of entity occurrences/postings in the text index (if text index present)


The names of a index is the name of the input nt file (and wordsfile for the text index) but can also be specified manually while building an index.
Therefore, IndexbuilderMain takes two optional arguments: --text-index-name (-T) and --kb-index-name (-K).


### Send vs Compute

Currently, QLever does not compute partial results if there is a LIMIT modifier.
However, strings (for entities and text excerpts) are only resolved for those items that are actually send.
Furthermore, in a UI, it is usually beneficial to get less than all result rows by default.

While it is recommended for applications to specify a LIMIT, some experiments want to measure the time to produce the full result but not block the UI.
Therefore an additional HTTP parameter "&send=<x>" can be used to only send x result rows but to compute the fully readable result for everything according to LIMIT.

**IMPORTANT: Unless you want to measure QLever's performance, using LIMIT (+ OFFSET for sequential loading) should be preferred in all applications. That way should be faster and standard SPARQL without downsides.**

## Troubleshooting

If you have problems, try to rebuild when compiling with `-DCMAKE_BUILD_TYPE=Debug`.
In particular also rebuild the index.
The release build assumes machine written words- and docsfiles and omits sanity checks for the sake of speed.

### Run End-to-End Tests

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

### Converting old Indices For Current QLever Versions

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


### High RAM Usage During Runtime

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

