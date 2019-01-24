# QLever

[![Build
Status](https://travis-ci.org/ad-freiburg/QLever.svg?branch=master)](https://travis-ci.org/ad-freiburg/QLever)

QLever (pronounced "clever") is a high performance SPARQL engine for efficient
search on large scale knowledge bases (KB) such as Wikidata.
On top of its best in class performance and scalability to large datasets (> 1.5
TB) QLever has some unique features and SPARQL extensions.

## QLever's Superpowers

QLever extends SPARQL with several useful features such as the ability to
combine classic SPARQL queries with search on a knowledge base linked text
corpus. Such a linked corpus is basically a large (several TBs) collection of
text in which mentions of entities are linked to the
respective entities in the knowledge base.

In QLever this enables efficient queries such as the following query asking
which astronauts walked on the moon.

    SELECT ?a TEXT(?t) SCORE(?t) WHERE {
        ?a <is-a> <Astronaut> .
        ?t ql:contains-entity ?a .
        ?t ql:contains-word "walk* moon"
    } ORDER BY DESC(SCORE(?t))

Technically it finds entities which are mentioned with the word prefix "walk"
and the word "moon" which are known to be astronauts.

The format required of such a linked corpus as well as a more details aboute the
queries this enables can be found [here](docs/sparql_plus_text.md).

Another extension to SPARQL is the ability to efficiently look up the predicates
that a, possibly very large, group of entities has, including how often they
appear.

For example the following query gives us a list of predicates that are typical
of astronauts ordered by how often they appear.

    SELECT ?predicate (COUNT(?predicate) as ?count) WHERE {
      ?x <is-a> <Astronaut> .
      ?x ql:has-predicate ?predicate
    }
    GROUP BY ?predicate
    ORDER BY DESC(?count)

## Research paper

The first research paper ["QLever: A Query Engine for Efficient SPARQL+Text
Search"](http://ad-publications.informatik.uni-freiburg.de/CIKM_qlever_BB_2017.pdf)
about this work was presented at [CIKM 2017](http://cikm2017.org/)!

The paper describes the research behind QLever, how it works, and most
importantly contains an evaluation where we compare QLever to state-of-the-art
SPARQL engines. Query times are competitive and often faster on the pure SPARQL
queries, and several orders of magnitude faster on the SPARQL+Text queries.

To reproduce our experimental evaluation, best use the according release (git
tag). Since then, we have made several changes, including some to query syntax
that has not be carried over to the input queries for our experiments.

# How to use

We recommend using QLever with `docker` as this alleviates the need for
installing dependencies. If you want to build QLever natively for your host or
are targeting a non Linux Unix-like system see [here](docs/native_setup.md).

## Get the code

This requires `git` to be installed

    git clone --recursive https://github.com/Buchhold/QLever.git

## Requirements:

A 64 bit host system (32 bit systems can't deal with `mmap` on > 4 GB files or
allocate enough RAM for larger KBs)

* docker 18.05 or newer (needs multi-stage builds without leaking files (for
  End-to-End Tests))

## Build the image

Inside the the repositories root folder run

    docker build -t qlever .

This creates a docker image named "qlever" which contains all the libraries and
dependencies necessary to use QLever. If you want to be sure that everything is
working as it should before proceeding, you can run the [end-to-end
tests](#run-end-to-end-tests)

## Create an Index
### Provide QLever with access to the `/index` and `/input` volumes

When running **without user namespaces**, the container will use a user with UID
1000 which on desktop Linux is almost always the first real user.  If your UID
is not 1000 add `-u "$(id -u):$(id -g)"` to `docker run` to let QLever
execute as the current user.

When running **with user namespaces** you may need to make the index folder
accessible to the user the QLever process is mapped to on the host (e.g. nobody,
see `/etc/subuid`)

    chmod -R o+rw ./index

### Create an Index

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

#### Using an NTriples or Turtle file:

Note that the string passed to `-i` is the base name of the index files QLever
creates.

    IndexBuilderMain -i /index/<prefix> -n /input/knowledge_base.ttl

Where `<perfix>` is the base name for all index files. If you use `index` tou
can later skip the `-e INDEX_PREFIX=<prefix>` flag.

If instead you are using a *non standard* TSV knowledge base file use the `-t`
flag instead of `-n`. This is *not recommended* but supported for legacy
reasons.

To include a text collection, the wordsfile and docsfiles (see
[here](docs/sparql_plus_text.md) for the required format) is provided with the
`-w` and `-d` flags respectively.

The full call will look like this:

    IndexBuilderMain -i /index/<prefix> -n /input/knowledge_base.ttl -w /input/wordsfile.tsv -d /input/docsfile.tsv

If you want some literals to be written to an on disk vocabulary (by default
this concerns literals longer than 50 chars and literals in less frequent
lagnuages), add an optional parameter `-l`. This is useful for large knowledge
bases that includ texts (descriptions etc) as literals and thus consume lots
of memory on startup without this option.

    IndexBuilderMain -i /index/<prefix> -n /input/knowledge_base.ttl -l

You can also add a text index to an existing knowledge base index by ommitting
`-n` and `-t` parameters (for KB input)

    IndexBuilderMain -i /index/<prefix> -w /input/wordsfile.tsv -d /input/docsfile.tsv

All options can, of course, be combined. The full call with all permutations and
literals on disk will look like this:

    IndexBuilderMain -a -l -i /input/<prefix> -n /input/knowledge_base.ttl -w /input/wordsfile.tsv -d /input/docsfile.tsv

## Run the QLever Server

To run a QLever server container use the following command.

    docker run -it -p 7001:7001 -v "$(pwd)/index:/index" -e INDEX_PREFIX=<prefix> --name qlever qlever <ServerMain args>

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
will work but throw a warning at runtime. We have provided a converter which
allows to only modify the meta data without having to rebuild the index. Just
run `./MetaDataConverterMain <index-prefix>` . This will not automatically
overwrite the old index but copy the permutations and create new files with the
suffix `.converted` (e.g. `<index-prefix>.index.ops.converted` These suffixes
have to be removed manually in order to use the converted index (rename to
`<index-prefix>.index.ops` in our example). Please consider creating backups of
the "original" index files before overwriting them like this.  them. Please note
that for 6 permutations the converter also builds new files
`<index-prefix>.index.xxx.meta-mmap` where parts of the meta data of OPS and OSP
permutations will be stored.


### High RAM Usage During Runtime

QLever uses an on-disk index and is usually able to operate with pretty low RAM
usage. However, there are data layouts that currently lead to an excessive
amount of memory being used. Firstly, note that even very large text corpora have little impact on
memory usage. Larger KBs are much more problematic.
There are two things that can contribute to high RAM usage (and large startup
times) during runtime:

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

### Workarounds

* Removing unnecessary objects (e.g. literals in unused languages) helps a lot,
but is no very "clean".

* Reduce ID size and switch from 64 to 32bit IDs. However this would only yield save a
low portion of memory since it doesn't effect pointers byte offsets into index
files.
