# QLever


[![Build Status via Docker](https://github.com/ad-freiburg/QLever/actions/workflows/docker.yml/badge.svg)](https://github.com/ad-freiburg/QLever/actions/workflows/docker.yml)
[![Build Status via G++10/Clang++11](https://github.com/ad-freiburg/QLever/actions/workflows/cmake.yml/badge.svg)](https://github.com/ad-freiburg/QLever/actions/workflows/cmake.yml)



QLever (pronounced "Clever") is a SPARQL engine that can efficiently index and query very large knowledge graphs with tens of billions of triples on a single standard PC or server.
In particular, QLever is fast for queries that involve large intermediate or final results, which are notoriously hard for engines like Blazegraph or Virtuoso.
QLever also supports search in text associated with the knowledge base, as well as SPARQL autocompletion.
[Here are demos of QLever](http://qlever.cs.uni-freiburg.de) on a variety of large knowledge graphs, including the complete Wikidata and OpenStreetMap.
Those demos also feature QLever's context-sensitiv autocompletion, which makes SPARQL query construction so much easier.

QLever was frist described and evaluated in this [CIKM'17
paper](http://ad-publications.informatik.uni-freiburg.de/CIKM_qlever_BB_2017.pdf).
Qlever's autocompletion functionality is described and evaluated in [this paper](https://ad-publications.cs.uni-freiburg.de/ARXIV_sparql_autocompletion_BKKKS_2021.pdf).
If you use QLever in your work, please cite those papers.
QLever supports standard SPARQL 1.1 constructs like:
LIMIT, OFFSET, ORDER BY, GROUP BY, HAVING, COUNT, DISTINCT, SAMPLE, GROUP_CONCAT, FILTER, REGEX, LANG, OPTIONAL, UNION, MINUS, VALUES, BIND.
Predicate paths and subqueries are also supported.
The SERVICE keyword is not yet supported.
We aim at full SPARQL 1.1 support and we are almost there (except for SPARQL Update operations, which are a longer-term project).

# Quickstart

For easy step-by-step instructions on how to get data, build an index using QLever, and
then start a SPARQL endpoint using that index, see our [Quickstart Guide](docs/quickstart.md).
This will take you through a simple example dataset ("olympics", with 1.8M triples)
as well as a very large dataset (the complete Wikidata, with 12 billion triples).

Here is an older [Wikidata Quickstart Guide](docs/wikidata.md). But actually, it is
just as easy to use QLever for the large Wikidata as for a small example. Building the
index just takes longer for Wikidata (about 20 hours).

QLever's [advanced features are described here](docs/advanced_features.md).

# Overview

The rest of this page is organized in the following sections. Taking you
through the steps necessary to get a QLever instance up and runnining starting
from a simple Turtle dump of a Knowledge Base.

* [Building the QLever Docker Container](#building-the-qlever-docker-container)
* [Creating an Index](#creating-an-index)
  * [Obtaining Data](#obtaining-data)
  * [Permissions](#permissions)
  * [Building the Index](#building-the-index)
* [Run QLever](#running-qlever)
  * [Executing Queries](#executing-queries)

Further documentation is available on the following topics

* [Supported Knowledge Bases and Example Queries](docs/knowledge_bases.md)
* [SPARQL + Text](docs/sparql_plus_text.md)
* [Advanced Features](docs/advanced_features.md)
* [Native Setup](docs/native_setup.md)
* [Troubleshooting](docs/troubleshooting.md)
* [Quickstart Guide](docs/quickstart.md)
* [Wikidata Quickstart Guide](docs/wikidata.md)

# Building the QLever Docker Container

We recommend using QLever with [docker](https://www.docker.com). If you
absolutely want to run QLever directly on your host see
[here](docs/native_setup.md).

The installation requires a 64-bit system, docker version 18.05 or newer and
`git`.

    git clone --recursive https://github.com/ad-freiburg/QLever.git qlever
    cd qlever
    docker build -t qlever .

This creates a docker image named "qlever" which contains everything needed
to use QLever. If you want to be sure that everything is working as it should
before proceeding, you can run the [end-to-end
tests](docs/troubleshooting.md#run-end-to-end-tests)

# Creating an Index

## Obtaining Data

First make sure that you have your input data ready and accessible on your
machine. If you have no input data yet obtain it from one of our [recommended
sources](docs/knowledge_bases.md) or create your own knowledge base in standard
*NTriple* or *Turtle* formats and (optionally) add a [text
corpus](docs/sparql_plus_text.md).

Note that QLever only accepts UTF-8 encoded input files. Then again [you should
be using UTF-8 anyway](http://utf8everywhere.org/)

## Permissions

By default and when running `docker` **without user namespaces**, the container
will use the user ID 1000 which on Linux is almost always the first real user.
If the default user does not work add `-u "$(id -u):$(id -g)"` to `docker run`
so that QLever executes as the current user.

When running `docker` **with user namespaces** you may need to make the index
folder accessible to the user the QLever process is mapped to on the host (e.g.
nobody, see `/etc/subuid`)

    chmod -R o+rw ./index

## Building the Index

Then proceed with creating an index.

**Important: Ensure that you have enough disk space where your `./index`
folder resides or see below for using a separate path**

To build a new index run a bash inside the QLever container as follows

    docker run -it --rm \
               -v "<absolute_path_to_input>:/input" \
               -v "$(pwd)/index:/index" --entrypoint "bash" qlever

If you want to use a *separate path* you **MUST** change the `"$(pwd)/index`
part in all `docker …` commands and replace it with the **absolute** path to
your index.

From now on we are inside the container, make sure you follow all the coming instructions
for creating an index and **only then** proceed to the [next
section](#running-qlever).

If your input knowledge base is in the standard *NTriple* or *Turtle* format
create the index with the following command

    IndexBuilderMain -l -i /index/<prefix> -f /input/knowledge_base.ttl

Where `<prefix>` is the base name for all index files and `-l` externalizes long literals to disk.
If you use `index` as the prefix you can later skip the `-e
INDEX_PREFIX=<prefix>` flag.

To include a text collection, the wordsfile and docsfiles (see
[here](docs/sparql_plus_text.md) for the required format) is provided with the
`-w` and `-d` flags respectively.

Then the full command will look like this:

    IndexBuilderMain -l -i /index/<prefix> -f /input/knowledge_base.ttl \
      -w /input/wordsfile.tsv -d /input/docsfile.tsv

You can also add a text index to an existing knowledge base index by adding the
`-A` flag and ommitting the `-f` flag.

# Running QLever

To run a QLever server container use the following command.

    docker run -it -p 7001:7001 \
      -v "$(pwd)/index:/index" \
      -e INDEX_PREFIX=<prefix> \
      --name qlever \
      qlever

Where additional arguments can be added at the end of the command. If you want
the container to run in the background and restart automatically replace `-it`
with `-d --restart=unless-stopped`

## Executing queries

The quickest way to run queries is to use the minimal web interface, available
at the port specified above (7001 in the example). For a more advanced web
interface you can use the [QLever
UI](http://ad-publications.informatik.uni-freiburg.de/student-projects/qlever-ui/).

Queries can also be executed from the command line using `curl`

    curl 'http://localhost:7001/?query=SELECT ?x WHERE {?x <rel> ?y}'



