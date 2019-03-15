# QLever

[![Build
Status](https://travis-ci.org/ad-freiburg/QLever.svg?branch=master)](https://travis-ci.org/ad-freiburg/QLever)

QLever (pronounced "clever") is an efficient SPARQL engine supporting
large datasets including the full Wikidata (7 billion triples). Even on very
large datasets QLever uses only about 40 GB RAM, builds indices in less than 12
hours and executes most queries in less than a second.

On top of the standard SPARQL functionality, QLever also supports SPARQL+Text
search and SPARQL autocompletion; these are described on the [advanced
features](docs/advanced_features.md) page.

A demo of QLever on a variety of large datasets, including Wikidata, can be
found [here](http://qlever.cs.uni-freiburg.de).

The basic design behind QLever was described in this [CIKM'17
paper](http://ad-publications.informatik.uni-freiburg.de/CIKM_qlever_BB_2017.pdf).
If you use QLever in your work, please cite that paper.

# Quickstart

If you want to skip the details and just get a running QLever instance to play
around with. Follow the [quickstart guide](docs/quickstart.md).

Alternatively to get started with a real (and really big) dataset we have prepared
a [Wikidata Quickstart Guide](docs/wikidata.md). This guide takes you through the entire
process of loading the full Wikidata Knowledge Base into QLever, but don't worry
it is pretty simple.

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

We recommend using QLever with [docker](https://www.docker.com) if you
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
*NTriple* or *Turtle* formats and (obtionally) add a [text
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

    IndexBuilderMain -a -l -i /index/<prefix> -n /input/knowledge_base.ttl

Where `<prefix>` is the base name for all index files, `-a` enables certain
queries using predicate variables and `-l` externalizes long literals to disk.
If you use `index` as the prefix you can later skip the `-e
INDEX_PREFIX=<prefix>` flag.

To include a text collection, the wordsfile and docsfiles (see
[here](docs/sparql_plus_text.md) for the required format) is provided with the
`-w` and `-d` flags respectively.

Then the full command will look like this:

    IndexBuilderMain -l -a -i /index/<prefix> -n /input/knowledge_base.ttl \
      -w /input/wordsfile.tsv -d /input/docsfile.tsv

You can also add a text index to an existing knowledge base index by adding the
`-A` flag and ommitting the `-n` flag.

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



