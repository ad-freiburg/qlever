# QLever

[![Build
Status](https://travis-ci.org/ad-freiburg/QLever.svg?branch=master)](https://travis-ci.org/ad-freiburg/QLever)

QLever (pronounced "clever") is a query engine for efficient search on
a combined knowledge base (KB) and text corpus. Where in the text entities from the
knowledge base have been identified.

The query language is SPARQL extended by three QLever-specific predicates
`ql:contains-entity`, `ql:contains-word` and `ql:has-predicate`.
`ql:contains-entity` and `ql:contains-word` can express the occurrence of an
entity or word (the object of the predicate) in a text record (the subject of
the predicate). `ql:has-predicate` can be used to efficiently count available
predicates for a set of entities.  Pure SPARQL is supported as well.

With this, it is possible to answer queries like the following one for
astronauts who walked on the moon:

    SELECT ?a TEXT(?t) SCORE(?t) WHERE {
        ?a <is-a> <Astronaut> .
        ?t ql:contains-entity ?a .
        ?t ql:contains-word "walk* moon"
    } ORDER BY DESC(SCORE(?t))

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
are targeting a non Linux Unix-like system see [here](native_setup.md).

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

## Create or reuse an index
### Allow QLever to access the `/index` and `/input` volumes
When running **without user namespaces**, the container will use a user with UID
1000 which on desktop Linux is almost always the first real user.  If your UID
is not 1000 add `-u "$(id -u):$(id -g)"` to `docker run` to let QLever
execute as the current user.

When running **with user namespaces** you may need to make the index folder
accessible to the user the QLever process is mapped to on the host (e.g. nobody,
see `/etc/subuid`)

    chmod -R o+rw ./index

### Use an existing index or create a new one
To build a new index run a bash inside the container as follows

    docker run -it --rm \
               -v "<absolute_path_to_input>:/input" \
               -v "$(pwd)/index:/index" --entrypoint "bash" qlever-<name>


Then inside the container follow the instructions for [creating an
index](#creating-an-index). **Only then** proceed to the [next
section](#run-the-qlever-server)

For an existing index, copy it into the `./index` folder and make sure to either
name it so that all files start with `index`  or set `-e INDEX_PREFIX=<prefix>`
during docker run where `<prefix>` is the part of the index file names before
the first `.`.

## Creating an Index:

**Important: Make sure that you have enough disk space where your `./index`
folder lives or see below for using a separate path**

If you want to use a *separate path* you **MUST** change the `"$(pwd)/index`
oart in all `docker …` commands.

QLever only accepts UTF-8 encoded input files, then again [you should be using
UTF-8 anyway](http://utf8everywhere.org/)

If you don't have a knowledge base in *Turtle*, *NTriple* or *TSV* format
already, you can [use our playground
datasets](#how-to-obtain-data-to-play-around-with)

### Using an NTriples or Turtle file:

Note that the string passed to `-i` is the base name of the index files QLever
creates.

    IndexBuilderMain -i /index/<prefix> -n /input/knowledge_base.ttl

### Using a TSV File (no spaces / tabs in spo):

    IndexBuilderMain -i /index/<prefix> -t /input/knowledge_base.tsv

Where `<perfix>` is the base name for all index files. If you use `index` tou
can later skip the `-e INDEX_PREFIX=<prefix>` flag.

To include a text collection, the wordsfile (see below for the required format)
has to be passed with `-w`.  To support text snippets, a docsfile (see below for
the required format)has to be passed with `-d`

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

## Running queries:

    curl 'http://localhost:7001/?query=SELECT ?x WHERE {?x <rel> ?y}'

or visit:

    http://localhost:7001/index.html

## Text Features

### Input Data

The following two input files are needed for full feature support:

#### Wordsfile

A tab-separated file with one line per posting and following the format:

    word    isEntity    recordId   score

For example, for a sentence `Alexander Fleming discovered penicillin, a drug.`,
it could look like this when using contexts as records. In this case the
`penicilin` entity was prepended to the second context by the CSD-IE
preprocessing. In a more simple setting each record could also be a full
sentence.

    Alexander           0   0   1
    <Alexander_Fleming> 1   0   1
    Fleming             0   0   1
    <Alexander_Fleming> 1   0   1
    dicovered           0   0   1
    penicillin          0   0   1
    <Penicillin>        1   0   1
    penicillin          0   1   1
    <Penicillin>        1   1   1
    a                   0   1   1
    drug                0   1   1

Note that some form of entity recognition / linking has been done.
This file is used to build the text index from.

#### Docsfile

A tab-separated file with one line per original unit of text and following the format:

    max_record_id  text

For example, for the sentence above:

    1   Alexander Fleming discovered penicillin, a drug.

Note that this file is only used to display proper excerpts as evidence for texttual matches.

### Supported Queries


Typical SPARQL queries can then be augmented. The features are best explained using examples:

A query for plants with edible leaves:

    SELECT ?plant WHERE {
        ?plant <is-a> <Plant> .
        ?t ql:contains-entity ?plant .
        ?t ql:contains-word "'edible' 'leaves'"
    }

The special predicate `ql:contains-entity` requires that results for `?plant` have to occur in a text record `?t`.
In records matching `?t`, there also have to be both words `edible` and `leaves` as specified thorugh the `ql:contains-word` predicate.
Note that the single quotes can also be omitted and will be in further examples.
Single quotes are necessary to mark phrases (which are not supported yet, but may be in the future).

A query for Astronauts who walked on the moon:

    SELECT ?a TEXT(?t) SCORE(?t) WHERE {
        ?a <is-a> <Astronaut> .
        ?t ql:contains-entity ?a .
        ?t ql:contains-word "walk* moon"
    } ORDER BY DESC(SCORE(?t))
    TEXTLIMIT 2

Note the following features:

* A star `*` can be used to search for a prefix as done in the keyword `walk*`. Note that there is a min prefix size depending on settings at index build-time.
* `SCORE` can be used to obtain the score of a text match. This is important to achieve a good ordering in the result. The typical way would be to `ORDER BY DESC(SCORE(?t))`.
* Where `?t` just matches a text record Id, `TEXT(?t)` can be used to extract a snippet.
* `TEXTLIMIT` can be used to control the number of result lines per text match. The default is 1.

An alternative query for astronauts who walked on the moon:

        SELECT ?a TEXT(?t) SCORE(?t) WHERE {
            ?a <is-a> <Astronaut> .
            ?t ql:contains-entity ?a .
            ?t ql:contains-word "walk*" .
            ?t ql:contains-entity <Moon> .
        } ORDER BY DESC(SCORE(?t))
        TEXTLIMIT 2

This query doesn't search for an occurrence of the word moon but played where the entity `<Moon>` has been linked.
For the sake of brevity, it is possible to treat the concrete URI `<Moon>` like word and include it in the `contains-word` triple.
This can be convenient but should be avoided to keep queries readable: `?t ql:contains-word "walk* <Moon>"`


Text / Knowledge-base data can be nested in queries. This allows queries like one for politicians that were friends with a scientist associated with the manhattan project:

    SELECT ?p TEXT(?t) ?s TEXT(?t2) WHERE {
        ?p <is-a> <Politician> .
        ?t ql:contains-entity ?p .
        ?t ql:contains-word friend* .
        ?t ql:contains-entity ?s .
        ?s <is-a> <Scientist> .
        ?t2 ql:contains-entity ?s .
        ?t2 ql:contains-word "manhattan project"
    } ORDER BY DESC(SCORE(?t))


For now, each text-record variable is required to have a triple `ql:contains-word/entity WORD/URI`.
Pure connections to variables (e.g. "Books with a description that mentions a plant.") are planned for the future.

To obtain a list of available predicates and their counts `ql:has-predicate` can be used if the index was build with the `--patterns` option, and the server was started with the `--patterns` option:

    SELECT ?relations (COUNT(?relations) as ?count) WHERE {
      ?s <is-a> <Scientist> .
      ?t2 ql:contains-entity ?s .
      ?t2 ql:contains-word "manhattan project" .
      ?s ql:has-predicate ?relations .
    }
    GROUP BY ?relations
    ORDER BY DESC(?count)

`ql:has-predicate` can also be used as a normal predicate in an arbitrary query.

Group by is supported, its aggregates can be used both for selecting as well as in ORDER BY clauses:

    SELECT ?profession (AVG(?height) as ?avg) WHERE {
      ?a <is-a> ?profession .
      ?a <Height> ?height .
    }
    GROUP BY ?profession
    ORDER BY ?avg
    HAVING (?profession > <H)


    SELECT ?profession  WHERE {
      ?a <is-a> ?profession .
      ?a <Height> ?height .
    }
    ORDER BY (AVG(?height) as ?avg)
    GROUP BY ?profession

Supported aggregates are `MIN, MAX, AVG, GROUP_CONCAT, SAMPLE, COUNT, SUM`. All of the aggreagates support `DISTINCT`, e.g. `(GROUP_CONCAT(DISTINCT ?a) as ?b)`.
Group concat also supports a custom separator: `(GROUP_CONCAT(?a ; separator=" ; ") as ?concat)`. Xsd types float, decimal and integer are recognized as numbers, other types or unbound variables (e.g. no entries for an optional part) in one of the aggregates that need to interpret the variable (e.g. AVG) lead to either no result or nan. MAX with an unbound variable will always return the unbound variable.
A query can only have one GROUP BY and one HAVING clause, but may have several
variables in the GROUP BY and several filters in the HAVING clause (which will
then be concatenated using and)

    SELECT ?profession ?gender (AVG(?height) as ?avg) WHERE {
      ?a <is-a> ?profession .
      ?a <Gender> ?gender .
      ?a <Height> ?height .
    }
    GROUP BY ?profession ?gender
    HAVING (?a > <H) (?gender == <Female>)


QLever has support for both OPTIONAL as well as UNION. When marking part of a query as optional the variable bindings of the optional parts are only added to results that
are created by the non optional part. Furthermore, if the optional part has no result matching the rest of the graph pattern its variables are left unbound, leading
to empty entries in the result table. UNION combines two graph patterns by appending the result of one pattern to the result of the other. Results for Variables with the same name
in both graph patterns will end up in the same column in the result table.


    SELECT ?profession ?gender (AVG(?height) as ?avg) WHERE {
      { ?a <is-a> ?profession . }
      UNION
      {
        ?a <Gender> ?gender .
        OPTIONAL {
          ?a <Height> ?height .
        }
      }
    }

## Converting Old Indices For Current QLever Versions

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


## How to obtain data to play around with

### Use the tiny examples contained in the repository


These are tiny and there's nothing meaningful to discover.
They are fine for setting up a working sever within seconds and getting
comfortable with the query language:

    QLever/misc/tiny-example.kb.nt
    QLever/misc/tiny-example.wordsfile.tsv
    QLever/misc/tiny-example.docsfile.tsv

Note that we left out stopwords (unlike in the docsfile) to demonstrate how this
can be done if desired.
If you build an index using these files and ask the query:

    SELECT ?x TEXT(?t) WHERE {
        ?x <is-a> <Scientist> .
        ?t ql:contains-entity ?x .
        ?t ql:contains-word "penicillin"
    }  ORDER BY DESC(SCORE(?t))

You should find `<Alexander_Fleming>` and the textual evidence for that match.

You can also display his awards or find `<Albert_Einstein>` and his awards with
the following query:

    SELECT ?x ?award TEXT(?t) WHERE {
        ?x <is-a> <Scientist> .
        ?t ql:contains-entity ?x .
        ?t ql:contains-word "theory rela*" .
        ?x <Award_Won> ?award
    }  ORDER BY DESC(SCORE(?t))

have a look at the (really tiny) input files to get a feeling for how this works.

Curl-versions (ready for copy&paste) of the queries:

    SELECT ?x TEXT(?t) WHERE \{ ?x <is-a> <Scientist> . ?t ql:contains-entity ?x . ?t ql:contains-word \"penicillin\" \} ORDER BY DESC(SCORE(?t))

    SELECT ?x ?award TEXT(?t) WHERE \{ ?x <is-a> <Scientist> . ?t ql:contains-entity ?x . ?t ql:contains-word \"theory rela*\" . ?x <Award_Won> ?award \}  ORDER BY DESC(SCORE(?t))

Again, there's not much to be done with this data.
For a meaningful index, use the example data below.

### Download prepared input files for a collection about scientists

These files are of medium size (facts about scientists - only one hop from a scientist in a knowledge graph. Text are Wikipedia articles about scientists.)
Includes a knowledge base as nt file, and a words- and docsfile as tsv.

[scientist-collection.zip](http://filicudi.informatik.uni-freiburg.de/bjoern-data/scientist-collection.zip):

* 78 MB zipped
* 318 MB unzipped
* 350 k facts
* 11.7 m text postings

Here is a sample query to try and check if everything worked for you:

    SELECT ?x SCORE(?t) TEXT(?t) WHERE {
        ?x <is-a> <Scientist> .
        ?t ql:contains-entity ?x .
        ?t ql:contains-word "relati*"
    }
    ORDER BY DESC(SCORE(?t))

Curl-version (ready for copy&paste) of the query:

    SELECT ?x SCORE(?t) TEXT(?t) WHERE \{ ?x <is-a> <Scientist> . ?t ql:contains-entity ?x . ?t ql:contains-word \"relati*\" \} ORDER BY DESC(SCORE(?t))

### Download prepared input for English Wikipedia text and a KB derived from Freebase


Includes a knowledge base as nt file, and a words- and docsfile as tsv.  Text
and facts are basically equivalent to the
[Broccoli](http://broccoli.cs.uni-freiburg.de) search engine.

[wikipedia-freebase.zip](http://filicudi.informatik.uni-freiburg.de/bjoern-data/wikipedia-freebase.zip):

* 21 GB zipped
* 103 GB unzipped
* 372 million facts
* 2.8 billion text postings

Here is a sample query to try and check if everything worked for you:

    SELECT ?x SCORE(?t) TEXT(?t) WHERE {
        ?x <is-a> <Astronaut> .
        ?t ql:contains-entity ?x .
        ?t ql:contains-word "walk* moon"
    }
    ORDER BY DESC(SCORE(?t))

Curl-version (ready for copy&paste) of the query:

    SELECT ?x SCORE(?t) TEXT(?t) WHERE \{ ?x <is-a> <Astronaut> . ?t ql:contains-entity ?x . ?t ql:contains-word \"walk* moon\" \} ORDER BY DESC(SCORE(?t))

### Use any knowledge base and text collection of your choice


Create the files similar to the three files provided as sample downloads for
other data sets.  Usually, knowledge base files do not have to be changed. Only
words- and docsfile have to be produced.


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


### High RAM Usage During Runtime

QLever uses an on-disk index and is usually able to operate with pretty low RAM
usage. However, there are data layouts that currently lead to an excessive
amount of memory being used. Firstly, note that even very large text corpora have little impact on
memory usage. Larger KBs are much more problematic.
There are two things that can contribute to high RAM usage (and large startup
times) during runtime:

1) The size of the KB vocabulary. Using the -l flag while building the index
causes long and rarely used strings to be externalized to
disk. This saves a significant amount of memory at little to no time cost for
typical queries. The strategy can be modified to be more aggressive (currently
by editing directly in the code during index construction)

2) Building all 6 permutations over large KBs (or generelly having a
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
