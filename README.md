SparqlEngineDraft
=================

How to use
==========

0. Requirements:
----------------

Make sure you use a 64bit Linux with:

* git
* g++ 4.8 or higher
* CMake 2.8.4 or higher

Other compilers (and OS) are not supported, yet. 
So far no major problems are known. 
Support for more platforms would be a highly appreciated contribution.

1. Build:
---------

a) Checkout this project:

    git clone https://github.com/Buchhold/SparqlEngineDraft.git --recursive

Don't forget --recursive so that submodules will be updated. 
For old versions of git, that do not support this parameter, you can do:

    git clone https://github.com/Buchhold/SparqlEngineDraft.git
    cd SparqlEngineDraft
    git submodule init
    git submodule update
    

b) Go to a folder where you want to build the binaries.
Don't do this directly in SparqlEngineDraft

    cd /path/to/YOUR_FOLDER

c) Build the project

    cmake /path/to/SparqlEngineDraft/ -DCMAKE_BUILD_TYPE=Release; make -j

d) Run ctest. All tests should pass:

    ctest


2. Creating an Index:
---------------------

IMPORTANT:
THERE HAS TO BE SUFFICIENT DISK SPACE IN UNDER THE PATH USE CHOOSE FOR YOUR INDEX!
FOR NOW - ALL FILES HAVE TO BE UTF8 ENCODED!

    You can use the files described and linked later in this document: 
    "How to obtain data to play around with"

a) from an NTriples file (currently no blank nodes allowed):

    ./IndexBuilderMain -i /path/to/myindex -n /path/to/input.nt

b) from a TSV File (no spaces / tabs in spo):

    ./IndexBuilderMain -i /path/to/myindex -t /path/to/input.tsv 

To include a text collection, the wordsfile (see below for the required format) has to be passed with -w.
To support text snippest a docsfile (see below for the required format)has to be passed with -d

The full call will look like this:

    ./IndexBuilderMain -i /path/to/myindex -n /path/to/input.nt -w /path/to/wordsfile -d /path/to/docsfile 
    
If you want to use predicate variables (perfectly normal for SPARQL but rarely used in semantic text queries), use an optional argument -a to build all permutations:

    ./IndexBuilderMain -i /path/to/myindex -n /path/to/input.nt -a -w /path/to/wordsfile -d /path/to/docsfile 
3. Starting a Sever:
--------------------

a) Without text collection:

    ./ServerMain -i /path/to/myindex -p <PORT>

b) With text collection:

    ./ServerMain -i /path/to/myindex -p <PORT> -t

Depending on if you built the index with the -a version, two or six index permutations will be registered.
For some data this can be a significant difference in memory consumption.

4. Running queries:
-------------------

    curl 'http://localhost:<PORT>/&query=SELECT ?x WHERE {?x <rel> ?y}'

or visit:

    http://localhost:<PORT>/index.html
    
5. Text Features
----------------

5.1 Input Data
--------------
The following two input files are needed for full feature support:

a) Wordsfile
------------
A tab-separated file with one line per posting and following the format:

    word    isEntity    contextId   score
        
For example, for a sentence "He discovered penicillin, a drug.", it could look like this:

    He                  0   0   1
    <Alexander_Fleming> 1   0   1
    dicovered           0   0   1
    penicillin          0   0   1
    <Penicillin>        1   0   1
    penicillin          0   1   1
    <Penicillin>        1   1   1
    a                   0   1   1
    drug                0   1   1


This file is used to build the text index from.

b) Docsfile
-----------
A tab-separated file with one line per original unit of text and following the format:

    max_context_id  text
    
For example, for the sentence above:

    1   He discovered penicillin, a drug.
    
Note that this file is only used to display proper excerpts as evidence for texttual matches.



5.2 Supported Queries
---------------------

Typical SPARQL queries can then be augmented. The features are best explained using examples:

A query for plants with edible leaves:

    SELECT ?plant WHERE { 
        ?plant <is-a> <Plant> . 
        ?plant <in-context> ?c . 
        ?c <in-context> edible leaves
    } 
    
The special relation `<in-context>` to state that results for `?plant` have to occur in a context `?c`.
In contexts matching `?c`, there also have to be oth words `edible` and `leaves`.

A query for Astronauts who walked on the moon:

    SELECT ?a TEXT(?c) SCORE(?c) WHERE {
        ?a <is-a> <Astronaut> . 
        ?a <in-context> walk* moon
    } ORDER BY DESC(SCORE(?c))
    TEXTLIMIT 2
    
Note the following features:

* A star `*` can be used to search for a prefix as done in the keyword `walk*`. Note that there is a min prefix size depending on settingsat index build-time.
* `SCORE` can be used to obtain the score of a text match. This is important to acieve a good ordering in the result. The typical way would be to `ORDER BY DESC(SCORE(?c))`.
* Where `?c` just matches a context Id, `TEXT(?c)` can be used to extract a snippet.
* `TEXTLIMIT` can be used to control the number of result lines per text match. The default is 1.

An alternative query for astronauts who walked on the moon:

    SELECT ?a TEXT(?c) SCORE(?c) WHERE {
        ?a <is-a> <Astronaut> . 
        ?a <in-context> walk* <Moon> 
    } ORDER BY DESC(SCORE(?c))

This query doesn't search for an occurrence of the word moon but played where the entity `<Moon>` has been linked.


Text / Knowledge-base data can be nested in queries. This allows queries like one for politicians that were friends with a scientist associated with the manhattan project:

    SELECT ?p TEXT(?c) ?s TEXT(?c2) WHERE
        ?p <is-a> <Politician> .
        ?p <in-context> ?c .
        ?c <in-context> friend*
        ?c <in-context> ?s .
        ?s <is-a> <Scientist> .
        ?s <in-context> ?c2 .
        ?c2 <in-context> manhattan project 
    } ORDER BY DESC(SCORE(?c))

In addition to `<in-context>` there is another special relation `<has-context>` that is useful when search for documents.

A query for documents that state that a plan has edible leaves:

    SELECT ?doc ?plant WHERE { 
        ?doc <has-context> ?c
        ?plant <is-a> <Plant> . 
        ?plant <in-context> ?c . 
        ?c <in-context> edible leaves
    }    

Again, features can be nested.

A query for books with descriptions that contain the word drug.

    SELECT ?book TEXT(?c) WHERE {
        ?book <is-a> <Book> .
        ?book <description ?d .
        ?d <has-context> ?c .
        ?c <in-context> drug
    }
    
Note the use the the relation `has-context` that links the context to a text source (in this case the description) that may be an entity itself.


For now, each context is required to have a triple `<in-context> ENTITY/WORD`. 
Pure connections to variables (e.g. "Books with a description that mentions a plant.") are planned for the future.



How to obtain data to play around with
======================================

1. Use the tiny examples contained in the repository
--------------------------------------------------

These are tiny and there's nothing meaningful to discover.

They are fine for setting up a working sever within seconds and getting comfortable with the query language.

TODO: add and describe content and queries

2. Download these prepared input files
-------------------------------

TODO: describe.


3. Download prepared input for English Wikipedia text and a KB derived from Freebase
--------------------------

TODO: example queries


4. Use any knowledge base and text collection of your choice
------------------------
 
TODO: give examples + describe how to do ER

