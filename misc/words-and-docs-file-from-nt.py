"""
Copyright 2020 University of Freiburg
Hannah Bast <bast@cs.uni-freiburg.de>
"""

import sys
import re

def write_words_and_docs_file_from_nt(
        nt_file_name, words_file_name, docs_file_name):
    """
    Read triples from NT file and for each literal, create a text record
    containing that literal as entity and the words from the literal as words.

    The content in the docs file is not important, but we need a docs file
    because QLever currently crashes without a <base>.text.docsDB file
    """
    
    with open(nt_file_name, "r") as nt_file, \
         open(words_file_name, "w") as words_file, \
         open(docs_file_name, "w") as docs_file:
        record_id = 0
        for triple in nt_file:
            # Check if object is a literal.
            literal = re.search("(\"(.*)\"(\^\^<.*>|@[a-z]+|))\s*\.?\s*$", triple)
            if not literal:
                continue
            entity = literal.group(1)
            contents = literal.group(2)
            datatype = literal.group(3)
            # Discard numeric literals.
            if re.search("XMLSchema#(integer|int|double|decimal|bool)>", datatype):
                continue
            # Write entity and words to words file.
            print("%s\t1\t%d\t1" % (entity, record_id), file = words_file)
            for word in re.split("\W+", contents):
                if len(word) > 0:
                    print("%s\t0\t%d\t1" % (word.lower(), record_id),
                          file = words_file)
            # Write dummy entry to docs file.
            print("%d\tLiteral #%d" % (record_id, record_id), file = docs_file)
            record_id += 1

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 words-and-docs-file-from-nt.py <base name>")
        sys.exit(1)
    base_name = sys.argv[1]
    nt_file_name = base_name + ".nt"
    words_file_name = base_name + ".wordsfile.tsv"
    docs_file_name = base_name + ".docsfile.tsv"
    write_words_and_docs_file_from_nt(
            nt_file_name, words_file_name, docs_file_name)
