#!/bin/bash

#compare two indices for binary equality. Can be used for regression testing of the Index Building procedure

if [ "$#" -ne 2 ]
then
  echo "Usage: $0 <indexPrefix1> <indexPrefix2>"
  echo "Passed $# command line arguments"
  exit 1
fi



for i in .index.pso .index.pos \
         .index.spo .index.spo.meta-mmap .index.sop .index.spo.meta-mmap \
         .index.osp .index.osp.meta-mmap .index.ops .index.ops.meta-mmap \
         .index.patterns .prefixes .vocabulary .meta-data.json .literals-index
do
  f1=$1$i
  f2=$2$i
  echo "Comparing $f1 and $f2"
  if  cmp $f1 $f2
  then
    echo "$f1 and $f2 match, continuing"
  else
    echo "Error, $f1 and $f2 are not equal"
    #exit 1
  fi

done

echo "Indices with prefixes $1 and $2 are binary equal"