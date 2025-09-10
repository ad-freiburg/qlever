#!/usr/bin/env bash

printf "Rewriting operator== automatically\n"
SOURCE_FILES=()
#find . -not \( -path "./third_party/*" -prune \) -not \( -path "./cmake-build*" -prune \) -not \( -path "./build*" -prune \) -name \*.h -print0 -o -name \*.cpp -print0 > sourcelist
find "./test"  \( -name ""*.h"" -o -name "*.cpp" \) -print0 > sourcelist
while IFS=  read -r -d $'\0'; do
    SOURCE_FILES+=("$REPLY")
done <sourcelist

for source in "${SOURCE_FILES[@]}" ;do
  echo $source
  /local/data-ssd/kalmbacj/llvm-project/llvm/cmake-build-release/bin/backport-defaulted-equality -p cmake-build-clang-trunk-for-tools $source

done
sem --wait
echo "Finished rewriting all files"
