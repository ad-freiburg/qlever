#!/usr/bin/env bash

printf "Checking sources for code style\n"
SOURCE_FILES=()
find ./src/ ./test/ -regextype egrep -regex '.*\.(h|c)(pp|xx)?$' -print0 > sourcelist
while IFS=  read -r -d $'\0'; do
    SOURCE_FILES+=("$REPLY")
done <sourcelist

ERROR=0
for source in "${SOURCE_FILES[@]}" ;do
	clang-format -output-replacements-xml $source | grep "<replacement " &> /dev/null
	HAS_WRONG_FILES=$?
	if [ $HAS_WRONG_FILES -ne 1 ] ; then
		# Print an error and exit
		printf "\x1b[31mError: "
		printf "The source file \x1b[m$source\x1b[31m does not match the code style\n"
		printf "Use clang-format with the .clang-format provided in the QLever\n"
		printf "repository's root to ensure all code files are formatted "
		printf "properly. We currently use the clang-format version in\n"
		printf "Ubuntu 18.04.x which is version 6.0\n"
		printf "\x1b[m"
		ERROR=1
	fi
done

if [ $ERROR -eq 0 ]; then
	printf "\x1b[32mCongratulations! All sources match the code style\x1b[m\n"
else
	exit 1
fi
