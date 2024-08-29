#!/bin/bash

fuzzTime=${1:-15s}

files=$(grep -r --include='**_test.go' --files-with-matches 'func Fuzz' .)
for file in ${files}; do
  funcs=$(grep '^func Fuzz' "$file" | sed s/func\ // | sed 's/(.*$//')
  for func in ${funcs}; do
    echo "Fuzzing $func in $file"
    parentDir=$(dirname "$file")
    go test "$parentDir" -run="$func" -fuzz="$func" -fuzztime="${fuzzTime}"
  done
done
