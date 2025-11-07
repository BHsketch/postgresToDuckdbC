#!/bin/bash

benchPath=$1

extension="${benchPath##*.}"

# Check the extension to determine if it's a C or C++ file
if [[ "$extension" == "c" ]]; then
  echo "$benchPath is a C file."
  gcc "$benchPath" -I duckdb/src/include -L duckdb/build/release/src -lduckdb -Wl,-rpath=$(pwd)/duckdb/build/release/src -o test 
elif [[ "$extension" == "cpp" || "$extension" == "cc" || "$extension" == "cxx" ]]; then
  echo "$benchPath is a C++ file."
  g++ "$benchPath" -I duckdb/src/include -L duckdb/build/release/src -lduckdb -Wl,-rpath=$(pwd)/duckdb/build/release/src -o test 
else
  echo "$benchPath is neither a C nor a C++ file based on its extension."
fi


