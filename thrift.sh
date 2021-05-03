#!/bin/bash
cd "$(dirname "$0")"

if [ $# -ne 1 ] || [ ! -d "$1"/ignis/rpc/ ] || [ ! -f $(find "$1"/ignis/rpc/ -name "*thrift" | head -n 1) ];
    then echo "usage thrift.sh <rpc-folder>"
    exit
fi


out="."
rm -fr "$out/ignis/rpc/"
for file in `find $1/ignis -name "*thrift"`; do
    thrift --gen go -out $out $file
done



