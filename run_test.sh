#!/usr/bin/env bash

cd "$(dirname "$0")"
DIR=$(pwd)

cd $DIR/ignis
export GOPATH=$DIR//debug

mkdir -p ../debug

mpirun "$@" bash -c 'go test -p 1 -coverpkg=./executor/core/... -coverprofile=../debug/coverage_$PMI_RANK.out -v ./... > ../debug/log$PMI_RANK.txt'
cat ../debug/log0.txt
head -n 1 ../debug/coverage_0.out > ../debug/coverage.out
tail -n +2 -q ../debug/coverage_*.out >> ../debug/coverage.out
go tool cover -html=../debug/coverage.out -o ../debug/coverage.html