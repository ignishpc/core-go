#!/usr/bin/env bash

cd "$(dirname "$0")"
DIR=$(pwd)

cd $DIR/ignis
export GOPATH=$DIR//debug

mkdir -p ../debug
go test -coverprofile=../debug/coverage.out ./...
go tool cover -html=../debug/coverage.out -o ../debug/coverage.html