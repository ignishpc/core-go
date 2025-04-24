#!/bin/bash

export DEBIAN_FRONTEND=noninteractive
apt update
apt -y install --no-install-recommends gcc golang-${GO_VERSION}
ln -s /usr/lib/go-${GO_VERSION}/bin/go /usr/bin/go
ln -s /usr/lib/go-${GO_VERSION}/bin/gofmt /usr/bin/gofmt
rm -rf /var/lib/apt/lists/*
