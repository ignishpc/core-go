#!/bin/bash

export DEBIAN_FRONTEND=noninteractive
apt update
apt -y install --no-install-recommends gcc golang-${GO_VERSION}
rm -rf /var/lib/apt/lists/*
