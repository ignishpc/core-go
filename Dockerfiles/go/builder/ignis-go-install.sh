#!/bin/bash

mv ${IGNIS_HOME}/core/go/go /usr/local/
ln -s /usr/local/go/bin/go /usr/bin/go
ln -s /usr/local/go/bin/gofmt /usr/bin/gofmt

cp -R ${IGNIS_HOME}/core/go/lib/* ${IGNIS_HOME}/lib
rm -fR ${IGNIS_HOME}/core/go/lib
ldconfig
