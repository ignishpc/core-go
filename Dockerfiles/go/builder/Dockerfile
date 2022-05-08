
ARG REGISTRY=""
ARG NAMESPACE="ignishpc/"
ARG TAG=""
FROM ${REGISTRY}${NAMESPACE}common-builder${TAG}
ARG RELPATH=""

ENV GO_VERSION=go1.18beta2
RUN cd ${IGNIS_HOME} && \
	mkdir -p core/go && \
	cd core/go && \
	wget https://go.dev/dl/${GO_VERSION}.linux-$(dpkg --print-architecture).tar.gz -O golang.tar.gz && \
	tar -xvzf  golang.tar.gz && \
	rm -fR golang.tar.gz

COPY ${RELPATH}ignis-go-install.sh ${IGNIS_HOME}/bin
RUN chmod +x ${IGNIS_HOME}/bin/ignis-go-install.sh

COPY / /tmp/core-go
RUN cd /tmp/core-go && \
	cp -R ${IGNIS_DEVEL}/mpi/include ${IGNIS_HOME}/core/go/include && \
	cp -R ${IGNIS_DEVEL}/mpi/lib ${IGNIS_HOME}/core/go/lib && \
	export CGO_CFLAGS="-I${IGNIS_HOME}/core/go/include" && \
	export LIBRARY_PATH=${IGNIS_HOME}/core/go/lib && \
    export go=${IGNIS_HOME}/core/go/go/bin/go && \
    mv ignis ${IGNIS_HOME}/core/go/ && \
    mkdir ignis && \
    mv ${IGNIS_HOME}/core/go/ignis/main.go ignis/ && \
    cd ignis && \
    printf "module ignism\ngo 1.18\nrequire ignis v0.0.0\n" > go.mod && \
    $go mod edit -replace ignis=${IGNIS_HOME}/core/go/ignis go.mod && \
    $go get && \
    $go build -trimpath -o ${IGNIS_HOME}/bin/ignis-go && \
	cd .. && \
	rm -fR /tmp/ignis-go
