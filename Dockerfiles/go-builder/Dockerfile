
ARG REGISTRY=""
ARG NAMESPACE="ignishpc/"
ARG TAG=""
FROM ${REGISTRY}${NAMESPACE}builder${TAG}
ARG DOCK_DIR=""

ENV GO_VERSION=1.22
RUN export DEBIAN_FRONTEND=noninteractive && \
    apt update && \
    apt -y --no-install-recommends install golang-${GO_VERSION} && \
    ln -s /usr/lib/go-${GO_VERSION}/bin/go /usr/bin/go && \
    rm -rf /var/lib/apt/lists/*

COPY / ${IGNIS_HOME}/core/go/core

RUN mkdir ${IGNIS_HOME}/lib/go && \
    ln -s ${IGNIS_HOME}/core/go/core/ignis ${IGNIS_HOME}/lib/go/core && \
    mkdir -p /tmp/ignis && \
    cd /tmp/ignis && \
    ln -s ${IGNIS_HOME}/lib/go libs && \
    mv ${IGNIS_HOME}/core/go/core/ignis/main.go ./ && \
    printf "module ignis-go\ngo ${GO_VERSION}\nrequire ignis v0.0.0\n replace ignis => ./libs/core" > go.mod && \
    export C_INCLUDE_PATH=${IGNIS_DEVEL}/mpi/include/ && \
    export LIBRARY_PATH=${IGNIS_DEVEL}/mpi/lib/ && \
    go mod tidy && \
    go build -trimpath -o ${IGNIS_HOME}/bin/ignis-go && \
    rm -fR /tmp/ignis

COPY ${DOCK_DIR}bin/* ${IGNIS_HOME}/bin/
RUN chmod +x ${IGNIS_HOME}/bin/* && \
    envsubst <ignis-go-install.sh | sponge ignis-go-install.sh
