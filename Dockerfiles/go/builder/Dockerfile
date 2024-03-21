
ARG REGISTRY=""
ARG NAMESPACE="ignishpc/"
ARG TAG=""
FROM ${REGISTRY}${NAMESPACE}builder${TAG}
ARG DOCK_DIR=""

ENV GO_VERSION=1.22
RUN export DEBIAN_FRONTEND=noninteractive && \
	apt update && \
	apt -y --no-install-recommends install golang-${GO_VERSION} && \
	rm -rf /var/lib/apt/lists/* && \

COPY / ${IGNIS_HOME}/core/go/core

RUN mkdir -p ${IGNIS_HOME}/core/go/src &&  \
    source ${DOCK_DIR}env.d/go.env && \
    mkdir -p /tmp/ignis && \
    cd /tmp/ignis && \
    mv ${IGNIS_HOME}/core/go/ignis/main.go ./ && \
    printf "module ignis-go\ngo ${GO_VERSION}\nrequire ignis v0.0.0\n" > go.mod && \
    go build -trimpath -o ${IGNIS_HOME}/bin/ignis-go && \
    rm -fR /tmp/ignis

COPY ${DOCK_DIR}bin/* ${IGNIS_HOME}/bin/
RUN chmod +x ${IGNIS_HOME}/bin/* && \
    envsubst <ignis-go-install.sh | sponge ignis-go-install.sh
