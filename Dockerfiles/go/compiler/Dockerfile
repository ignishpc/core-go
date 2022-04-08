
ARG REGISTRY=""
ARG NAMESPACE="ignishpc/"
ARG TAG=""
FROM ${REGISTRY}${NAMESPACE}go-builder${TAG} as go-builder
FROM ${REGISTRY}${NAMESPACE}common${TAG}
ARG RELPATH=""

COPY --from=go-builder ${IGNIS_HOME} ${IGNIS_HOME}

ENV GOPATH=${IGNIS_HOME}/core/go/
RUN ignis-go-install.sh && \
	mv ${IGNIS_HOME}/core/go/include/* /usr/include && \
	rm -f ${IGNIS_HOME}/bin/ignis-go-install.sh
