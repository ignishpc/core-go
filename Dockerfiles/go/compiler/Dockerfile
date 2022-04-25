
ARG REGISTRY=""
ARG NAMESPACE="ignishpc/"
ARG TAG=""
FROM ${REGISTRY}${NAMESPACE}go-builder${TAG} as go-builder
FROM ${REGISTRY}${NAMESPACE}common${TAG}
ARG RELPATH=""

COPY --from=go-builder ${IGNIS_HOME} ${IGNIS_HOME}

ENV IGNIS_GO=${IGNIS_HOME}/core/go/
RUN ignis-go-install.sh && \
	cp -R ${IGNIS_HOME}/core/go/include/* /usr/include && \
	cp -R ${IGNIS_HOME}/lib/* /usr/lib && \
	rm -f ${IGNIS_HOME}/bin/ignis-go-install.sh
