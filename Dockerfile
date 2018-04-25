FROM alpine

RUN apk update
RUN mkdir -p /etc/
RUN mkdir -p /plugin/

COPY ./bin/csc /csc
COPY ./bin/nexentaedge-csi-plugin /nexentaedge-csi-plugin

ENTRYPOINT ["/nexentaedge-csi-plugin"]
