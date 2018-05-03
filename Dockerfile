FROM alpine:3.2

RUN apk update
RUN mkdir -p /etc/
RUN mkdir -p /config/

COPY ./bin/csc /csc
COPY ./bin/nexentaedge-csi-plugin /nexentaedge-csi-plugin

ENTRYPOINT ["/nexentaedge-csi-plugin"]
