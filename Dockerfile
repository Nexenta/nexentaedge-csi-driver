FROM alpine

RUN apk update
RUN mkdir -p /etc/
RUN mkdir -p /plugin/

COPY ./app/nexentaedge-config.json /etc/nexentaedge-config.json
COPY ./bin/nexentaedge-csi-plugin /nexentaedge-csi-plugin

ENTRYPOINT ["/nexentaedge-csi-plugin"]
