FROM alpine

RUN apk update
RUN mkdir -p /etc/

COPY ./app/nexentaedge.json /etc/nexentaedge.json
COPY ./bin/nexentaedge-csi-plugin /nexentaedge-csi-plugin

ENTRYPOINT ["/nexentaedge-csi-plugin"]
