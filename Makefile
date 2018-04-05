IMAGE_NAME=nexenta/nexentaedge-csi-plugin
IMAGE_TAG=stable
PLUGIN_BIN=nexentaedge-csi-plugin

.PHONY: all nfs 

all: nfs

test:
	go test github.com/Nexenta/nexentaedge-csi-driver/csi/... -cover
	go vet  github.com/Nexenta/nexentaedge-csi-driver/csi/...
nfs:
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o bin/$(PLUGIN_BIN) ./app

build-container: nfs 
	cp bin/$(PLUGIN_BIN) deploy/docker
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) deploy/docker

clean:
	go clean -r -x
	-rm -rf bin
