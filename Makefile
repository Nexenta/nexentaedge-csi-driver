IMAGE_NAME=antonskriptsov/nexentaedge-csi-plugin
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
	
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) Dockerfile .

push-container: build-container
	docker push $(IMAGE_NAME):$(IMAGE_TAG)
clean:
	go clean -r -x
	-rm -rf bin
