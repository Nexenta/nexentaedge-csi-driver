PLUGIN_NAME=nexentaedge-csi-plugin
IMAGE_NAME=antonskriptsov/$(PLUGIN_NAME)
IMAGE_TAG=latest

.PHONY: all nfs 

all: nfs

test:
	go test github.com/Nexenta/nexentaedge-csi-driver/csi/... -cover
	go vet  github.com/Nexenta/nexentaedge-csi-driver/csi/...
nfs:
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o bin/$(PLUGIN_NAME) ./app

build-container: nfs 
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

push-container: build-container
	docker push $(IMAGE_NAME):$(IMAGE_TAG)

clean:
	go clean -r -x
	-rm -rf bin
