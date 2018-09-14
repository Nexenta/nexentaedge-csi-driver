PLUGIN_NAME=nexentaedge-csi-plugin
IMAGE_NAME=$(PLUGIN_NAME)
DOCKER_FILE=Dockerfile.centos7
REGISTRY=nexenta
IMAGE_TAG=latest

.PHONY: all nfs 

all: nfs

test:
	go test github.com/Nexenta/nexentaedge-csi-driver/csi/... -cover
	go vet  github.com/Nexenta/nexentaedge-csi-driver/csi/...
nfs:
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o bin/$(PLUGIN_NAME) ./app

build-container: nfs 
	docker build -f $(DOCKER_FILE) -t $(IMAGE_NAME) .

push-container: build-container
	docker tag  $(IMAGE_NAME) $(REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)
	docker push $(REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)

clean:
	go clean -r -x
	-rm -rf bin
