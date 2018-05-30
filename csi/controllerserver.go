package csi

import (
	"fmt"

	"github.com/Nexenta/nexentaedge-csi-driver/csi/nedgeprovider"

	"github.com/Nexenta/nexentaedge-csi-driver/csi/nexentaedge"
	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type controllerServer struct {
	*csicommon.DefaultControllerServer
}

func nedgeVolumeToCSIVolume(volume *csi.Volume, nedgeVolume *nedgeprovider.NedgeNFSVolume) {
	volume.Id = nedgeVolume.VolumeID
	volume.Attributes = make(map[string]string)
	volume.Attributes["share"] = nedgeVolume.Share
}

func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	log.Infof("CreateVolume req[%+v]", *req)

	nedge, err := nexentaedge.InitNexentaEdge()
	if err != nil {
		log.Fatal("Failed to get NexentaEdge instance")
		return nil, err
	}
	// Volume Name
	volumeName := req.GetName()
	if len(volumeName) == 0 {
		volumeName = "csi-volume-" + uuid.NewUUID().String()
	}

	// CreateVolume response
	resultVolume := &csi.Volume{}
	resp := &csi.CreateVolumeResponse{
		Volume: resultVolume,
	}

	volumeID, err := nedgeprovider.ParseVolumeID(volumeName)
	if err != nil {
		log.Infof("Failed to GetVolumeID(%s): %v", volumeID, err)
		return nil, err
	}

	nedgeVolume, err := nedge.GetVolume(volumeName)
	//volume already exists, returns
	if nedgeVolume != nil {
		nedgeVolumeToCSIVolume(resultVolume, nedgeVolume)
		return resp, nil
	}

	// get params from command params
	//service := req.GetParameters()["service"]
	//cluster := req.GetParameters()["cluster"]
	//tenant := req.GetParameters()["tenant"]

	// Volume Create
	log.Info("Creating volume: ", volumeID)
	err = nedge.CreateVolume(volumeName, 100)
	if err != nil {
		log.Infof("Failed to CreateVolume: %v", err)
		return nil, err
	}

	newNedgeVolume, err := nedge.GetVolume(volumeName)
	if newNedgeVolume == nil {
		log.Infof("Failed to get created volume by name, %v", err)
		return nil, err
	}

	// Return information on existing volume
	nedgeVolumeToCSIVolume(resultVolume, newNedgeVolume)
	return resp, nil
}

func (cs *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	log.Infof("DeleteVolume req[%+v]", *req)

	nedge, err := nexentaedge.InitNexentaEdge()
	if err != nil {
		log.Fatal("Failed to get NexentaEdge instance")
		return nil, err
	}

	// VolumeID
	volumeID := req.GetVolumeId()
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume id must be provided")
	}

	// If the volume is not found, then we can return OK
	if nedge.IsVolumeExist(volumeID) == false {
		log.Infof("DeleteVolume:IsVolumeExist volume %s does not exist", volumeID)
		return &csi.DeleteVolumeResponse{}, nil
	}

	err = nedge.DeleteVolume(volumeID)
	if err != nil {
		e := fmt.Sprintf("Unable to delete volume with id %s: %s",
			req.GetVolumeId(),
			err.Error())
		log.Errorln(e)
		return nil, status.Error(codes.Internal, e)
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *controllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	log.Infof("ControllerPublishVolume req[%#v]", req)

	// Volume Attach
	instanceID := req.GetNodeId()
	volumeID := req.GetVolumeId()

	log.Info("ControllerPublishVolume ", volumeID, " on ", instanceID)

	return &csi.ControllerPublishVolumeResponse{}, nil
}

func (cs *controllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	log.Infof("ControllerUnpublishVolume req[%#v]", req)
	// Volume Detach
	instanceID := req.GetNodeId()
	volumeID := req.GetVolumeId()

	log.Info("ControllerUnpublishVolume ", volumeID, "on ", instanceID)

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (cs *controllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	log.Infof("ControllerListVolumes req[%#v]", req)

	nedge, err := nexentaedge.InitNexentaEdge()
	if err != nil {
		log.Fatal("Failed to get NexentaEdge instance")
		return nil, err
	}

	volumes, err := nedge.ListVolumes()
	log.Info("ControllerListVolumes ", volumes)

	entries := make([]*csi.ListVolumesResponse_Entry, len(volumes))
	for i, v := range volumes {
		// Initialize entry
		entries[i] = &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{},
		}
		nedgeVolumeToCSIVolume(entries[i].Volume, &v)
	}

	return &csi.ListVolumesResponse{
		Entries: entries,
	}, nil
}
