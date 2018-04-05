package csi

import (
    "github.com/Nexenta/nexentaedge-csi-driver/csi/nexentaedge"
    log "github.com/sirupsen/logrus"
    csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
    csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
    "github.com/pborman/uuid"
    "golang.org/x/net/context"
)

type controllerServer struct {
    *csicommon.DefaultControllerServer
}

func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {

    // Volume Name
    volName := req.GetName()
    if len(volName) == 0 {
        volName = uuid.NewUUID().String()
    }

    // Volume Type
    //volType := req.GetParameters()["type"]

    // Volume Availability
    //volAvailability := req.GetParameters()["availability"]

    // Volume Create
    resID, err := nexentaedge.CreateVolume(volName)
    if err != nil {
        log.Info("Failed to CreateVolume: ", err)
        return nil, err
    }

    log.Info("Create volume ", resID)

    return &csi.CreateVolumeResponse{
        Volume: &csi.Volume{
            Id: resID,
        },
    }, nil
}

func (cs *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {

    // Volume Delete
    volID := req.GetVolumeId()
    err := nexentaedge.DeleteVolume(volID)
    if err != nil {
        log.Info("Failed to DeleteVolume: ", err)
        return nil, err
    }

    log.Info("Delete volume :", volID)

    return &csi.DeleteVolumeResponse{}, nil
}

func (cs *controllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {

    // Volume Attach
    instanceID := req.GetNodeId()
    volumeID := req.GetVolumeId()

    log.Info("ControllerPublishVolume ", volumeID, " on ", instanceID)

    // Publish Volume Info
    pvInfo := map[string]string{}
    //pvInfo["DevicePath"] = devicePath

    return &csi.ControllerPublishVolumeResponse{
        PublishInfo: pvInfo,
    }, nil
}

func (cs *controllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {

    // Volume Detach
    instanceID := req.GetNodeId()
    volumeID := req.GetVolumeId()

    log.Info("ControllerUnpublishVolume ", volumeID, "on ", instanceID)

    return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (cs *controllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
    log.Info("ControllerListVolumes ")
    entries := make([]*csi.ListVolumesResponse_Entry, 0)
    return &csi.ListVolumesResponse{
		Entries: entries,
	}, nil
}


