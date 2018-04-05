package csi

import (
    "github.com/container-storage-interface/spec/lib/go/csi/v0"
    csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
    log "github.com/sirupsen/logrus"
    "golang.org/x/net/context"
)

type nodeServer struct {
    *csicommon.DefaultNodeServer
}

func (ns *nodeServer) NodeGetId(ctx context.Context, req *csi.NodeGetIdRequest) (*csi.NodeGetIdResponse, error) {
    log.Infof("NodeGetId req[%#v]", req)
    // Using default function
    log.Info("NodeGetId invoked")
    return ns.DefaultNodeServer.NodeGetId(ctx, req)
}

func (ns *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
    log.Infof("NodePublishVolume req[%#v]", req)
    targetPath := req.GetTargetPath()
    fsType := req.GetVolumeCapability().GetMount().GetFsType()
    devicePath := req.GetPublishInfo()["DevicePath"]

    log.Info("NodePublishVolume invoked: targetPath:", targetPath, ", fsType:", fsType, " devicePath:", devicePath)
    return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
    log.Infof("NodeUnpublishVolume req[%#v]", req)
    targetPath := req.GetTargetPath()
    log.Info("NodeUnpublishVolume invoked: targetPath:", targetPath)

    return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
    return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *nodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
    return &csi.NodeStageVolumeResponse{}, nil
}
