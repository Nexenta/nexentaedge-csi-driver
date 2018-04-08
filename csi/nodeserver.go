package csi

import (
	"fmt"
	"os"
	"strings"

	"github.com/Nexenta/nexentaedge-csi-driver/csi/nexentaedge"
	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/volume/util"
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

	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()
	// Check arguments
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume id must be provided")
	}
	if len(targetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path must be provided")
	}

	nedge, err := nexentaedge.GetNexentaEdgeProvider()
	if err != nil {
		log.Fatalf("Failed to get NexentaEdgeProvider instance %v", err)
	}

	nedgeVolume := nedge.GetVolume(volumeID)
	if nedgeVolume == nil {
		log.Infof("No %s volume found for volumeID: %s ", volumeID)
		return nil, status.Errorf(codes.NotFound, "Volume id %s not found", volumeID)
	}

	mounter := mount.New("")
	notMnt, err := mounter.IsLikelyNotMountPoint(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(targetPath, 0750); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			notMnt = true
		} else {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	if !notMnt {
		return &csi.NodePublishVolumeResponse{}, nil
	}

	source := fmt.Sprintf("%s:%s", nedge.GetDataIP(), nedgeVolume["share"])
	log.Infof("NexentaEdge exports %s as %s", volumeID, source)

	err = mounter.Mount(source, targetPath, "nfs", nil)
	if err != nil {
		if os.IsPermission(err) {
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		if strings.Contains(err.Error(), "invalid argument") {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	//log.Info("NodePublishVolume invoked: targetPath:", targetPath, ", fsType:", fsType, " devicePath:", devicePath)
	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	log.Infof("NodeUnpublishVolume req[%#v]", req)

	/*
		nedge, err := nexentaedge.GetNexentaEdgeProvider()
		if err != nil {
			log.Fatalf("Failed to get NexentaEdgeProvider instance %v", err)
		}

		// Check arguments
		if len(req.GetVolumeId()) == 0 {
			return nil, status.Error(codes.InvalidArgument, "Volume id must be provided")
		}

		//log.Info("NodeUnpublishVolume invoked: targetPath:", targetPath)
	*/
	targetPath := req.GetTargetPath()
	notMnt, err := mount.New("").IsLikelyNotMountPoint(targetPath)

	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Error(codes.NotFound, "Targetpath not found")
		} else {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	if notMnt {
		return nil, status.Error(codes.NotFound, "Volume not mounted")
	}

	err = util.UnmountPath(req.GetTargetPath(), mount.New(""))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	//return &csi.NodeUnstageVolumeResponse{}, nil

	return nil, status.Error(codes.Unimplemented, "")
}

func (ns *nodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	//return &csi.NodeStageVolumeResponse{}, nil
	return nil, status.Error(codes.Unimplemented, "")
}
