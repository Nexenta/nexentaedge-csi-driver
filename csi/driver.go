package csi

import (
	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/kubernetes-csi/drivers/pkg/csi-common"
	log "github.com/sirupsen/logrus"
)

type driver struct {
	csiDriver *csicommon.CSIDriver
	endpoint  string

	ids *csicommon.DefaultIdentityServer
	cs  *controllerServer
	ns  *nodeServer

	cap   []*csi.VolumeCapability_AccessMode
	cscap []*csi.ControllerServiceCapability
}

const (
	DriverName = "nexentaedge-csi-plugin"
)

var (
	version = "0.2.0"
)

/*GetCSIDriver returns pointer to driver */
func GetCSIDriver() *driver {
	return &driver{}
}

/*NewDriver creates new nexentaedge csi driver with required capabilities */
func NewDriver(nodeID string, endpoint string) *driver {
	log.Info("NewDriver: ", DriverName, " version:", version)

	d := &driver{}

	d.endpoint = endpoint

	csiDriver := csicommon.NewCSIDriver(DriverName, version, nodeID)
	csiDriver.AddControllerServiceCapabilities(
		[]csi.ControllerServiceCapability_RPC_Type{
			csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
			csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
			csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
		})
	csiDriver.AddVolumeCapabilityAccessModes([]csi.VolumeCapability_AccessMode_Mode{csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER})

	d.csiDriver = csiDriver

	return d
}

/*NewControllerServer created commin controller server */
func NewControllerServer(d *driver) *controllerServer {
	return &controllerServer{
		DefaultControllerServer: csicommon.NewDefaultControllerServer(d.csiDriver),
	}
}

/*NewNodeServer creates new default Node server */
func NewNodeServer(d *driver) *nodeServer {
	return &nodeServer{
		DefaultNodeServer: csicommon.NewDefaultNodeServer(d.csiDriver),
	}
}

func (d *driver) Run() {
	csicommon.RunControllerandNodePublishServer(d.endpoint, d.csiDriver, NewControllerServer(d), NewNodeServer(d))
}
