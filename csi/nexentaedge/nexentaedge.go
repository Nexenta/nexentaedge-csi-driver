package nexentaedge

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Nexenta/nexentaedge-csi-driver/csi/nedgeprovider"
	log "github.com/sirupsen/logrus"
)

const defaultChunkSize int = 1048576

/*INexentaEdge interface to provide base methods */
type INexentaEdge interface {
	CreateVolume(volumeName string, size int) error
	DeleteVolume(volumeID string) error
	ListVolumes() ([]nedgeprovider.NedgeNFSVolume, error)
	IsVolumeExist(volumeID string) bool
	GetVolume(volumeName string) (volume *nedgeprovider.NedgeNFSVolume)
	GetVolumeID(volumeName string) (volumeID string, err error)
	GetDataIP() string
}

type NexentaEdge struct {
	Mutex      *sync.Mutex
	provider   nedgeprovider.INexentaEdgeProvider
	k8sCluster NedgeK8sCluster
}

var NexentaEdgeInstance INexentaEdge

/*InitNexentaEdge discover nedge k8s cluster */
func InitNexentaEdge() (nedge INexentaEdge, err error) {
	log.Info("InitNexentaEdgeProvider")
	k8sCluster, err := GetNedgeCluster()
	if err != nil {
		msg := fmt.Sprintf("Can't get NexentaEdge k8s cluster instance, Error: %s", err.Error)
		log.Error(msg)
		return nil, fmt.Errorf("%s", msg)
	}

	if k8sCluster.NedgeMgmtSvc.Name == "" || k8sCluster.NedgeMgmtSvc.ClusterIP == "" {
		msg := fmt.Sprintf("Can't find k8s nedge cluster information, Error: %s", err.Error)
		log.Error(msg)
		return nil, fmt.Errorf("%s", msg)
	}

	if len(k8sCluster.NfsServices) < 1 {
		msg := "Can't find k8s nedge cluster NFS services"
		log.Error(msg)
		return nil, fmt.Errorf("%s", msg)
	}

	/* TODO change hardcoded parameters Port, login, password */
	provider := nedgeprovider.InitNexentaEdgeProvider(k8sCluster.NedgeMgmtSvc.ClusterIP, 8080, "admin", "nexenta")

	NexentaEdgeInstance = &NexentaEdge{
		Mutex:      &sync.Mutex{},
		k8sCluster: k8sCluster,
		provider:   provider,
	}

	return NexentaEdgeInstance, nil
}

/*GetDataIP returns nfs endpoint IP to create share */
func (nedge *NexentaEdge) GetDataIP() string {
	return nedge.k8sCluster.NfsServices[0].ClusterIP
}

/*IsVolumeExist check volume existance, */
func (nedge *NexentaEdge) IsVolumeExist(volumeID string) bool {

	if isVolumeID(volumeID) {
		cluster, tenant, bucket := parseVolumeID(volumeID)
		// get first service from list, should be changed later

		if nedge.provider.IsBucketExist(cluster, tenant, bucket) {
			volume := nedge.GetVolume(volumeID)
			if volume != nil {
				return true
			}
		}
	}
	return false
}

func (nedge *NexentaEdge) GetVolumeID(name string) (volumeID string, err error) {
	// get first service from list, should be changed later
	if isVolumeID(name) {
		return name, err
	}

	// take first cluster name + first tenant
	clusters, err := nedge.provider.ListClusters()
	if err != nil {
		log.Errorf("ListClusters failed Error: ", err)
		return "", err
	}

	if len(clusters) == 0 {
		log.Error("ListClusters failed Error: No available cluster name")
		err = errors.New("ListClusters failed Error: No available cluster name")
		return "", err
	}

	//get first available cluster name
	cluster := clusters[0]
	tenants, err := nedge.provider.ListTenants(cluster)
	if err != nil {
		log.Errorf("ListClusters failed Error: ", err)
		return "", err
	}

	if len(tenants) == 0 {
		log.Error("ListClusters failed Error: No available tenant name")
		err = errors.New("ListClusters failed Error: No available tenant name")
		return "", err
	}

	return cluster + "/" + tenants[0] + "/" + name, err

}

/*GetVolume returns NedgeNFSVolume if it exists, otherwise return nil*/
func (nedge *NexentaEdge) GetVolume(volumeID string) (volume *nedgeprovider.NedgeNFSVolume) {
	// get first service from list, should be changed later
	service := nedge.k8sCluster.NfsServices[0]

	volumes, err := nedge.provider.GetNfsVolumes(service.Name)
	if err != nil {
		log.Fatal("ListVolumes failed Error: ", err)
	}

	log.Info("GetVolume:ListVolumes volumes", volumes)
	log.Info("Volume name to find: ", volumeID)

	for _, v := range volumes {

		if volumeID == v.VolumeID {
			return &v
		}
	}

	return nil
}

/*CreateVolume remotely creates bucket on nexentaedge service*/
func (nedge *NexentaEdge) CreateVolume(name string, size int) (err error) {
	// get first service from list, should be changed later
	service := nedge.k8sCluster.NfsServices[0]

	if !isVolumeID(name) {
		name = "clu/ten/" + name
	}

	cluster, tenant, bucket := parseVolumeID(name)

	log.Infof("NexentaEdgeProvider:CreateVolume for serviceName: %s, %s/%s/%s, size: %d", service.Name, cluster, tenant, bucket, size)
	err = nedge.provider.CreateBucket(cluster, tenant, bucket, 100, nil)
	if err != nil {
		err = fmt.Errorf("CreateVolume failed on createBucket error: %s", err)
		return err
	}

	err = nedge.provider.ServeService(service.Name, cluster, tenant, bucket)
	if err != nil {
		err = fmt.Errorf("CreateVolume failed on serveService error: %s", err)
		return err
	}
	return err
}

/*DeleteVolume remotely deletes bucket on nexentaedge service*/
func (nedge *NexentaEdge) DeleteVolume(volumeID string) (err error) {
	log.Info("NexentaEdgeProvider:DeleteVolume  VolumeID: ", volumeID)

	// get first service from list, should be changed later
	service := nedge.k8sCluster.NfsServices[0]
	cluster, tenant, bucket := parseVolumeID(volumeID)

	log.Infof("NexentaEdgeProvider:DeleteVolume for serviceName: %s: %s/%s/%s", service, cluster, tenant, bucket)

	err = nedge.provider.UnserveService(service.Name, cluster, tenant, bucket)
	if err != nil {
		err = fmt.Errorf("DeleteVolume failed on unserveService, error: %s", err)
		return err
	}

	err = nedge.provider.DeleteBucket(cluster, tenant, bucket)
	if err != nil {
		err = fmt.Errorf("DeleteVolume failed on deleteBucket, error: %s", err)
		return err
	}

	return err
}

/*ListVolumes list all available volumes*/
func (nedge *NexentaEdge) ListVolumes() (volumes []nedgeprovider.NedgeNFSVolume, err error) {
	log.Info("NexentaEdgeProvider ListVolumes: ")
	service := nedge.k8sCluster.NfsServices[0]
	nfsVolumes, err := nedge.provider.GetNfsVolumes(service.Name)
	if err != nil {
		log.Error(err)
		return nil, fmt.Errorf("ListVolumes failed. Error: %v", err)
	}

	return nfsVolumes, nil
}

func isVolumeID(volumeID string) bool {

	parts := strings.Split(volumeID, "/")
	if len(parts) == 3 {
		return true
	}

	return false
}

func parseVolumeID(volumeID string) (cluster string, tenant string, bucket string) {
	parts := strings.Split(volumeID, "/")
	if len(parts) == 3 {
		cluster = parts[0]
		tenant = parts[1]
		bucket = parts[2]
		return
	}
	return "", "", ""
}
