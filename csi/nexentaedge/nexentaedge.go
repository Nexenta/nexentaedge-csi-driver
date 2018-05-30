package nexentaedge

import (
	"fmt"
	"strconv"
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
	GetVolume(volumeName string) (volume *nedgeprovider.NedgeNFSVolume, err error)
	//GetVolumeID(volumeName string) (volumeID string, err error)
	GetDataIP(serviceName string) (string, error)
}

type NexentaEdge struct {
	Mutex      *sync.Mutex
	provider   nedgeprovider.INexentaEdgeProvider
	k8sCluster NedgeK8sCluster
}

var NexentaEdgeInstance INexentaEdge

/*InitNexentaEdge discover nedge k8s cluster */
func InitNexentaEdge() (nedge INexentaEdge, err error) {

	var k8sCluster NedgeK8sCluster
	var provider nedgeprovider.INexentaEdgeProvider
	log.Info("InitNexentaEdgeProvider")

	k8sCluster, err = GetNedgeCluster()
	log.Info("k8s Cluster: %+v", k8sCluster)
	if err != nil {
		msg := fmt.Sprintf("Can't get NexentaEdge k8s cluster instance, Error: %s", err)
		log.Error(msg)
		return nil, fmt.Errorf("%s", msg)
	}

	if k8sCluster.Cluster.Name == "" || k8sCluster.Cluster.Address == "" {
		msg := fmt.Sprintf("Can't find k8s nedge cluster information, Error: %s", err)
		log.Error(msg)
		return nil, fmt.Errorf("%s", msg)
	}

	clusterPort := int16(8080)
	i, err := strconv.ParseInt(k8sCluster.Cluster.Port, 10, 16)
	if err == nil {
		clusterPort = int16(i)
	}

	provider = nedgeprovider.InitNexentaEdgeProvider(k8sCluster.Cluster.Address, clusterPort, k8sCluster.Cluster.User, k8sCluster.Cluster.Password)
	err = provider.CheckHealth()
	if err != nil {
		log.Infof("InitNexentaEdge failed during CheckHealth : %+v\n", err)
		return nil, err
	}
	log.Infof("Check healtz for %s is OK!", k8sCluster.Cluster.Address)

	// if it StandAlone NedgeCluster we need to get Services list via API
	/*
		if k8sCluster.IsStandAloneCluster() == false {

			services, err := provider.ListServices()
			if err != nil {
				log.Infof("InitNexentaEdge failed during ListServices : %+v\n", err)
				return nil, err
			}

			for _, service := range services {
				if service.ServiceType == "nfs" && service.Status == "enabled" {
					/*TODO Fix NedgeK8Service to support multiple service IPs
					newService := NedgeK8sService{Name: service.Name, DataIP: service.Network[0]}
					k8sCluster.NfsServices = append(k8sCluster.NfsServices, newService)
				}
			}
		}

		//check services presence
		if len(k8sCluster.NfsServices) < 1 {
			msg := "Can't find k8s nedge cluster NFS services"
			log.Error(msg)
			return nil, fmt.Errorf("%s", msg)
		}
	*/
	/* TODO change hardcoded parameters Port, login, password */
	NexentaEdgeInstance = &NexentaEdge{
		Mutex:      &sync.Mutex{},
		k8sCluster: k8sCluster,
		provider:   provider,
	}

	return NexentaEdgeInstance, nil
}

/*GetDataIP returns nfs endpoint IP to create share, for Nedge K8S cluster only */
func (nedge *NexentaEdge) GetDataIP(serviceName string) (dataIP string, err error) {

	services := nedge.k8sCluster.NfsServices
	if nedge.k8sCluster.isStandAloneCluster {
		services, err = nedge.provider.ListServices()
		if err != nil {
			return dataIP, err
		}
	}

	for _, service := range services {
		if service.Name == serviceName {
			return service.Network[0], err
		}
	}
	return dataIP, fmt.Errorf("No service %s found ", serviceName)
}

/*IsVolumeExist check volume existance, */
func (nedge *NexentaEdge) IsVolumeExist(volumeID string) bool {

	volID, err := nedgeprovider.ParseVolumeID(volumeID)
	if err != nil {
		return false
	}

	if nedge.provider.IsBucketExist(volID.Cluster, volID.Tenant, volID.Bucket) {
		volume, _ := nedge.GetVolume(volumeID)
		if volume != nil {
			return true
		}
	}
	return false
}

/*GetVolume returns NedgeNFSVolume if it exists, otherwise return nil*/
func (nedge *NexentaEdge) GetVolume(volumeID string) (volume *nedgeprovider.NedgeNFSVolume, err error) {
	// get first service from list, should be changed later

	volID, err := nedgeprovider.ParseVolumeID(volumeID)
	if err != nil {
		return nil, err
	}

	// cluster services detected by K8S API not by Nedge Service list
	if nedge.k8sCluster.isStandAloneCluster == false {
		// check service name in cluster service list
		serviceFound := false
		for _, k8sNedgeNfsService := range nedge.k8sCluster.NfsServices {
			if k8sNedgeNfsService.Name == volID.Service {
				serviceFound = true
				break
			}
		}
		if serviceFound != true {
			return nil, fmt.Errorf("Service %s has not been detected for volumeId %s", volID.Service, volumeID)
		}
	}

	volumes, err := nedge.provider.ListNFSVolumes(volID.Service)
	if err != nil {
		log.Fatal("ListVolumes failed Error: ", err)
		return nil, err
	}

	log.Info("GetVolume:ListVolumes volumes", volumes)
	log.Info("Volume name to find: ", volumeID)

	for _, v := range volumes {

		if volumeID == v.VolumeID {
			return &v, err
		}
	}

	return nil, err
}

/*CreateVolume remotely creates bucket on nexentaedge service*/
func (nedge *NexentaEdge) CreateVolume(name string, size int) (err error) {
	// get first service from list, should be changed later

	volID, err := nedgeprovider.ParseVolumeID(name)
	if err != nil {
		return err
	}

	log.Infof("NexentaEdgeProvider:CreateVolume for serviceName: %s, %s/%s/%s, size: %d", volID.Service, volID.Cluster, volID.Tenant, volID.Bucket, 0)
	err = nedge.provider.CreateBucket(volID.Cluster, volID.Tenant, volID.Bucket, 0, nil)
	if err != nil {
		err = fmt.Errorf("CreateVolume failed on createBucket error: %s", err)
		return err
	}

	err = nedge.provider.ServeBucket(volID.Service, volID.Cluster, volID.Tenant, volID.Bucket)
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
	volID, err := nedgeprovider.ParseVolumeID(volumeID)
	if err != nil {
		return err
	}

	log.Infof("NexentaEdgeProvider:DeleteVolume for serviceName: %s@%s/%s/%s", volID.Service, volID.Cluster, volID.Tenant, volID.Bucket)

	//TODO Add check that service already served
	err = nedge.provider.UnserveBucket(volID.Service, volID.Cluster, volID.Tenant, volID.Bucket)
	if err != nil {
		err = fmt.Errorf("DeleteVolume failed on unserveService, error: %s", err)
		return err
	}

	//TODO Add check that bucket already exist
	err = nedge.provider.DeleteBucket(volID.Cluster, volID.Tenant, volID.Bucket)
	if err != nil {
		err = fmt.Errorf("DeleteVolume failed on deleteBucket, error: %s", err)
		return err
	}

	return err
}

/*ListVolumes list all available volumes */
func (nedge *NexentaEdge) ListVolumes() (volumes []nedgeprovider.NedgeNFSVolume, err error) {
	log.Info("NexentaEdgeProvider ListVolumes: ")

	services := nedge.k8sCluster.NfsServices
	if nedge.k8sCluster.isStandAloneCluster == true {
		services, err = nedge.provider.ListServices()
		if err != nil {
			return volumes, err
		}
	}

	for _, service := range services {
		if service.ServiceType == "nfs" && service.Status == "enabled" && len(service.Network) > 0 {
			nfsVolumes, err := nedge.provider.ListNFSVolumes(service.Name)
			if err == nil {
				volumes = append(volumes, nfsVolumes...)
			}
		}
	}

	return volumes, nil
}
