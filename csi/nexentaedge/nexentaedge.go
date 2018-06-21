package nexentaedge

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/Nexenta/nexentaedge-csi-driver/csi/nedgeprovider"
	log "github.com/sirupsen/logrus"
)

const defaultChunkSize int = 1048576

/*INexentaEdge interface to provide base methods */
type INexentaEdge interface {
	//CreateVolume(volumeName string, size int) error
	//DeleteVolume(volumeID string) error
	ListVolumes() ([]nedgeprovider.NedgeNFSVolume, error)
	//IsVolumeExist(volumeID string) bool
	//GetVolume(volumeName string) (volume *nedgeprovider.NedgeNFSVolume, err error)
	//GetVolumeID(volumeName string) (volumeID string, err error)
	GetClusterData(serviceName ...string) (ClusterData, error)
}

type NexentaEdge struct {
	Mutex               *sync.Mutex
	provider            nedgeprovider.INexentaEdgeProvider
	clusterConfig       NedgeClusterConfig
	isStandAloneCluster bool
}

type NedgeClusterConfig struct {
	Name                string
	Address             string
	Port                string
	User                string
	Password            string
	Cluster             string
	ForceBucketDeletion bool            `json:"forceBucketDeletion"`
	ServiceFilter       string          `json:"serviceFilter"`
	ServiceFilterMap    map[string]bool `json:"-"`
}

var NexentaEdgeInstance INexentaEdge

/*InitNexentaEdge reads config and discovers Nedge clusters*/
func InitNexentaEdge() (nedge INexentaEdge, err error) {

	var config NedgeClusterConfig
	var provider nedgeprovider.INexentaEdgeProvider
	isStandAloneCluster := true

	log.Info("InitNexentaEdgeProvider")

	if IsConfigFileExists() {
		log.Infof("Config file %s found", nedgeConfigFile)
		config, err = ReadParseConfig()
		if err != nil {
			err = fmt.Errorf("Error reading config file %s error: %s\n", nedgeConfigFile, err)
			return nil, err
		}

		log.Infof("ClusterConfig: %+v ", config)
	}

	if config.ServiceFilter != "" {
		services := strings.Split(config.ServiceFilter, ",")
		for _, srvName := range services {
			config.ServiceFilterMap[strings.TrimSpace(srvName)] = true
		}
	}

	// No address information for k8s Nedge cluster
	if config.Address == "" {
		isClusterExists, err := DetectNedgeK8sCluster(&config)
		if isClusterExists && err != nil {
			isStandAloneCluster = false
		}
	}

	//default port
	clusterPort := int16(8080)
	i, err := strconv.ParseInt(config.Port, 10, 16)
	if err == nil {
		clusterPort = int16(i)
	}

	provider = nedgeprovider.InitNexentaEdgeProvider(config.Address, clusterPort, config.User, config.Password)
	err = provider.CheckHealth()
	if err != nil {
		log.Infof("InitNexentaEdge failed during CheckHealth : %+v\n", err)
		return nil, err
	}
	log.Infof("Check healtz for %s is OK!", config.Address)

	NexentaEdgeInstance = &NexentaEdge{
		Mutex:               &sync.Mutex{},
		provider:            provider,
		clusterConfig:       config,
		isStandAloneCluster: isStandAloneCluster,
	}

	return NexentaEdgeInstance, nil

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

}

/*GetDataIP returns nfs endpoint IP to create share, for Nedge K8S cluster only */
/*
func (nedge *NexentaEdge) GetDataIP(serviceName string) (dataIP string, err error) {

	services := nedge.k8sCluster.NfsServices
	if nedge.k8sCluster.isStandAloneCluster == true {
		services, err = nedge.provider.ListServices()
		log.Infof("GetDataIP StandAloneCluster ServiceList: %+v\n", services)
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
*/

/*IsVolumeExist check volume existance, */
/*
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
*/

/*GetVolume returns NedgeNFSVolume if it exists, otherwise return nil*/
/*
func (nedge *NexentaEdge) GetVolume(volumeID string) (volume *nedgeprovider.NedgeNFSVolume, err error) {
	// get first service from list, should be changed later

	volID, err := nedgeprovider.ParseVolumeID(volumeID)
	if err != nil {
		return nil, err
	}

	// cluster services detected by K8S API not by Nedge Service list
	if nedge.isStandAloneCluster == false {
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
*/

/*CreateVolume remotely creates bucket on nexentaedge service*/
/*
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
*/
/*DeleteVolume remotely deletes bucket on nexentaedge service*/
/*
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
*/
func (nedge *NexentaEdge) GetK8sNedgeService(serviceName string) (resultService nedgeprovider.NedgeService, err error) {
	services, err := GetNedgeK8sClusterServices()
	if err != nil {
		return resultService, err
	}

	for _, service := range services {
		if service.Name == serviceName {
			return service, err
		}
	}

	return resultService, fmt.Errorf("No service %s found", serviceName)
}

func (nedge *NexentaEdge) ListServices(serviceName ...string) (resultServices []nedgeprovider.NedgeService, err error) {

	var service nedgeprovider.NedgeService
	var services []nedgeprovider.NedgeService
	if nedge.isStandAloneCluster == true {
		if len(serviceName) > 0 {
			service, err = nedge.provider.GetService(serviceName[0])
			services = append(services, service)
		} else {
			services, err = nedge.provider.ListServices()
		}
	} else {
		if len(serviceName) > 0 {
			service, err = nedge.GetK8sNedgeService(serviceName[0])
			services = append(services, service)
		} else {
			services, err = GetNedgeK8sClusterServices()
		}
	}

	if err != nil {
		return resultServices, err
	}

	for _, service := range services {

		//if ServiceFilter not empty, skip every service not presented in list(map)
		if len(nedge.clusterConfig.ServiceFilter) > 0 {
			if _, ok := nedge.clusterConfig.ServiceFilterMap[service.Name]; !ok {
				continue
			}
		}

		if service.ServiceType == "nfs" && service.Status == "enabled" && len(service.Network) > 0 {
			resultServices = append(resultServices, service)

		}
	}
	return resultServices, err
}

/*ListVolumes list all available volumes */
func (nedge *NexentaEdge) ListVolumes() (volumes []nedgeprovider.NedgeNFSVolume, err error) {
	log.Info("NexentaEdgeProvider ListVolumes: ")

	//already filtered services with serviceFilter, service type e.t.c.
	services, err := nedge.ListServices()
	if err != nil {
		return nil, err
	}

	for _, service := range services {

		nfsVolumes, err := nedge.provider.ListNFSVolumes(service.Name)
		if err == nil {
			volumes = append(volumes, nfsVolumes...)
		}
	}

	return volumes, nil
}

/*GetClusterData if serviceName specified we will get data from the one service only */
func (nedge *NexentaEdge) GetClusterData(serviceName ...string) (ClusterData, error) {

	clusterData := ClusterData{nfsServicesData: []NfsServiceData{}}
	var err error

	var services []nedgeprovider.NedgeService
	if len(serviceName) > 0 {
		services, err = nedge.ListServices(serviceName[0])
	} else {
		services, err = nedge.ListServices()
	}

	/*
	   services := []nedgeprovider.NedgeService{}
	   if len(serviceName) > 0 {
	       service, retError := nedge.provider.GetService(serviceName[0])
	       if retError != nil {
	           log.Error("Failed to retrieve service by name ", serviceName[0])
	           return clusterData, err
	       }
	       services = append(services, service)
	   } else {
	       services, err = nedge.ListServices()
	   }
	*/

	if err != nil {
		log.Panic("Failed to retrieve service list", err)
		return clusterData, err
	}

	for _, service := range services {

		nfsVolumes, err := nedge.provider.ListNFSVolumes(service.Name)
		if err == nil {

			nfsServiceData := NfsServiceData{Service: service, NfsVolumes: nfsVolumes}

			clusterData.nfsServicesData = append(clusterData.nfsServicesData, nfsServiceData)
		}
	}

	return clusterData, nil
}
