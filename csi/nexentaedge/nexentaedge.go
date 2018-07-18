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
	CreateVolume(volumeName string, size int, options map[string]string) (string, error)
	DeleteVolume(volumeID string) error
	ListVolumes() ([]nedgeprovider.NedgeNFSVolume, error)
	CheckNfsServiceExists(serviceName string) error
	IsClusterExists(clusterName string) bool
	IsTenantExists(clusterName string, tenantName string) bool
	//IsVolumeExist(volumeID string) bool
	//GetVolume(volumeID string) (volume *nedgeprovider.NedgeNFSVolume, err error)
	//GetVolumeID(volumeName string) (volumeID string, err error)
	GetClusterDataByVolumeID(volumeID string) (nedgeprovider.VolumeID, ClusterData, error)
}

type NexentaEdge struct {
	Mutex               *sync.Mutex
	provider            nedgeprovider.INexentaEdgeProvider
	clusterConfig       NedgeClusterConfig
	isStandAloneCluster bool
}

type NedgeClusterConfig struct {
	Name                string
	Nedgerest           string
	Nedgeport           string
	Username            string
	Password            string
	Cluster             string
	Tenant		    string
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

	config, err = ReadParseConfig()
	if err != nil {
		err = fmt.Errorf("Error reading config file %s \nError: %s\n", nedgeConfigFile, err)
		return nil, err
	}

	config.ServiceFilterMap = make(map[string]bool)
	if config.ServiceFilter != "" {
		services := strings.Split(config.ServiceFilter, ",")
		for _, srvName := range services {
			config.ServiceFilterMap[strings.TrimSpace(srvName)] = true
		}
	}

	// No address information for k8s Nedge cluster
	if config.Nedgerest == "" {
		isClusterExists, _ := DetectNedgeK8sCluster(&config)

		if isClusterExists {
			isStandAloneCluster = false
		} else {
			return nil, fmt.Errorf("No NexentaEdge Cluster has been found\n")
		}
	}

	//default port
	clusterPort := int16(8080)
	i, err := strconv.ParseInt(config.Nedgeport, 10, 16)
	if err == nil {
		clusterPort = int16(i)
	}

	provider = nedgeprovider.InitNexentaEdgeProvider(config.Nedgerest, clusterPort, config.Username, config.Password)
	err = provider.CheckHealth()
	if err != nil {
		log.Infof("InitNexentaEdge failed during CheckHealth : %+v\n", err)
		return nil, err
	}
	log.Infof("Check healtz for %s is OK!", config.Nedgerest)

	NexentaEdgeInstance = &NexentaEdge{
		Mutex:               &sync.Mutex{},
		provider:            provider,
		clusterConfig:       config,
		isStandAloneCluster: isStandAloneCluster,
	}

	return NexentaEdgeInstance, nil
}

func (nedge *NexentaEdge) CheckNfsServiceExists(serviceName string) error {
	nedgeService, err := nedge.provider.GetService(serviceName)
	if err != nil {
		return fmt.Errorf("No NexentaEdge service %s has been found", serviceName)
	}

	if nedgeService.ServiceType != "nfs" {
		return fmt.Errorf("Service %s is not nfs type service", nedgeService.Name)
	}

	// in case of In-Cluster nedge configuration, there is no network configured
	if nedge.isStandAloneCluster && len(nedgeService.Network) < 1 {
		return fmt.Errorf("Service %s isn't configured, no client network assigned", nedgeService.Name)
	}

	if nedgeService.Status != "enabled" {
		return fmt.Errorf("Service %s not enabled, enable service to make it available", nedgeService.Name)
	}

	return nil
}

func (nedge *NexentaEdge) PrepareConfigMap() map[string]string {
	configMap := make(map[string]string)

	if nedge.clusterConfig.Cluster != "" {
		configMap["cluster"] = nedge.clusterConfig.Cluster
	}

	if nedge.clusterConfig.Tenant != "" {
                configMap["tenant"] = nedge.clusterConfig.Tenant
        }

	return configMap
}

// Checks only service name is missing in volume id
func IsNoServiceSpecified(missedParts map[string]bool) bool {
	if len(missedParts) == 1 {
		if _, ok := missedParts["service"]; ok {
			return true
		}
	}
	return false
}

/*CreateVolume creates bucket and serve it via nexentaedge service*/
func (nedge *NexentaEdge) CreateVolume(name string, size int, options map[string]string) (volumeID string, err error) {
	// get first service from list, should be changed later

	configMap := nedge.PrepareConfigMap()
	volID, missedPathParts, err := nedgeprovider.ParseVolumeID(name, configMap)
	if err != nil {

		// Only service missed in path notation, we should select appropriate service for new volume
		if IsNoServiceSpecified(missedPathParts) {
			log.Infof("No service specified!")
			// get all services information to find service by path
			clusterData, err := nedge.GetClusterData()
			if err != nil {
				return "", err
			}

			// find service to serve
			appropriateServiceData, err := clusterData.FindApropriateServiceData()
			log.Infof("Appropriate service is : %+v\n", appropriateServiceData)
			if err != nil {
				log.Infof("Appropriate service selection failed : %s\n", err)
				return "", err
			}

			// assign aprppriate service name to VolumeID
			volID.Service = appropriateServiceData.Service.Name

		} else {
			log.Errorf("ParseVolumeID error : %s\n", err)
			return "", err
		}
	}

	log.Infof("VolumeID : %+v", volID)
	err = nedge.CheckNfsServiceExists(volID.Service)
	if err != nil {
		log.Error(err)
		return "", err
	}

	// check for cluster name existance
	if !nedge.IsClusterExists(volID.Cluster) {
		return "", fmt.Errorf("No cluster name %s found", volID.Cluster)
	}

	// check for tenant name existance
	if !nedge.IsTenantExists(volID.Cluster, volID.Tenant) {
		return "", fmt.Errorf("No cluster/tenant name %s/%s found", volID.Cluster, volID.Tenant)
	}

	log.Info("Creating bucket")
	if !nedge.provider.IsBucketExist(volID.Cluster, volID.Tenant, volID.Bucket) {
		log.Info("Bucket doesnt exist")

		err := nedge.provider.CreateBucket(volID.Cluster, volID.Tenant, volID.Bucket, 0, options)
		if err != nil {
			log.Error(err)
			return "", err
		}
	}

	// setup quota configuration
	if quota, ok := options["size"]; ok {
		err = nedge.provider.SetBucketQuota(volID.Cluster, volID.Tenant, volID.Bucket, quota)
		if err != nil {
			log.Error(err)
			return "", err
		}
	}

	//setup service configuration
	if options["acl"] != "" {
		err := nedge.provider.SetServiceAclConfiguration(volID.Service, volID.Tenant, volID.Bucket, options["acl"])
		if err != nil {
			log.Error(err)
		}
	}

	err = nedge.provider.ServeBucket(volID.Service, volID.Cluster, volID.Tenant, volID.Bucket)
	if err != nil {
		log.Error(err)
	}

	return volID.FullObjectPath(), err
}

/*DeleteVolume remotely deletes bucket on nexentaedge service*/
func (nedge *NexentaEdge) DeleteVolume(volumeID string) (err error) {
	log.Info("NexentaEdgeProvider:DeleteVolume  VolumeID: ", volumeID)

	var clusterData ClusterData
	configMap := nedge.PrepareConfigMap()
	volID, missedPathParts, err := nedgeprovider.ParseVolumeID(volumeID, configMap)
	if err != nil {
		// Only service missed in path notation, we should select appropriate service for new volume
		if IsNoServiceSpecified(missedPathParts) {
			log.Infof("No service specified!")
			// get all services information to find service by path
			clusterData, err = nedge.GetClusterData()
			if err != nil {
				return err
			}
		}
	} else {
		clusterData, err = nedge.GetClusterData(volID.Service)
		if err != nil {
			return err
		}
	}

	// find service to serve
	serviceData, err := clusterData.FindServiceDataByVolumeID(volID)
	log.Infof("Service is : %+v\n", serviceData)
	if err != nil {
		return err
	}

	// find nfs volume in service information
	nfsVolume, err := serviceData.FindNFSVolumeByVolumeID(volID)
	if err != nil {
		return err
	}

	// before unserve bucket we need to unset ACL property
	nedge.provider.SetServiceAclConfiguration(nfsVolume.VolumeID.Service, nfsVolume.VolumeID.Tenant, nfsVolume.VolumeID.Bucket, "")

	nedge.provider.UnserveBucket(nfsVolume.VolumeID.Service, nfsVolume.VolumeID.Cluster, nfsVolume.VolumeID.Tenant, nfsVolume.VolumeID.Bucket)

	if nedge.provider.IsBucketExist(nfsVolume.VolumeID.Cluster, nfsVolume.VolumeID.Tenant, nfsVolume.VolumeID.Bucket) {
		nedge.provider.DeleteBucket(nfsVolume.VolumeID.Cluster, nfsVolume.VolumeID.Tenant, nfsVolume.VolumeID.Bucket, nedge.clusterConfig.ForceBucketDeletion)
	}

	return err
}

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
		//log.Infof("List k8s services for NExentaEdge\n")
		if len(serviceName) > 0 {
			service, err = nedge.GetK8sNedgeService(serviceName[0])
			services = append(services, service)
		} else {
			services, err = GetNedgeK8sClusterServices()
		}
		//log.Infof("Service list %+v\n", services)
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

/* returns ClusterData by raw volumeID string */
func (nedge *NexentaEdge) GetClusterDataByVolumeID(volumeID string) (nedgeprovider.VolumeID, ClusterData, error) {
	var clusterData ClusterData
	log.Infof("GetClusterDataByVolumeID: %s\n", volumeID)
	configMap := nedge.PrepareConfigMap()
	volID, missedPathParts, err := nedgeprovider.ParseVolumeID(volumeID, configMap)
	if err != nil {
		// Only service missed in path notation, we should select appropriate service for new volume
		if IsNoServiceSpecified(missedPathParts) {
			log.Infof("No service specified!")
			// get all services information to find service by path
			clusterData, err = nedge.GetClusterData()
			if err != nil {
				return volID, clusterData, err
			}
		}
	} else {
		log.Infof("GetClusterDataByVolumeID.GetClusterData: by service: %s\n", volID.Service)
		clusterData, err = nedge.GetClusterData(volID.Service)
		if err != nil {
			return volID, clusterData, err
		}
	}

	return volID, clusterData, err
}

/*GetClusterData if serviceName specified we will get data from the one service only */
func (nedge *NexentaEdge) GetClusterData(serviceName ...string) (ClusterData, error) {

	clusterData := ClusterData{nfsServicesData: []NfsServiceData{}}
	var err error

	var services []nedgeprovider.NedgeService

	services, err = nedge.ListServices()
	if err != nil {
                log.Panic("Failed to retrieve service list", err)
                return clusterData, err
	}

	if len(serviceName) > 0 {
		serviceFound := false
		for _, service := range services {
			if service.Name == serviceName[0] {
				services = []nedgeprovider.NedgeService{service}
				serviceFound = true
				break
			}
		}
		if serviceFound != true {
			log.Errorf("No service %s found in NexentaEdge cluster.\n", serviceName[0])
			return clusterData, fmt.Errorf("No service %s found in NexentaEdge cluster.", serviceName[0])
		}
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

func (nedge *NexentaEdge) IsClusterExists(clusterName string) bool {
	clusters, err := nedge.provider.ListClusters()
	if err != nil {
		return false
	}

	for _, cluster := range clusters {
		if cluster == clusterName {
			return true
		}
	}
	return false
}

func (nedge *NexentaEdge) IsTenantExists(clusterName string, tenantName string) bool {
	tenants, err := nedge.provider.ListTenants(clusterName)
	if err != nil {
		return false
	}

	for _, tenant := range tenants {
		if tenant == tenantName {
			return true
		}
	}
	return false
}
