package nexentaedge

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Nexenta/nexentaedge-csi-driver/csi/nedgeprovider"
	log "github.com/sirupsen/logrus"
)

const (
	defaultChunkSize       int    = 1048576
	xorKey                 string = "#o$3dfMJd@#4_;sdf789G%$789Slpo(Zv~"
	defaultUserName        string = "admin"
	defaultPassword        string = "TQpcVgoSLA=="
	defaultNFSMountOptions string = "vers=3,tcp"
)

/*INexentaEdge interface to provide base methods */
type INexentaEdge interface {
	CreateVolume(volumeName string, size int, options map[string]string) (string, error)
	DeleteVolume(volumeID string) error
	ListVolumes() ([]nedgeprovider.NedgeNFSVolume, error)
	CheckNfsServiceExists(serviceName string) error
	IsClusterExists(clusterName string) bool
	IsTenantExists(clusterName string, tenantName string) bool
	GetClusterDataByVolumeID(volumeID string) (nedgeprovider.VolumeID, ClusterData, error)
	GetClusterConfig() (config *NedgeClusterConfig)
}

type NexentaEdge struct {
	provider            nedgeprovider.INexentaEdgeProvider
	clusterConfig       NedgeClusterConfig
	isStandAloneCluster bool
}

type NedgeClusterConfig struct {
	Name                      string
	Nedgerest                 string
	Nedgeport                 string
	Username                  string
	Password                  string
	Cluster                   string
	Tenant                    string
	NfsMountOptions           string `json:"nfsMountOptions"`
	ForceBucketDeletion       bool   `json:"forceBucketDeletion"`
	ServiceFilter             string `json:"serviceFilter"`
	NfsServiceSelectionPolicy string `json:"nfsServiceSelectionPolicy"`
}

/* GetMountOptions */
func (config *NedgeClusterConfig) GetMountOptions() (options []string) {

	mountOptionsParts := strings.Split(config.NfsMountOptions, ",")
	for _, option := range mountOptionsParts {
		options = append(options, strings.TrimSpace(option))
	}
	return options
}

func (config *NedgeClusterConfig) GetServiceFilterMap() (filterMap map[string]bool) {

	if config.ServiceFilter != "" {
		filterMap = make(map[string]bool)
		services := strings.Split(config.ServiceFilter, ",")
		for _, srvName := range services {
			filterMap[strings.TrimSpace(srvName)] = true
		}
	}

	return filterMap
}

/* Method to XOR input password string */
func EncryptDecrypt(input string) (output string) {
	key := xorKey
	for i := 0; i < len(input); i++ {
		output += string(input[i] ^ key[i%len(key)])
	}

	return output
}

func elapsed(what string) func() {
	start := time.Now()
	return func() {
		log.Debugf("::Measurement NexentaEdge::%s took %v", what, time.Since(start))
	}
}

/*InitNexentaEdge reads config and discovers Nedge clusters*/
func InitNexentaEdge(invoker string) (nedge INexentaEdge, err error) {
	defer elapsed(invoker + ".InitNexentaEdge")()
	var config NedgeClusterConfig
	var provider nedgeprovider.INexentaEdgeProvider
	isStandAloneCluster := true

	config, err = ReadParseConfig()
	if err != nil {
		err = fmt.Errorf("failed to read config file %s Error: %s", nedgeConfigFile, err)
		log.Infof("%+v", err)
		return nil, err
	}

	/* Apply default values here */
	if len(config.Username) == 0 {
		config.Username = defaultUserName
	}

	if len(config.Password) == 0 {
		config.Username = defaultPassword
	}

	//set default NfsMountOptions values
	if len(config.NfsMountOptions) == 0 {
		config.NfsMountOptions = defaultNFSMountOptions
	}

	// No address information for k8s Nedge cluster
	if config.Nedgerest == "" {
		isClusterExists, _ := DetectNedgeK8sCluster(&config)

		if isClusterExists {
			isStandAloneCluster = false
		} else {
			return nil, fmt.Errorf("No NexentaEdge Cluster has been found")
		}
	}

	//default port
	clusterPort := int16(8080)
	i, err := strconv.ParseInt(config.Nedgeport, 10, 16)
	if err == nil {
		clusterPort = int16(i)
	}

	/* Decode from BASE64 nexentaEdge REST password */
	passwordData, err := base64.StdEncoding.DecodeString(config.Password)
	if err != nil {
		err = fmt.Errorf("failed to decode password. error %+v", err)
		log.Error(err)
		return nil, err
	}

	// XOR password data to plain REST password */
	configPassword := EncryptDecrypt(string(passwordData[:]))

	provider = nedgeprovider.InitNexentaEdgeProvider(config.Nedgerest, clusterPort, config.Username, configPassword)
	err = provider.CheckHealth()
	if err != nil {
		log.Error("InitNexentaEdge failed during CheckHealth : %+v", err)
		return nil, err
	}
	log.Debugf("Check healtz for %s is OK!", config.Nedgerest)

	NexentaEdgeInstance := &NexentaEdge{
		provider:            provider,
		clusterConfig:       config,
		isStandAloneCluster: isStandAloneCluster,
	}

	return NexentaEdgeInstance, nil
}

func (nedge *NexentaEdge) GetClusterConfig() (config *NedgeClusterConfig) {
	return &nedge.clusterConfig
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
	defer elapsed("CreateVolume")()
	// get first service from list, should be changed later

	configMap := nedge.PrepareConfigMap()
	volID, missedPathParts, err := nedgeprovider.ParseVolumeID(name, configMap)

	// throws error when can't substitute volume fill path, no service isn't error
	if err != nil && !IsNoServiceSpecified(missedPathParts) {
		log.Errorf("ParseVolumeID error : %+v", err)
		return "", err
	}

	// get all services information to find already existing volume by path
	clusterData, err := nedge.GetClusterData()
	if err != nil {
		log.Errorf("Couldn't get ClusterData : %+v", err)
		return "", err
	}

	//try to find already existing service with specified volumeID
	serviceData, _ := clusterData.FindServiceDataByVolumeID(volID)
	if serviceData != nil {
		log.Warningf("Volume %s already exists via %s service", volID.FullObjectPath(), serviceData.Service.Name)
		// returns no error because volume already exists
		return volID.FullObjectPath(), nil
	}

	// When service name is missed in path notation, we should select appropriate service for new volume
	if IsNoServiceSpecified(missedPathParts) {

		// find apropriate service to serve
		appropriateServiceData, err := clusterData.FindApropriateServiceData(nedge.GetClusterConfig().NfsServiceSelectionPolicy)

		if err != nil {
			log.Errorf("Appropriate service selection failed : %+v", err)
			return "", err
		}

		// assign appropriate service name to VolumeID
		volID.Service = appropriateServiceData.Service.Name
	}

	log.Infof("NexentaEdge::CreateVolume Appropriate VolumeID : %+v", volID)
	serviceData, err = clusterData.FindNfsServiceData(volID.Service)
	//err = nedge.CheckNfsServiceExists(volID.Service)
	if serviceData == nil {
		log.Error(err.Error)
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

	if !nedge.provider.IsBucketExist(volID.Cluster, volID.Tenant, volID.Bucket) {
		log.Debugf("NexentaEdge::CreateVolume Bucket %s/%s/%s doesnt exist. Creating one", volID.Cluster, volID.Tenant, volID.Bucket)
		err := nedge.provider.CreateBucket(volID.Cluster, volID.Tenant, volID.Bucket, 0, options)
		if err != nil {
			log.Error(err)
			return "", err
		}
		log.Debugf("NexentaEdge::CreateVolume Bucket %s/%s/%s created", volID.Cluster, volID.Tenant, volID.Bucket)
	} else {
		log.Debugf("NexentaEdge::CreateVolume Bucket %s/%s/%s already exists", volID.Cluster, volID.Tenant, volID.Bucket)
	}

	// setup service configuration if asked
	if options["acl"] != "" {
		err := nedge.provider.SetServiceAclConfiguration(volID.Service, volID.Tenant, volID.Bucket, options["acl"])
		if err != nil {
			log.Error(err)
		}
	}

	err = nedge.provider.ServeBucket(volID.Service, volID.Cluster, volID.Tenant, volID.Bucket)
	if err != nil {
		log.Error(err)
		return "", err
	}
	log.Infof("NexentaEdge::CreateVolume Bucket %s/%s/%s served to service %s", volID.Cluster, volID.Tenant, volID.Bucket, volID.Service)

	return volID.FullObjectPath(), nil
}

/*DeleteVolume remotely deletes bucket on nexentaedge service*/
func (nedge *NexentaEdge) DeleteVolume(volumeID string) (err error) {
	defer elapsed("DeleteVolume")()
	log.Debugf("NexentaEdgeProvider::DeleteVolume  VolumeID: %s", volumeID)

	var clusterData ClusterData
	configMap := nedge.PrepareConfigMap()
	volID, missedPathParts, err := nedgeprovider.ParseVolumeID(volumeID, configMap)
	if err != nil {
		// Only service missed in path notation, we should select appropriate service for new volume
		if IsNoServiceSpecified(missedPathParts) {
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

	if err != nil {
		log.Warnf("Can't find service by volumeID %+v", volID)
		// returns nil, because there is no service with such volume
		return nil
	}

	// find nfs volume in service information
	nfsVolume, err := serviceData.FindNFSVolumeByVolumeID(volID)
	if err != nil {
		log.Warnf("Can't find served volume by volumeID %+v, Error: %s", volID, err)
		// returns nil, because volume already unserved
		return nil
	}
	log.Infof("NexentaEdge::DeleteVolume by VolumeID: %+v", nfsVolume.VolumeID)

	// before unserve bucket we need to unset ACL property
	nedge.provider.SetServiceAclConfiguration(nfsVolume.VolumeID.Service, nfsVolume.VolumeID.Tenant, nfsVolume.VolumeID.Bucket, "")

	nedge.provider.UnserveBucket(nfsVolume.VolumeID.Service, nfsVolume.VolumeID.Cluster, nfsVolume.VolumeID.Tenant, nfsVolume.VolumeID.Bucket)

	if nedge.provider.IsBucketExist(nfsVolume.VolumeID.Cluster, nfsVolume.VolumeID.Tenant, nfsVolume.VolumeID.Bucket) {
		nedge.provider.DeleteBucket(nfsVolume.VolumeID.Cluster, nfsVolume.VolumeID.Tenant, nfsVolume.VolumeID.Bucket, nedge.clusterConfig.ForceBucketDeletion)
	}

	return nil
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
	defer elapsed("ListServices")()
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
		//log.Infof("List k8s services for NExentaEdge")
		if len(serviceName) > 0 {
			service, err = nedge.GetK8sNedgeService(serviceName[0])
			services = append(services, service)
		} else {
			services, err = GetNedgeK8sClusterServices()
		}
		//log.Infof("Service list %+v", services)
	}

	if err != nil {
		return resultServices, err
	}

	for _, service := range services {

		//if ServiceFilter not empty, skip every service not presented in list(map)
		serviceFilterMap := nedge.clusterConfig.GetServiceFilterMap()
		if len(serviceFilterMap) > 0 {
			if _, ok := serviceFilterMap[service.Name]; !ok {
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
	defer elapsed("NexentaEdge::ListVolumes")
	log.Debug("NexentaEdgeProvider::ListVolumes")

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
	//log.Infof("GetClusterDataByVolumeID: %s", volumeID)
	configMap := nedge.PrepareConfigMap()
	volID, missedPathParts, err := nedgeprovider.ParseVolumeID(volumeID, configMap)
	if err != nil {
		// Only service missed in path notation, we should select appropriate service for new volume
		if IsNoServiceSpecified(missedPathParts) {
			// get all services information to find service by path
			clusterData, err = nedge.GetClusterData()
			if err != nil {
				return volID, clusterData, err
			}
		}
	} else {
		//log.Infof("GetClusterDataByVolumeID.GetClusterData: by service: %s", volID.Service)
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
		log.Warningf("No services in service list. %v", err)
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
			log.Errorf("No service %s found in NexentaEdge cluster", serviceName[0])
			return clusterData, fmt.Errorf("No service %s found in NexentaEdge cluster", serviceName[0])
		}
	}

	for _, service := range services {

		nfsVolumes, err := nedge.provider.ListNFSVolumes(service.Name)
		if err == nil {
			nfsServiceData := NfsServiceData{Service: service, NfsVolumes: nfsVolumes}
			clusterData.nfsServicesData = append(clusterData.nfsServicesData, nfsServiceData)
		} else {
			log.Warningf("No nfs exports found for %s service. Error: %+v", service.Name, err)
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
