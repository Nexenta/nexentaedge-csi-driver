package nexentaedge

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/Nexenta/nexentaedge-csi-driver/csi/nedgeprovider"
)

type NfsServiceData struct {
	Service    nedgeprovider.NedgeService
	NfsVolumes []nedgeprovider.NedgeNFSVolume
}

func (nfsServiceData *NfsServiceData) FindNFSVolumeByVolumeID(volumeID nedgeprovider.VolumeID) (resultNfsVolume nedgeprovider.NedgeNFSVolume, err error) {

	for _, nfsVolume := range nfsServiceData.NfsVolumes {
		if nfsVolume.VolumeID.FullObjectPath() == volumeID.FullObjectPath() {
			return nfsVolume, nil
		}
	}
	return resultNfsVolume, fmt.Errorf("Can't find NFS volume by VolumeID : %+v", volumeID)
}

func (nfsServiceData *NfsServiceData) GetNFSVolumeAndEndpoint(volumeID nedgeprovider.VolumeID) (nfsVolume nedgeprovider.NedgeNFSVolume, endpoint string, err error) {
	nfsVolume, err = nfsServiceData.FindNFSVolumeByVolumeID(volumeID)
	if err != nil {
		return nfsVolume, "", err
	}

	return nfsVolume, fmt.Sprintf("%s:%s", nfsServiceData.Service.Network[0], nfsVolume.Share), err
}

/*ClusterData represents all available(enabled, has networks e.t.c) services and its NFS volumes on cluster
or among the listed in serviceFilter (if serviceFilter option specified)
*/
type ClusterData struct {
	nfsServicesData []NfsServiceData
}

/* template method to implement different Nfs service balancing types */
type nfsServiceSelectorFunc func(clusterData *ClusterData) (*NfsServiceData, error)

/* selects service with minimal export count from whole cluster (if serviceFilter ommited) or from serviceFilter's services */
func minimalExportsServiceSelector(clusterData *ClusterData) (*NfsServiceData, error) {
	if len(clusterData.nfsServicesData) > 0 {
		minService := &clusterData.nfsServicesData[0]

		for _, data := range clusterData.nfsServicesData[1:] {
			currentValue := len(data.NfsVolumes)
			if len(minService.NfsVolumes) > currentValue {
				minService = &data
			}
		}

		return minService, nil
	}

	return nil, fmt.Errorf("No NFS Services available along nedge cluster")
}

/* selects random service from whole cluster (if serviceFilter ommited) or from serviceFilter's services */
func randomServiceSelector(clusterData *ClusterData) (*NfsServiceData, error) {
	if len(clusterData.nfsServicesData) > 0 {
		rand.Seed(time.Now().UnixNano())
		randomIndex := rand.Intn(len(clusterData.nfsServicesData) - 1)
		return &clusterData.nfsServicesData[randomIndex], nil
	}

	return nil, fmt.Errorf("No NFS Services available along nedge cluster")
}

func processServiceSelectionPolicy(serviceSelector nfsServiceSelectorFunc, clusterData *ClusterData) (*NfsServiceData, error) {
	return serviceSelector(clusterData)
}

/*FindApropriateService find service with minimal export count*/
func (clusterData *ClusterData) FindApropriateServiceData(nfsBalancingPolicy string) (*NfsServiceData, error) {
	var serviceSelector nfsServiceSelectorFunc
	switch nfsBalancingPolicy {
	case "minimalServiceSelector":
		serviceSelector = minimalExportsServiceSelector
	case "randomServiceSelector":
		serviceSelector = randomServiceSelector
	default:
		serviceSelector = minimalExportsServiceSelector
	}

	return processServiceSelectionPolicy(serviceSelector, clusterData)
}

func (clusterData *ClusterData) FindServiceDataByVolumeID(volumeID nedgeprovider.VolumeID) (result *NfsServiceData, err error) {
	//log.Debug("FindServiceDataByVolumeID ")

	for _, data := range clusterData.nfsServicesData {
		for _, nfsVolume := range data.NfsVolumes {
			if nfsVolume.Path == volumeID.FullObjectPath() {
				return &data, nil
			}
		}
	}

	return nil, fmt.Errorf("Can't find NFS service data by VolumeID %s", volumeID)
}

/*FillNfsVolumes Fills outer volumes hashmap, format {VolumeID: volume nfs endpoint} */
func (clusterData *ClusterData) FillNfsVolumes(vmap map[string]string, defaultCluster string) {

	for _, data := range clusterData.nfsServicesData {
		for _, nfsVolume := range data.NfsVolumes {

			var volumePath string
			if defaultCluster != "" && nfsVolume.VolumeID.Cluster == defaultCluster {
				volumePath = nfsVolume.VolumeID.MinimalObjectPath()
			} else {
				volumePath = nfsVolume.VolumeID.FullObjectPath()
			}
			vname := volumePath
			vmap[vname] = fmt.Sprintf("%s:%s", data.Service.Network[0], nfsVolume.Share)
		}
	}
}

/* FindNfsServiceData finds and returns pointer to NfsServiceData stored in ClusterData */
func (clusterData *ClusterData) FindNfsServiceData(serviceName string) (serviceData *NfsServiceData, err error) {
	for _, serviceData := range clusterData.nfsServicesData {
		if serviceData.Service.Name == serviceName {
			return &serviceData, nil
		}
	}

	return nil, fmt.Errorf("Can't find Service Data by name %s", serviceName)
}
