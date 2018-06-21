package nexentaedge

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/Nexenta/nexentaedge-csi-driver/csi/nedgeprovider"
	log "github.com/sirupsen/logrus"
	//"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	nedgeConfigFile    = "/config/cluster-config.json"
	K8sNedgeNamespace  = "nedge"
	K8sNedgeMgmtPrefix = "nedge-mgmt"
	K8sNedgeNfsPrefix  = "nedge-svc-nfs-"
	k8sClientInCluster = true
)

func IsConfigFileExists() bool {
	if _, err := os.Stat(nedgeConfigFile); os.IsNotExist(err) {
		return false
	}
	return true
}

func ReadParseConfig() (config NedgeClusterConfig, err error) {

	if !IsConfigFileExists() {
		log.Infof("Config file %s has not been found\n", nedgeConfigFile)
		return config, fmt.Errorf("Config file %s has not been found\n", nedgeConfigFile)
	}

	content, err := ioutil.ReadFile(nedgeConfigFile)
	if err != nil {
		err = fmt.Errorf("error reading config file: %s error: %s\n", nedgeConfigFile, err)
		log.Error(err.Error)
		return config, err
	}

	err = json.Unmarshal(content, &config)
	if err != nil {
		err = fmt.Errorf("error parsing config file: %s error: %s\n", nedgeConfigFile, err)
		log.Error(err.Error)
		return config, err
	}

	return config, nil
}

// Should be deleted in "In-Cluster" build
func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}

/* TODO should be expanded to multiple clusters */
/*
func GetNedgeCluster() (cluster ClusterData, err error) {

	//check config file exists
	if IsConfigFileExists() {
		log.Infof("Config file %s found", nedgeConfigFile)
		config, err := ReadParseConfig()
		if err != nil {
			err = fmt.Errorf("Error reading config file %s error: %s\n", nedgeConfigFile, err)
			return cluster, err
		}

		log.Infof("StandAloneClusterConfig: %+v ", config)
		cluster = ClusterData{isStandAloneCluster: true, clusterConfig: config, nfsServicesData: make([]NfsServiceData, 0)}
	} else {
		isClusterExists, err := DetectNedgeK8sCluster()
		if isClusterExists {
			cluster.isStandAloneCluster = false
		}
	}

	return cluster, err
}
*/

/* Will check k8s nedge cluster existance and will update NedgeClusterConfig information*/
func DetectNedgeK8sCluster(config *NedgeClusterConfig) (clusterExists bool, err error) {
	var kubeconfig string
	var restConfig *rest.Config
	if k8sClientInCluster == true {
		restConfig, err = rest.InClusterConfig()
		if err != nil {
			panic(err.Error())
		}
	} else {
		if home := homeDir(); home != "" {
			kubeconfig = filepath.Join(home, ".kube", "config")
		}
		// use the current context in kubeconfig
		restConfig, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return false, err
		}
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return false, err
	}

	svcs, err := clientset.CoreV1().Services(K8sNedgeNamespace).List(metav1.ListOptions{})
	//log.Infof("SVCS: %+v\n", svcs)
	if err != nil {
		log.Errorf("Error: %v\n", err)
		return false, err
	}

	for _, svc := range svcs.Items {
		//log.Infof("Item: %+v\n", svc)

		serviceName := svc.GetName()
		serviceClusterIP := svc.Spec.ClusterIP

		if strings.HasPrefix(serviceName, K8sNedgeMgmtPrefix) {
			config.Nedgerest = serviceClusterIP
			return true, err
		}
	}
	return false, err
}

func GetNedgeK8sClusterServices() (services []nedgeprovider.NedgeService, err error) {
	var kubeconfig string
	var restConfig *rest.Config
	if k8sClientInCluster == true {
		restConfig, err = rest.InClusterConfig()
		if err != nil {
			panic(err.Error())
		}
	} else {
		if home := homeDir(); home != "" {
			kubeconfig = filepath.Join(home, ".kube", "config")
		}
		// use the current context in kubeconfig
		restConfig, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return services, err
		}
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return services, err
	}

	svcs, err := clientset.CoreV1().Services(K8sNedgeNamespace).List(metav1.ListOptions{})
	//log.Infof("SVCS: %+v\n", svcs)
	if err != nil {
		log.Errorf("Error: %v\n", err)
		return services, err
	}

	for _, svc := range svcs.Items {
		//log.Infof("Item: %+v\n", svc)

		serviceName := svc.GetName()
		serviceClusterIP := svc.Spec.ClusterIP

		if strings.HasPrefix(serviceName, K8sNedgeNfsPrefix) {
			nfsSvcName := strings.TrimPrefix(serviceName, K8sNedgeNfsPrefix)
			serviceNetwork := []string{serviceClusterIP}

			newService := nedgeprovider.NedgeService{Name: nfsSvcName, ServiceType: "nfs", Status: "active", Network: serviceNetwork}
			services = append(services, newService)
		}
	}

	return services, err
}
