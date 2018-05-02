package nexentaedge

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

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

type NedgeK8sService struct {
	Name      string
	ClusterIP string
}

type NedgeK8sCluster struct {
	Cluster             NedgeClusterConfig
	isStandAloneCluster bool
	NfsServices         []NedgeK8sService
}

type NedgeClusterConfig struct {
	Name     string
	Address  string
	Port     string
	User     string
	Password string
}

func (cluster *NedgeK8sCluster) IsStandAloneCluster() bool {
	return cluster.isStandAloneCluster
}

func IsConfigFileExists() bool {
	if _, err := os.Stat(nedgeConfigFile); os.IsNotExist(err) {
		return false
	}
	return true
}

func ReadParseConfig() (config NedgeClusterConfig, err error) {
	content, err := ioutil.ReadFile(nedgeConfigFile)
	if err != nil {
		err = fmt.Errorf("Error reading config file: %s error: %s\n", nedgeConfigFile, err)
		log.Error(err.Error)
		return config, err
	}

	err = json.Unmarshal(content, &config)
	if err != nil {
		err = fmt.Errorf("Error parsing config file: %s error: %s\n", nedgeConfigFile, err)
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
func GetNedgeCluster() (cluster NedgeK8sCluster, err error) {

	//check config file exists
	if IsConfigFileExists() {
		log.Infof("Config file %s found", nedgeConfigFile)
		config, err := ReadParseConfig()
		if err != nil {
			err = fmt.Errorf("Error reading config file %s error: \n", nedgeConfigFile, err.Error)
			return cluster, err
		}

		log.Infof("StandAloneClusterConfig: %+v ", config)
		cluster = NedgeK8sCluster{Cluster: config, NfsServices: make([]NedgeK8sService, 0)}
		cluster.isStandAloneCluster = true

	} else {
		cluster, err = DetectNedgeK8sCluster()
		cluster.isStandAloneCluster = false
	}

	return cluster, err
}

func DetectNedgeK8sCluster() (cluster NedgeK8sCluster, err error) {
	var kubeconfig string
	var config *rest.Config
	if k8sClientInCluster == true {
		config, err = rest.InClusterConfig()
		if err != nil {
			panic(err.Error())
		}
	} else {
		if home := homeDir(); home != "" {
			kubeconfig = filepath.Join(home, ".kube", "config")
		}
		// use the current context in kubeconfig
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return cluster, err
		}
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return cluster, err
	}

	svcs, err := clientset.CoreV1().Services(K8sNedgeNamespace).List(metav1.ListOptions{})
	//log.Infof("SVCS: %+v\n", svcs)
	if err != nil {
		log.Errorf("Error: %v\n", err)
		return cluster, err
	}

	for _, svc := range svcs.Items {
		//log.Infof("Item: %+v\n", svc)

		serviceName := svc.GetName()
		serviceClusterIP := svc.Spec.ClusterIP

		if strings.HasPrefix(serviceName, K8sNedgeMgmtPrefix) {
			cluster.Cluster.Name = serviceName
			cluster.Cluster.Address = serviceClusterIP
			cluster.Cluster.Port = "8080"        // should be discoverable
			cluster.Cluster.Name = "admin"       // should be discoverable
			cluster.Cluster.Password = "nexenta" // should be discoverable
			continue
		}

		if strings.HasPrefix(serviceName, K8sNedgeNfsPrefix) {
			nfsSvcName := strings.TrimPrefix(serviceName, K8sNedgeNfsPrefix)
			cluster.NfsServices = append(cluster.NfsServices, NedgeK8sService{Name: nfsSvcName, ClusterIP: serviceClusterIP})
		}
	}

	return cluster, err
}
