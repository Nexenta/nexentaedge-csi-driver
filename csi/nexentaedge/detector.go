package nexentaedge

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	//"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	K8sNedgeNamespace  = "nedge"
	K8sNedgeMgmtPrefix = "nedge-mgmt"
	K8sNedgeNfsPrefix  = "nedge-svc-nfs-"
	k8sClientInCluster = false
)

type NedgeK8sService struct {
	Name      string
	ClusterIP string
}

type NedgeK8sCluster struct {
	NedgeMgmtSvc NedgeK8sService
	NfsServices  []NedgeK8sService
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

		flag.Parse()

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
	//fmt.Printf("SVCS: %+v\n", svcs)
	if err != nil {
		fmt.Errorf("Error: %v\n", err)
		return cluster, err
	}

	//cluster.NfsServices = make(map[string]string)
	for _, svc := range svcs.Items {
		//fmt.Printf("Item: %+v\n", svc)
		serviceName := svc.GetName()
		serviceClusterIP := svc.Spec.ClusterIP

		if strings.HasPrefix(serviceName, K8sNedgeMgmtPrefix) {
			cluster.NedgeMgmtSvc.Name = serviceName
			cluster.NedgeMgmtSvc.ClusterIP = serviceClusterIP
			continue
		}

		if strings.HasPrefix(serviceName, K8sNedgeNfsPrefix) {
			nfsSvcName := strings.TrimPrefix(serviceName, K8sNedgeNfsPrefix)
			cluster.NfsServices = append(cluster.NfsServices, NedgeK8sService{Name: nfsSvcName, ClusterIP: serviceClusterIP})
		}
	}

	return cluster, err

}
