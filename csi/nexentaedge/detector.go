package nexentaedge

import (
        "flag"
        "os"
	"fmt"
        "path/filepath"
        "strings"

//      "k8s.io/apimachinery/pkg/api/errors"
        metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
        "k8s.io/client-go/kubernetes"
        "k8s.io/client-go/tools/clientcmd"
)

const (
	K8S_NEDGE_NAMESPACE = "nedge"
	K8S_NEDGE_MGMT_PREFIX = "nedge-mgmt"
	K8S_NEDGE_NFS_PREFIX = "nedge-svc-nfs-"
)

type NedgeK8sCluster struct {
	Name string
	ClusterMgmtIP string
	NfsServices map[string]string //map service name (w/o prefix):clusterIP - mount point
}

func homeDir() string {
        if h := os.Getenv("HOME"); h != "" {
                return h
        }
        return os.Getenv("USERPROFILE") // windows
}

/* TODO should be expanded to multiple clusters */
func GetNedgeCluster() (cluster NedgeK8sCluster, err error) {
	var kubeconfig *string
	//fmt.Println("GetNedgeCluster: ")
        if home := homeDir(); home != "" {
                kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
        }

        flag.Parse()

        // use the current context in kubeconfig

        config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
        if err != nil {
                return cluster, err
        }

        // create the clientset
        clientset, err := kubernetes.NewForConfig(config)
        if err != nil {
                return cluster, err
        }

	svcs, err := clientset.CoreV1().Services(K8S_NEDGE_NAMESPACE).List(metav1.ListOptions{})
	//fmt.Printf("SVCS: %+v\n", svcs)
        if err != nil {
		fmt.Errorf("Error: %v\n", err)
                return cluster, err
        }

	cluster.NfsServices = make(map[string]string)
        for _, svc := range svcs.Items {
		//fmt.Printf("Item: %+v\n", svc)
                serviceName := svc.GetName()
                serviceClusterIP := svc.Spec.ClusterIP
		
                if strings.HasPrefix(serviceName, K8S_NEDGE_MGMT_PREFIX) {
                        cluster.Name = serviceName
			cluster.ClusterMgmtIP = serviceClusterIP
                        continue
                }

                if strings.HasPrefix(serviceName, K8S_NEDGE_NFS_PREFIX) {
                        nfsSvcName := strings.TrimPrefix(serviceName, K8S_NEDGE_NFS_PREFIX)
			cluster.NfsServices[nfsSvcName] = serviceClusterIP
                }
	        //        fmt.Printf("Service: %s ClusterIP: %s \n", svc.GetName(), svc.Spec.ClusterIP)
        }


	return cluster, err

}

