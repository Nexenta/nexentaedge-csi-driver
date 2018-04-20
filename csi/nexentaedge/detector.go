package nexentaedge

import (
        "flag"
        "os"
        "path/filepath"
        "strings"

//      "k8s.io/apimachinery/pkg/api/errors"
        metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
        "k8s.io/client-go/kubernetes"
        "k8s.io/client-go/tools/clientcmd"
)

const (
	K8S_NEDGE_NAMESPACE = "nedge"
	K8S_NEDGE_NFS_PREFIX = "nedge-svc-nfs-"
)

type NedgeK8sCluster struct {
	name string
	clusterMgmtIP string
	nfsServices map[string]string //map service name (w/o prefix):clusterIP - mount point
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
        if err != nil {
                return cluster, err
        }

        for _, svc := range svcs.Items {
                serviceName := svc.GetName()
                serviceClusterIP := svc.Spec.ClusterIP
                if serviceName == K8S_NEDGE_NAMESPACE {
                        cluster.name = serviceName
			cluster.clusterMgmtIP = serviceClusterIP
                        continue
                }

                if strings.HasPrefix(K8S_NEDGE_NFS_PREFIX, serviceName) {
                        nfsSvcName := strings.TrimPrefix(K8S_NEDGE_NFS_PREFIX, serviceName)
			cluster.nfsServices[nfsSvcName] = serviceClusterIP
                }
	        //        fmt.Printf("Service: %s ClusterIP: %s \n", svc.GetName(), svc.Spec.ClusterIP)
        }


	return cluster, err

}

