
package main

import (
	"fmt"

	"github.com/Nexenta/nexentaedge-csi-driver/csi/nedgeprovider"
	"github.com/Nexenta/nexentaedge-csi-driver/csi/nexentaedge"
)

func main() {

	clusterIP := "10.3.199.201"
	clusterPort := int16(8080)
	user := "admin"
	password := "nexenta"

	var NfsServices []nexentaedge.NedgeK8sService
	nedge := nedgeprovider.InitNexentaEdgeProvider(clusterIP, clusterPort, user, password)
	err := nedge.CheckHealth()
	if err != nil {
		fmt.Printf("Failed to CheckHealth: %s", err)
		return
	}

	services, err := nedge.ListServices()
	if err != nil {
                fmt.Printf("Failed to ListServices: %s", err)
                return
        }

	fmt.Printf("Services: %+v\n", services)
	
	for _, service := range services {
		if service.ServiceType == "nfs" && service.Status == "enabled" {
			/*TODO Fix NedgeK8Service to support multiple service IPs */
			newService := nexentaedge.NedgeK8sService{Name: service.Name, ClusterIP: service.Network[0]}
			NfsServices = append(NfsServices, newService)
		}
	}

	fmt.Printf("Available nfs Services: %+v\n", NfsServices)

}
