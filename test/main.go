
package main

import (
	"fmt"

//	"github.com/Nexenta/nexentaedge-csi-driver/csi/nedgeprovider"
	"github.com/Nexenta/nexentaedge-csi-driver/csi/nexentaedge"
)

func ListVolumes(nedge nexentaedge.INexentaEdge) {
	volumes, err := nedge.ListVolumes()
        if err != nil {
                fmt.Printf("Failed to ListVolumes: %s\n", err)
                return
        }

        fmt.Printf("Volumes  : %+v\n", volumes)
}

func CreateVolume(nedge nexentaedge.INexentaEdge, volumeID string) {
        volID, err := nedge.CreateVolume(volumeID, 0, make(map[string]string))
        if err != nil {
                fmt.Printf("Failed to CreateVolume: %s\n", err)
                return
        }
	fmt.Printf("Created VolumeID  : %+v\n", volID)
}

func DeleteVolume(nedge nexentaedge.INexentaEdge, volumeID string) {
        err := nedge.DeleteVolume(volumeID)
        if err != nil {
                fmt.Printf("Failed to DeleteVolume: %s\n", err)
                return
        }
}


func main() {

	nedge, err := nexentaedge.InitNexentaEdge("main")
	if err != nil {
                fmt.Printf("Failed to InitNexentaEdge: %s\n", err)
                return
        }


	fmt.Printf("nedge  : %+v\n", nedge)

	ListVolumes(nedge)
	/*
	cluData, err := nedge.GetClusterData()
	if err != nil {
                fmt.Printf("Failed to ListServices: %s", err)
                return
        }
	*/

	volumeID := "ten1/buk1"
	CreateVolume(nedge, volumeID)
	ListVolumes(nedge)
	DeleteVolume(nedge, volumeID)
	ListVolumes(nedge)
}
