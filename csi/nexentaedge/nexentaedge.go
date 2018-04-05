package nexentaedge

import (
	log "github.com/sirupsen/logrus"
)

var configFile = "/etc/nexentaedge-csi.json"

/*InitNexentaEdgeProvider set up variables*/
func InitNexentaEdgeProvider(config string) {
	configFile = config
}

/*CreateVolume remotely creates bucket on nexentaedge service*/
func CreateVolume(name string) (volume string, err error) {
	log.Info("NexentaEdge adapter creates volume: ", name)
	return name, nil
}

/*DeleteVolume remotely deletes bucket on nexentaedge service*/
func DeleteVolume(name string) (err error) {
	log.Info("NexentaEdge adapter deletes volume: ", name)
	return nil
}
