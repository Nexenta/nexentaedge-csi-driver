package nexentaedge

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "sync"

    log "github.com/sirupsen/logrus"
)

const defaultChunkSize int = 1048576
const defaultMountPoint string = "/var/lib/ndnfs"

/*INexentaEdge interface to provide base methods */
type INexentaEdge interface {
    CreateVolume(name string, size int) (string, error)
    DeleteVolume(name string) error
    ListVolumes() (map[string]string, error)
    //AttachVolume(instanceID, volumeID string) (string, error)
    //WaitDiskAttached(instanceID string, volumeID string) error
    //DetachVolume(instanceID, volumeID string) error
    //WaitDiskDetached(instanceID string, volumeID string) error
}

type Config struct {
    Name        string
    Nedgerest   string
    Nedgeport   int16
    Nedgedata   string
    Clustername string
    Tenantname  string
    Chunksize   int
    Username    string
    Password    string
    Mountpoint  string
    Servicename string
}

type NexentaEdgeProvider struct {
    Mutex    *sync.Mutex
    Endpoint string
    Config   *Config
}

var NexentaEdgeInstance INexentaEdge = nil
var configFile = "/etc/nexentaedge.json"

/*InitNexentaEdgeProvider set up variables*/
func InitNexentaEdgeProvider(config string) {
    configFile = config
    log.Infof("InitNexentaEdgeProvider configFile: %s", configFile)
}

func ReadParseConfig(fname string) Config {
    content, err := ioutil.ReadFile(fname)
    if err != nil {
        log.Fatal("Error reading config file: ", fname, " error: ", err)
    }
    var conf Config
    err = json.Unmarshal(content, &conf)
    if err != nil {
        log.Fatal("Error parsing config file: ", fname, " error: ", err)
    }
    return conf
}

/*GetNexentaEdgeProvider returns nedge provider instance*/
func GetNexentaEdgeProvider() (INexentaEdge, error) {
    if NexentaEdgeInstance == nil {

        conf := ReadParseConfig(configFile)
        if conf.Chunksize == 0 {
            conf.Chunksize = defaultChunkSize
        }
        if conf.Mountpoint == "" {
            conf.Mountpoint = defaultMountPoint
        }

        log.Info("GetNexentaedgeProvider config: ", conf)
        NexentaEdgeInstance = &NexentaEdgeProvider{
            Mutex:    &sync.Mutex{},
            Endpoint: fmt.Sprintf("http://%s:%d/", conf.Nedgerest, conf.Nedgeport),
            Config:   &conf,
        }
    }

    return NexentaEdgeInstance, nil
}

/*CreateVolume remotely creates bucket on nexentaedge service*/
func (nedge *NexentaEdgeProvider) CreateVolume(name string, size int) (volume string, err error) {
    log.Info("NexentaEdgeProvider:CreateVolume name : ", name, " size:", size)
    return name, nil
}

/*DeleteVolume remotely deletes bucket on nexentaedge service*/
func (nedge *NexentaEdgeProvider) DeleteVolume(name string) (err error) {
    log.Info("NexentaEdgeProvider:DeleteVolume  name: ", name)
    return nil
}

/*ListVolumes list all available volumes*/
func (nedge *NexentaEdgeProvider) ListVolumes() (map[string]string, error) {
    log.Info("NexentaEdgeProvider ListVolumes: ")
    return nil, nil
}
