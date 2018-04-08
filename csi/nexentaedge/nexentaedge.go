package nexentaedge

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
)

const defaultChunkSize int = 1048576
const defaultMountPoint string = "/var/lib/ndnfs"

/*INexentaEdge interface to provide base methods */
type INexentaEdge interface {
	CreateVolume(volumeName string, size int) error
	DeleteVolume(volumeID string) error
	ListVolumes() ([]map[string]string, error)
	IsVolumeExist(volumeID string) bool
	GetVolume(volumeName string) (volume map[string]string)
	GetDataIP() string
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
	log.Info("GetNexentaedgeProvider: ")
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

/*GetDataIP returns nfs endpoint IP to create share */
func (nedge *NexentaEdgeProvider) GetDataIP() string {
	return nedge.Config.Nedgedata
}

/*IsVolumeExist check volume existance, */
func (nedge *NexentaEdgeProvider) IsVolumeExist(volumeNameOrID string) bool {
	volume := nedge.GetVolume(volumeNameOrID)
	if volume != nil {
		return true
	}

	return false
}

/*GetVolume returns volume by volume name/ID if it exists, otherwise return nil*/
func (nedge *NexentaEdgeProvider) GetVolume(volumeNameOrID string) (volume map[string]string) {
	volumes, err := nedge.ListVolumes()
	if err != nil {
		log.Fatal("ListVolumes failed Error: ", err)
	}

	log.Info("GetVolumeIDByName:ListVolumes volumes", volumes)
	log.Info("Volume name to find: ", volumeNameOrID)

	propertyToCompare := "bucket"
	if isLikelyVolumeID(volumeNameOrID) {
		propertyToCompare = "volumeID"
	}
	for _, v := range volumes {

		if volumeNameOrID == v[propertyToCompare] {
			return v
		}
	}

	return nil
}

/*CreateVolume remotely creates bucket on nexentaedge service*/
func (nedge *NexentaEdgeProvider) CreateVolume(name string, size int) (err error) {

	serviceName := nedge.Config.Servicename
	clusterName := nedge.Config.Clustername
	tenantName := nedge.Config.Tenantname

	log.Infof("NexentaEdgeProvider:CreateVolume for serviceName: %s, %s/%s/%s, size: %d", serviceName, clusterName, tenantName, name, size)
	body, err := nedge.createBucket(clusterName, tenantName, name)
	if err != nil {
		err = fmt.Errorf("CreateVolume failed on createBucket error: %s", err)
		return err
	}

	body, err = nedge.serveService(serviceName, clusterName, tenantName, name)
	if err != nil {
		err = fmt.Errorf("CreateVolume failed on serveService error: %s", err)
		return err
	}

	// no usage of response body yet
	_ = body

	return err
}

/*DeleteVolume remotely deletes bucket on nexentaedge service*/
func (nedge *NexentaEdgeProvider) DeleteVolume(volumeNameOrID string) (err error) {
	log.Info("NexentaEdgeProvider:DeleteVolume  VolumeID: ", volumeNameOrID)

	serviceName := nedge.Config.Servicename
	clusterName := nedge.Config.Clustername
	tenantName := nedge.Config.Tenantname

	bucket := GetBucketFromVolumeID(volumeNameOrID)
	log.Infof("NexentaEdgeProvider:DeleteVolume for serviceName: %s: %s/%s/%s", serviceName, clusterName, tenantName, bucket)

	body, err := nedge.unserveService(serviceName, clusterName, tenantName, bucket)
	if err != nil {
		err = fmt.Errorf("DeleteVolume failed on unserveService, error: %s", err)
		return err
	}

	body, err = nedge.deleteBucket(clusterName, tenantName, bucket)
	if err != nil {
		err = fmt.Errorf("DeleteVolume failed on deleteBucket, error: %s", err)
		return err
	}

	// no usage of response body yet
	_ = body

	return err
}

func (nedge *NexentaEdgeProvider) doNexentaEdgeRequest(method string, commandPath string, data map[string]interface{}) (body []byte, err error) {

	body, err = nedge.Request(method, commandPath, data)
	resp := make(map[string]interface{})
	jsonerr := json.Unmarshal(body, &resp)

	if len(body) > 0 {
		if jsonerr != nil {
			log.Error("doNexentaEdgeRequest failed", jsonerr)
			return body, fmt.Errorf("doNexentaEdgeRequest failed: %v", jsonerr)
		}

		if (resp["code"] != nil) && (resp["code"] != "RT_ERR_EXISTS") {
			err = fmt.Errorf("Error while handling request: %s", resp)
			log.Errorf("Error: %v", err)
			return body, err
		}
	} else {
		err = fmt.Errorf("Empty response from NexentaEdge server: %s", resp)
		log.Panic(err)
	}

	return body, err
}

func (nedge *NexentaEdgeProvider) createBucket(
	clusterName string,
	tenantName string,
	bucketName string) (response []byte, err error) {

	path := fmt.Sprintf("clusters/%s/tenants/%s/buckets", clusterName, tenantName)

	data := make(map[string]interface{})
	data["bucketName"] = bucketName

	return nedge.doNexentaEdgeRequest("POST", path, data)
}

func (nedge *NexentaEdgeProvider) deleteBucket(
	clusterName string,
	tenantName string,
	bucketName string) (response []byte, err error) {

	path := fmt.Sprintf("clusters/%s/tenants/%s/buckets/%s", clusterName, tenantName, bucketName)

	data := make(map[string]interface{})
	data["bucketName"] = bucketName

	return nedge.doNexentaEdgeRequest("DELETE", path, data)
}

func (nedge *NexentaEdgeProvider) serveService(
	serviceName string,
	clusterName string,
	tenantName string,
	bucketName string) (response []byte, err error) {

	path := fmt.Sprintf("service/%s/serve", serviceName)

	data := make(map[string]interface{})
	data["serve"] = fmt.Sprintf("%s/%s/%s", clusterName, tenantName, bucketName)

	return nedge.doNexentaEdgeRequest("PUT", path, data)
}

func (nedge *NexentaEdgeProvider) unserveService(
	serviceName string,
	clusterName string,
	tenantName string,
	bucketName string) (response []byte, err error) {

	path := fmt.Sprintf("service/%s/serve", serviceName)

	data := make(map[string]interface{})
	data["serve"] = fmt.Sprintf("%s/%s/%s", clusterName, tenantName, bucketName)

	return nedge.doNexentaEdgeRequest("DELETE", path, data)
}

/*ListVolumes list all available volumes*/
func (nedge *NexentaEdgeProvider) ListVolumes() (volumes []map[string]string, err error) {
	log.Info("NexentaEdgeProvider ListVolumes: ")

	serviceObjects, err := nedge.GetServiceObjects()
	if err != nil {
		log.Error(err)
		return nil, fmt.Errorf("ListVolumes failed. Error: %v", err)
	}

	prefix := fmt.Sprintf("%s/%s", nedge.Config.Clustername, nedge.Config.Tenantname)
	for _, v := range serviceObjects {

		var service = strings.Split(v, ",")[1]
		var parts = strings.Split(service, "@")

		//check for equal cluster/tenant objects
		if strings.HasPrefix(parts[1], prefix) {
			objectPathParts := strings.Split(parts[1], "/")
			obj := map[string]string{
				"volumeID": v,
				"share":    parts[0],
				"cluster":  objectPathParts[0],
				"tenant":   objectPathParts[1],
				"bucket":   objectPathParts[2],
			}

			volumes = append(volumes, obj)
		}
	}

	return volumes, nil
}

func (nedge *NexentaEdgeProvider) GetServiceObjects() (objects []string, err error) {

	servicePath := fmt.Sprintf("service/%s", nedge.Config.Servicename)
	body, err := nedge.doNexentaEdgeRequest("GET", servicePath, nil)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	responseMap := make(map[string]map[string]map[string]interface{})
	jsonerr := json.Unmarshal(body, &responseMap)
	if jsonerr != nil {
		log.Error(jsonerr)
		return nil, fmt.Errorf("GetServiceObjects failed. Error: %v", err)
	}

	var exports []string
	strList := responseMap["response"]["data"]["X-Service-Objects"].(string)
	jsonerr = json.Unmarshal([]byte(strList), &exports)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	for _, v := range exports {
		if len(strings.Split(v, ",")) > 1 {
			objects = append(objects, v)
		}
	}
	return objects, err
}

func (nedge *NexentaEdgeProvider) checkError(resp *http.Response) (err error) {
	if resp.StatusCode > 399 {
		body, err := ioutil.ReadAll(resp.Body)
		log.Error(resp.StatusCode, body, err)
		return err
	}
	return err
}

func (nedge *NexentaEdgeProvider) Request(method, path string, data map[string]interface{}) (body []byte, err error) {
	url := nedge.Endpoint + path
	log.Debug("Issuing request to NexentaEdge, endpoint: ",
		url, " data: ", data, " method: ", method)

	if path == "" {
		err = errors.New("Unable to issue requests without specifying path")
		log.Fatal(err.Error())
	}

	datajson, err := json.Marshal(data)
	if err != nil {
		log.Fatal(err)
	}

	tr := &http.Transport{}
	client := &http.Client{Transport: tr}

	req, err := http.NewRequest(method, url, nil)
	if len(data) != 0 {
		req, err = http.NewRequest(method, url, strings.NewReader(string(datajson)))
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Basic "+basicAuth(nedge.Config.Username, nedge.Config.Password))

	resp, err := client.Do(req)
	log.Debug("Response :", resp, " and error: ", err)
	if err != nil {
		log.Fatal("Error while handling request ", err)
	}

	body, err = ioutil.ReadAll(resp.Body)
	log.Debug("Got response, code: ", resp.StatusCode, ", body: ", string(body))
	nedge.checkError(resp)
	defer resp.Body.Close()
	if err != nil {
		log.Fatal(err)
	}
	return body, err
}

/* check format of volumeID string true if it is */
func isLikelyVolumeID(volumeID string) bool {
	commaParts := strings.Split(volumeID, ",")
	if len(commaParts) == 2 {
		ampersandParts := strings.Split(commaParts[1], "@")
		if len(ampersandParts) == 2 {
			objectParts := strings.Split(ampersandParts[1], "/")
			if len(objectParts) == 3 {
				return true
			}
		}
	}
	return false
}

// volumeID format: <int>,<ten>/<buc>@<cluster>/ten/buc
func GetBucketFromVolumeID(volumeID string) string {
	commaParts := strings.Split(volumeID, ",")
	if len(commaParts) == 2 {
		ampersandParts := strings.Split(commaParts[1], "@")
		if len(ampersandParts) == 2 {
			objectParts := strings.Split(ampersandParts[1], "/")
			if len(objectParts) == 3 {
				return objectParts[2]
			}
		}
	}

	// looks like its not a VolumeID so return as it is
	return volumeID
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}
