package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	common "GolandProjects/2pc-gautamsardana/api_common"
	serverPool "GolandProjects/2pc-gautamsardana/server_pool"
)

const configPath = "/go/src/GolandProjects/2pc-gautamsardana/client/config/config.json"

type Config struct {
	Port                string   `json:"port"`
	ServerAddresses     []string `json:"server_addresses"`
	Clusters            int32    `json:"clusters"`
	ClusterSize         int32    `json:"cluster_size"`
	TotalUsers          int32    `json:"total_users"`
	DataItemsPerShard   int32
	Pool                *serverPool.ServerPool
	MapClusterToServers map[int32][]int32

	Lock         sync.Mutex
	TxnResponses map[string][]*common.ProcessTxnResponse
}

func GetConfig() *Config {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}
	jsonConfig, err := os.ReadFile(homeDir + configPath)
	if err != nil {
		log.Fatal(err)
	}
	conf := &Config{}
	if err = json.Unmarshal(jsonConfig, conf); err != nil {
		log.Fatal(err)
	}
	return conf
}

func InitiateServerPool(conf *Config) {
	pool, err := serverPool.NewServerPool(conf.ServerAddresses)
	if err != nil {
		log.Fatal(err)
	}
	conf.Pool = pool
}

func InitiateConfig(conf *Config) {
	conf.TxnResponses = make(map[string][]*common.ProcessTxnResponse)
}

func InitiateClusters(conf *Config) {
	conf.DataItemsPerShard = conf.TotalUsers / conf.Clusters
	conf.MapClusterToServers = make(map[int32][]int32)
	totalClusters := int(conf.Clusters)
	clusterSize := int(conf.ClusterSize)

	for i := 0; i < totalClusters; i++ {
		for j := 0; j < clusterSize; j++ {
			conf.MapClusterToServers[int32(i+1)] = append(conf.MapClusterToServers[int32(i+1)], int32(j+1+(i*clusterSize)))
		}
	}
	fmt.Println(conf.MapClusterToServers)
}
