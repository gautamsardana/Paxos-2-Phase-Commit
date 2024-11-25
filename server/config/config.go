package config

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
	"sync"
	"time"

	common "GolandProjects/2pc-gautamsardana/api_common"
	serverPool "GolandProjects/2pc-gautamsardana/server_pool"
)

type Config struct {
	BasePort            int32 `json:"base_port"`
	Port                string
	ServerNumber        int32 `json:"server_number"`
	ServerTotal         int32 `json:"server_total"`
	Majority            int32 `json:"majority"`
	Balance             map[string]float32
	DBDSN               string `json:"db_dsn"`
	DataStore           *sql.DB
	ServerAddresses     []string `json:"server_addresses"`
	Pool                *serverPool.ServerPool
	ClusterNumber       int32
	MapClusterToServers map[int32][]int32
	DataItemsPerShard   int32
	IsAlive             bool

	TxnQueueLock sync.Mutex
	TxnQueue     *TxnQueueInfo

	PaxosLock         sync.Mutex
	TermNumber        int32 `json:"term_number"`
	LastCommittedTerm int32
	AcceptVal         *AcceptValInfo

	TwoPCLock            sync.Mutex
	ShardPartitionLength int32 `json:"shard_partition_length"`
	UserLocks            []sync.Mutex

	TxnCount         int
	LatencyQueue     []time.Duration
	LatencyStartTime time.Time
}

type TxnQueueInfo struct {
	Queue    []*common.TxnRequest
	Response chan *common.ProcessTxnResponse
	Signal   chan struct{}
}

type PromiseRequestsInfo struct {
	Count        int
	HasProceeded bool
}

type AcceptValInfo struct {
	TermNumber   int32
	Transaction  *common.TxnRequest
	AcceptedTime time.Time
}

type User struct {
	User    int32
	Balance float32
}

func InitiateConfig(conf *Config) {
	InitiateServerPool(conf)
	conf.MapClusterToServers = make(map[int32][]int32)
	conf.UserLocks = make([]sync.Mutex, conf.ShardPartitionLength)
	conf.TxnQueue = &TxnQueueInfo{
		Queue:    make([]*common.TxnRequest, 0),
		Response: make(chan *common.ProcessTxnResponse, 1),
		Signal:   make(chan struct{}, 1),
	}
}

func GetConfig() *Config {
	configPath := flag.String("config", "config.json", "Path to the configuration file")
	serverNumber := flag.Int("server", 1, "Server number")
	flag.Parse()
	jsonConfig, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	conf := &Config{}
	if err = json.Unmarshal(jsonConfig, conf); err != nil {
		log.Fatal(err)
	}

	conf.ServerNumber = int32(*serverNumber)
	conf.Port = fmt.Sprintf("%d", int(conf.BasePort)+*serverNumber)

	conf.DBDSN = fmt.Sprintf(conf.DBDSN, conf.ServerNumber)
	return conf
}

func SetupDB(config *Config) {
	db, err := sql.Open("mysql", config.DBDSN)
	if err != nil {
		log.Fatal(err)
	}
	config.DataStore = db
	fmt.Println("MySQL Connected!!")
}

func InitiateServerPool(conf *Config) {
	pool, err := serverPool.NewServerPool(conf.ServerAddresses)
	if err != nil {
		log.Fatal(err)
	}
	conf.Pool = pool
}
