package logic

import (
	"context"
	"fmt"

	common "GolandProjects/2pc-gautamsardana/api_common"
	"GolandProjects/2pc-gautamsardana/client/config"
)

func ProcessIntraShardTxn(conf *config.Config, txn *common.TxnRequest, cluster int32, contactServers []string) {
	server, err := conf.Pool.GetServer(GetContactServerForCluster(conf, cluster, contactServers))
	if err != nil {
		fmt.Println(err)
	}

	_, err = server.EnqueueTxn(context.Background(), txn)
	if err != nil {
		fmt.Println(err)
	}
}
