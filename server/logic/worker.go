package logic

import (
	"context"
	"fmt"

	common "GolandProjects/2pc-gautamsardana/api_common"
	"GolandProjects/2pc-gautamsardana/server/config"
)

func ProcessNextTxn(conf *config.Config) {
	for {
		select {
		case <-conf.TxnQueue.Signal:
			if len(conf.TxnQueue.Queue) > 0 {
				currentTxn := conf.TxnQueue.Queue[0]
				err := ProcessTxn(context.Background(), conf, currentTxn)
				if err != nil {
					fmt.Println(err)
				}
			}
		}
	}
}

func TransactionWorker(conf *config.Config) {
	for {
		select {
		case response := <-conf.TxnQueue.Response:

			client, err := conf.Pool.GetServer(GetClientAddress())
			if err != nil {
				fmt.Println(err)
			}

			client.Callback(context.Background(), response)

			//in case a txn gets executed because of acceptVal, we don't need to remove the original txn
			conf.TxnQueueLock.Lock()
			if len(conf.TxnQueue.Queue) > 1 {
				if conf.TxnQueue.Queue[0].TxnID == response.Txn.TxnID {
					conf.TxnQueue.Queue = conf.TxnQueue.Queue[1:]
				}
			} else {
				conf.TxnQueue.Queue = make([]*common.TxnRequest, 0)
			}

			conf.TxnQueue.Signal <- struct{}{}
			conf.TxnQueueLock.Unlock()
		}
	}
}
