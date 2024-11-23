package logic

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	common "GolandProjects/2pc-gautamsardana/api_common"
	"GolandProjects/2pc-gautamsardana/server/config"
	"GolandProjects/2pc-gautamsardana/server/storage/datastore"
)

func SendPrepare(ctx context.Context, conf *config.Config, req *common.TxnRequest) ([]*common.Promise, error) {
	prepareReq := &common.CommonRequest{
		Term:              conf.TermNumber,
		LastCommittedTerm: conf.LastCommittedTerm,
		TxnRequest:        req,
		Server:            conf.ServerNumber,
	}
	fmt.Printf("Sending Prepare with request: %v\n", prepareReq)

	var promiseRequests []*common.Promise
	var prepareErr error
	var wg sync.WaitGroup
	for _, serverNo := range conf.MapClusterToServers[conf.ClusterNumber] {
		if serverNo == conf.ServerNumber {
			continue
		}

		wg.Add(1)
		go func(serverAddress string) {
			defer wg.Done()
			server, err := conf.Pool.GetServer(serverAddress)
			if err != nil {
				fmt.Println(err)
			}
			resp, err := server.Prepare(context.Background(), prepareReq)
			if err != nil {
				prepareErr = err
			}
			if resp != nil {
				fmt.Printf("received promise response from follower :%v", resp)
				if resp.NewTxns != nil {
					err = AddNewTxns(ctx, conf, &common.Sync{
						Server:            resp.Server,
						LastCommittedTerm: resp.LastCommittedTerm,
						NewTxns:           resp.NewTxns,
					})
					if err != nil {
						prepareErr = err
					}
				}
				if resp.Term > conf.TermNumber {
					conf.PaxosLock.Lock()
					conf.TermNumber = resp.Term
					req.Term = conf.TermNumber
					resp.Txn.Term = conf.TermNumber
					conf.PaxosLock.Unlock()
				}

				conf.PaxosLock.Lock()
				promiseRequests = append(promiseRequests, resp)
				conf.PaxosLock.Unlock()
			}
		}(MapServerNumberToAddress[serverNo])
	}
	wg.Wait()

	return promiseRequests, prepareErr
}

func ReceivePrepare(ctx context.Context, conf *config.Config, req *common.CommonRequest) (*common.Promise, error) {
	fmt.Printf("Received Prepare with request: %v\n", req)

	if !conf.IsAlive {
		return nil, errors.New("server not alive")
	}

	conf.LatencyStartTime = time.Now()

	if req.Term <= conf.TermNumber {
		req.Term = conf.TermNumber + 1
		req.TxnRequest.Term = req.Term
	}

	req.TxnRequest.Status = StatusInit
	err := datastore.InsertTransaction(conf.DataStore, req.TxnRequest)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			req.TxnRequest.Status = StatusFailed
			req.TxnRequest.Error = err.Error()
			err = datastore.UpdateTransactionStatus(conf.DataStore, req.TxnRequest)
			if err != nil {
				fmt.Println("Update transactionStatus error:", err)
			}
			conf.LatencyQueue = append(conf.LatencyQueue, time.Since(conf.LatencyStartTime))
		}
	}()

	var newTxns []*common.TxnRequest

	promiseResp, err := Promise(ctx, conf, req)
	if err != nil {
		return nil, err
	}

	if req.LastCommittedTerm < conf.LastCommittedTerm {
		newTxns, err = datastore.GetTransactionsAfterTerm(conf.DataStore, req.LastCommittedTerm)
		if err != nil {
			return nil, err
		}
		promiseResp.NewTxns = newTxns
	}

	err = SyncIfServerSlow(context.Background(), conf, req)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Sending promise response: %v\n", promiseResp)
	return promiseResp, nil
}

func SyncIfServerSlow(ctx context.Context, conf *config.Config, req *common.CommonRequest) error {
	if req.LastCommittedTerm > conf.LastCommittedTerm {
		fmt.Printf("server is slow, asking for new txns...")
		server, err := conf.Pool.GetServer(MapServerNumberToAddress[req.Server])
		if err != nil {
			return err
		}
		resp, err := server.SyncRequest(ctx, &common.Sync{LastCommittedTerm: conf.LastCommittedTerm, Server: conf.ServerNumber})
		if err != nil {
			return err
		}
		err = AddNewTxns(ctx, conf, resp)
		if err != nil {
			return err
		}
	}
	return nil
}
