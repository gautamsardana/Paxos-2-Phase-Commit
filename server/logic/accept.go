package logic

import (
	"context"
	"fmt"
	"sync"
	"time"

	common "GolandProjects/2pc-gautamsardana/api_common"
	"GolandProjects/2pc-gautamsardana/server/config"
	"GolandProjects/2pc-gautamsardana/server/storage/datastore"
)

func SendAccept(ctx context.Context, conf *config.Config, promiseRequests []*common.Promise) ([]*common.CommonRequest, error) {
	fmt.Println(conf.TermNumber)
	acceptReq := &common.CommonRequest{Server: conf.ServerNumber, Term: conf.TermNumber}
	var servers []int32

	maxAcceptNum := int32(0)
	for _, promiseReq := range promiseRequests {
		if promiseReq.AcceptVal != nil && promiseReq.AcceptNum > maxAcceptNum {
			acceptReq.TxnRequest = promiseReq.AcceptVal
			maxAcceptNum = promiseReq.AcceptNum
		} else if promiseReq.Txn != nil {
			acceptReq.TxnRequest = promiseReq.Txn
		}
		servers = append(servers, promiseReq.Server)
	}

	acceptReq.TxnRequest.Term = conf.TermNumber
	conf.PaxosLock.Lock()
	if conf.AcceptVal == nil {
		conf.AcceptVal = &config.AcceptValInfo{}
	}
	conf.AcceptVal.Transaction = acceptReq.TxnRequest
	conf.AcceptVal.TermNumber = acceptReq.TxnRequest.Term
	conf.AcceptVal.AcceptedTime = time.Now()
	conf.PaxosLock.Unlock()

	fmt.Printf("Sending Accept with request: %v\n", acceptReq)

	var acceptedRequests []*common.CommonRequest
	var wg sync.WaitGroup

	for _, serverNo := range servers {
		serverAddr := MapServerNumberToAddress[serverNo]
		wg.Add(1)
		go func(serverAddr string) {
			defer wg.Done()
			server, err := conf.Pool.GetServer(serverAddr)
			if err != nil {
				fmt.Println(err)
			}
			resp, err := server.Accept(ctx, acceptReq)
			if err != nil {
				fmt.Println(err)
			}
			if resp != nil {
				conf.PaxosLock.Lock()
				acceptedRequests = append(acceptedRequests, resp)
				conf.PaxosLock.Unlock()
			}
		}(serverAddr)
	}
	wg.Wait()

	return acceptedRequests, nil
}

func ReceiveAccept(ctx context.Context, conf *config.Config, req *common.CommonRequest) (*common.CommonRequest, error) {
	fmt.Printf("Received Accept with request: %v\n", req)

	err := AcquireLockWithAbort(conf, req.TxnRequest)
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
			ReleaseLock(conf, req.TxnRequest)
			conf.LatencyQueue = append(conf.LatencyQueue, time.Since(conf.LatencyStartTime))
		}
	}()

	if req.Term > conf.TermNumber {
		conf.PaxosLock.Lock()
		conf.TermNumber = req.Term
		conf.PaxosLock.Unlock()
	}

	err = ValidateBalance(conf, req.TxnRequest)
	if err != nil {
		return nil, err
	}

	conf.PaxosLock.Lock()
	if conf.AcceptVal == nil {
		conf.AcceptVal = &config.AcceptValInfo{}
	}
	conf.AcceptVal.Transaction = req.TxnRequest
	conf.AcceptVal.TermNumber = req.TxnRequest.Term
	conf.AcceptVal.AcceptedTime = time.Now()
	conf.PaxosLock.Unlock()

	return Accepted(ctx, conf, req)
}
