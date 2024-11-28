package logic

import (
	"context"
	"fmt"
	"sync"

	common "GolandProjects/2pc-gautamsardana/api_common"
	"GolandProjects/2pc-gautamsardana/server/config"
	"GolandProjects/2pc-gautamsardana/server/storage/datastore"
)

func SendCommit(ctx context.Context, conf *config.Config, acceptedRequests []*common.CommonRequest) (*common.TxnRequest, error) {
	var txnReq *common.TxnRequest
	var wg sync.WaitGroup
	fmt.Printf("Sending Commit with AcceptedRequests: %v\n", acceptedRequests)

	for _, acceptedRequest := range acceptedRequests {
		wg.Add(1)
		go func(req *common.CommonRequest) {
			defer wg.Done()
			if req.Term != conf.TermNumber {
				fmt.Println("Error: termNumber not match")
			}
			if txnReq != nil && req.TxnRequest.TxnID != txnReq.TxnID {
				fmt.Println(req.TxnRequest, txnReq)
				fmt.Println("Error: txnRequest does not match")
			}

			txnReq = req.TxnRequest

			acceptReq := &common.CommonRequest{
				Term:       req.Term,
				TxnRequest: req.TxnRequest,
			}

			server, err := conf.Pool.GetServer(MapServerNumberToAddress[req.Server])
			if err != nil {
				fmt.Println(err)
			}
			_, err = server.Commit(ctx, acceptReq)
			if err != nil {
				fmt.Println(err)
			}
		}(acceptedRequest)
	}
	wg.Wait()

	err := CommitTxn(ctx, conf, txnReq, false)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			ReleaseLock(conf, txnReq)
			txnReq.Status = StatusFailed
			txnReq.Error = err.Error()
			err = datastore.UpdateTransactionStatus(conf.DataStore, txnReq)
			if err != nil {
				fmt.Println("Update transactionStatus error:", err)
			}
		}
	}()

	conf.PaxosLock.Lock()
	if txnReq.Type == TypeIntraShard {
		conf.LastCommittedTerm = txnReq.Term

		if conf.AcceptVal != nil && conf.AcceptVal.Transaction != nil &&
			conf.AcceptVal.Transaction.TxnID == txnReq.TxnID {
			conf.AcceptVal = nil
		}
	}
	conf.PaxosLock.Unlock()

	return txnReq, nil
}

func ReceiveCommit(ctx context.Context, conf *config.Config, req *common.CommonRequest) error {
	fmt.Printf("Received Commit request: %v\n", req)

	var err error
	defer func() {
		if err != nil {
			req.TxnRequest.Status = StatusFailed
			req.TxnRequest.Error = err.Error()
			err = datastore.UpdateTransactionStatus(conf.DataStore, req.TxnRequest)
			if err != nil {
				fmt.Println("Update transactionStatus error:", err)
			}
		}
		ReleaseLock(conf, req.TxnRequest)
	}()

	if req.Term <= conf.LastCommittedTerm {
		err = fmt.Errorf("outdated commit request: %v\n", req)
		return err
	}

	err = CommitTxn(ctx, conf, req.TxnRequest, false)
	if err != nil {
		return err
	}

	conf.PaxosLock.Lock()
	if req.TxnRequest.Type == TypeIntraShard {
		conf.LastCommittedTerm = req.TxnRequest.Term

		if conf.AcceptVal != nil && conf.AcceptVal.Transaction != nil &&
			conf.AcceptVal.Transaction.TxnID == req.TxnRequest.TxnID {
			conf.AcceptVal = nil
		}
	}
	conf.PaxosLock.Unlock()

	return nil
}
