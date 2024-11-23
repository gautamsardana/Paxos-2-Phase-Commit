package logic

import (
	"context"
	"errors"
	"fmt"
	"time"

	common "GolandProjects/2pc-gautamsardana/api_common"
	"GolandProjects/2pc-gautamsardana/server/config"
	"GolandProjects/2pc-gautamsardana/server/storage/datastore"
)

func EnqueueTxn(_ context.Context, conf *config.Config, req *common.TxnRequest) {
	fmt.Printf("received enqueue txn request:%v\n", req)
	conf.TxnQueueLock.Lock()
	conf.TxnQueue.Queue = append(conf.TxnQueue.Queue, req)
	conf.TxnCount++
	conf.TxnQueueLock.Unlock()

	if len(conf.TxnQueue.Queue) == 1 {
		conf.TxnQueue.Signal <- struct{}{}
	}
}

func ProcessTxn(ctx context.Context, conf *config.Config, req *common.TxnRequest) error {
	fmt.Printf("Received ProcessTxn request: %v\n", req)

	conf.PaxosLock.Lock()
	conf.TermNumber++
	conf.PaxosLock.Unlock()

	conf.LatencyStartTime = time.Now()

	req.Term = conf.TermNumber
	GetTxnType(conf, req)

	req.Status = StatusInit
	err := datastore.InsertTransaction(conf.DataStore, req)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			conf.TxnQueueLock.Lock()
			conf.TxnQueue.Response <- FailureResponse(req, err)
			conf.TxnQueueLock.Unlock()

			req.Status = StatusFailed
			req.Error = err.Error()
			err = datastore.UpdateTransactionStatus(conf.DataStore, req)
			if err != nil {
				fmt.Println("Update transaction error:", err)
			}
		}
	}()

	promiseRequests, err := SendPrepare(ctx, conf, req)
	if len(promiseRequests) < int(conf.Majority-1) {
		return fmt.Errorf("not enough promise requests, err:%v", err)
	}

	req.Status = StatusPrepared
	err = datastore.UpdateTransactionStatus(conf.DataStore, req)
	if err != nil {
		return err
	}

	//todo: req will be different them actual txn in case of acceptVal
	err = AcquireLockWithAbort(conf, req)
	if err != nil {
		return err
	}

	defer ReleaseLock(conf, req)

	err = ValidateBalance(conf, req)
	if err != nil {
		return err
	}

	acceptedRequests, err := SendAccept(ctx, conf, promiseRequests)
	if err != nil {
		return err
	} else if len(acceptedRequests) < int(conf.Majority-1) {
		return errors.New("error: not enough accepted")
	}

	req.Status = StatusAccepted
	err = datastore.UpdateTransactionStatus(conf.DataStore, req)
	if err != nil {
		return err
	}

	txn, err := SendCommit(ctx, conf, acceptedRequests)
	if err != nil {
		return err
	}

	conf.TxnQueueLock.Lock()
	conf.TxnQueue.Response <- SuccessResponse(txn)
	conf.TxnQueueLock.Unlock()
	return nil
}
