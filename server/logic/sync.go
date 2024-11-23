package logic

import (
	"context"
	"fmt"

	common "GolandProjects/2pc-gautamsardana/api_common"
	"GolandProjects/2pc-gautamsardana/server/config"
	"GolandProjects/2pc-gautamsardana/server/storage/datastore"
)

// I am the updated server. I received this sync request from a slow server

func SyncRequest(ctx context.Context, conf *config.Config, req *common.Sync) (*common.Sync, error) {
	fmt.Printf("Received SyncRequest request: %v\n", req)

	resp, err := SendSyncResponse(ctx, conf, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// I am the updated server. I need to send new txns to slow servers

func SendSyncResponse(ctx context.Context, conf *config.Config, req *common.Sync) (*common.Sync, error) {
	newTxns, err := datastore.GetTransactionsAfterTerm(conf.DataStore, req.LastCommittedTerm)
	if err != nil {
		return nil, err
	}

	syncResponse := &common.Sync{
		NewTxns:           newTxns,
		LastCommittedTerm: conf.LastCommittedTerm,
	}

	return syncResponse, nil
}

// I am the slow server, i got these new transactions

func AddNewTxns(ctx context.Context, conf *config.Config, req *common.Sync) error {
	fmt.Printf("i was slow, adding these new txns: %v\n", req)

	if req.LastCommittedTerm <= conf.LastCommittedTerm {
		fmt.Printf("SyncNewTxns outdated %v\n", req)
		return nil
	}

	for _, txn := range req.NewTxns {
		err := AcquireLock(conf, txn)
		err = CommitTxn(ctx, conf, txn, true)
		if err != nil {
			fmt.Println(err)
			ReleaseLock(conf, txn)
			continue
		}
		ReleaseLock(conf, txn)

		conf.PaxosLock.Lock()
		if txn.Term > conf.LastCommittedTerm {
			conf.LastCommittedTerm = txn.Term
		}

		if conf.AcceptVal != nil && conf.AcceptVal.Transaction != nil &&
			conf.AcceptVal.Transaction.TxnID == txn.TxnID {
			conf.AcceptVal = nil
		}
		conf.PaxosLock.Unlock()
	}

	return nil
}
