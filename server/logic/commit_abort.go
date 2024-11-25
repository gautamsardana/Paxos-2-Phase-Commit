package logic

import (
	"context"
	"errors"
	"fmt"

	common "GolandProjects/2pc-gautamsardana/api_common"
	"GolandProjects/2pc-gautamsardana/server/config"
	"GolandProjects/2pc-gautamsardana/server/storage/datastore"
)

func Commit(ctx context.Context, conf *config.Config, req *common.TxnRequest) (*common.ProcessTxnResponse, error) {
	fmt.Printf("Received cross_shard.Commit with request: %v\n", req)

	if !conf.IsAlive {
		return nil, errors.New("server not alive")
	}

	dbTxn, err := datastore.GetTransactionByTxnID(conf.DataStore, req.TxnID)
	if err != nil {
		return FailureResponse(req, errors.New("txn id does not exist, invalid commit")), nil
	}

	if dbTxn.Status != StatusPreparedForCommit {
		return FailureResponse(req, errors.New("invalid commit")), nil
	}

	dbTxn.Status = StatusCommitted
	err = datastore.UpdateTransactionStatus(conf.DataStore, dbTxn)
	if err != nil {
		fmt.Printf("failed to update transaction status: %v\n", err)
	}

	conf.PaxosLock.Lock()
	if dbTxn.Term > conf.LastCommittedTerm {
		conf.LastCommittedTerm = dbTxn.Term
	}

	if conf.AcceptVal != nil && conf.AcceptVal.Transaction != nil &&
		conf.AcceptVal.Transaction.TxnID == req.TxnID {
		conf.AcceptVal = nil
	}
	conf.PaxosLock.Unlock()

	return SuccessResponse(req), nil
}

func Abort(ctx context.Context, conf *config.Config, req *common.TxnRequest) (*common.ProcessTxnResponse, error) {
	fmt.Printf("Received cross_shard.Abort with request: %v\n", req)

	if !conf.IsAlive {
		return nil, errors.New("server not alive")
	}

	dbTxn, err := datastore.GetTransactionByTxnID(conf.DataStore, req.TxnID)
	if err != nil {
		return FailureResponse(req, errors.New("txn id does not exist, invalid commit")), nil
	}

	err = RollbackTxn(conf, dbTxn)
	if err != nil {
		return FailureResponse(req, err), nil
	}

	conf.PaxosLock.Lock()
	if conf.AcceptVal != nil && conf.AcceptVal.Transaction != nil &&
		conf.AcceptVal.Transaction.TxnID == req.TxnID {
		conf.AcceptVal = nil
	}
	conf.PaxosLock.Unlock()

	return RolledBackResponse(req), nil
}

func RollbackTxn(conf *config.Config, req *common.TxnRequest) error {
	if req.Status != StatusPreparedForCommit {
		req.Status = StatusRolledBack
		err := datastore.UpdateTransactionStatus(conf.DataStore, req)
		if err != nil {
			fmt.Printf("failed to update transaction status: %v\n", err)
		}
		return nil
	}

	req.Status = StatusRolledBack
	err := datastore.UpdateTransactionStatus(conf.DataStore, req)
	if err != nil {
		fmt.Printf("failed to update transaction status: %v\n", err)
	}

	if req.Type == TypeCrossShardSender {
		senderBalance, err := datastore.GetBalance(conf.DataStore, req.Sender)
		if err != nil {
			return err
		}
		err = datastore.UpdateBalance(conf.DataStore, config.User{User: req.Sender, Balance: senderBalance + req.Amount})
		if err != nil {
			return err
		}
	} else if req.Type == TypeCrossShardReceiver {
		receiverBalance, err := datastore.GetBalance(conf.DataStore, req.Receiver)
		if err != nil {
			return err
		}
		err = datastore.UpdateBalance(conf.DataStore, config.User{User: req.Receiver, Balance: receiverBalance - req.Amount})
		if err != nil {
			return err
		}
	}
	return nil
}
