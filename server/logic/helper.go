package logic

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	common "GolandProjects/2pc-gautamsardana/api_common"
	"GolandProjects/2pc-gautamsardana/server/config"
	"GolandProjects/2pc-gautamsardana/server/storage/datastore"
)

const (
	TypeIntraShard         = "IntraShard"
	TypeCrossShardSender   = "CrossShard-Sender"
	TypeCrossShardReceiver = "CrossShard-Receiver"

	StatusInit              = "Init"
	StatusPrepared          = "Prepared"
	StatusAccepted          = "Accepted"
	StatusCommitted         = "Committed"
	StatusSuccess           = "Success"
	StatusPreparedForCommit = "PreparedForCommit"
	StatusRolledBack        = "RolledBack"
	StatusFailed            = "Failed"
)

var MapServerNumberToAddress = map[int32]string{
	1: "localhost:8081",
	2: "localhost:8082",
	3: "localhost:8083",
	4: "localhost:8084",
	5: "localhost:8085",
	6: "localhost:8086",
	7: "localhost:8087",
	8: "localhost:8088",
	9: "localhost:8089",
}

func ValidateBalance(conf *config.Config, req *common.TxnRequest) error {
	if req.Type == TypeIntraShard || req.Type == TypeCrossShardSender {
		balance, err := datastore.GetBalance(conf.DataStore, req.Sender)
		if err != nil {
			return err
		}
		if balance < req.Amount {
			return errors.New("insufficient balance")
		}
	}
	return nil
}

func GetTxnType(conf *config.Config, req *common.TxnRequest) {
	senderCluster := math.Ceil(float64(req.Sender) / float64(conf.DataItemsPerShard))
	receiverCluster := math.Ceil(float64(req.Receiver) / float64(conf.DataItemsPerShard))

	if conf.ClusterNumber == int32(senderCluster) && conf.ClusterNumber == int32(receiverCluster) {
		req.Type = TypeIntraShard
	} else if conf.ClusterNumber == int32(senderCluster) {
		req.Type = TypeCrossShardSender
	} else if conf.ClusterNumber == int32(receiverCluster) {
		req.Type = TypeCrossShardReceiver
	}
}

func AcquireLockWithAbort(conf *config.Config, req *common.TxnRequest) error {
	if req.Type == TypeIntraShard || req.Type == TypeCrossShardSender {
		if !conf.UserLocks[req.Sender%conf.ShardPartitionLength].TryLock() {
			return errors.New("lock not available for sender")
		}
	}
	if req.Type == TypeIntraShard || req.Type == TypeCrossShardReceiver {
		if !conf.UserLocks[req.Receiver%conf.ShardPartitionLength].TryLock() {
			return errors.New("lock not available receiver")
		}
	}
	return nil
}

func AcquireLock(conf *config.Config, req *common.TxnRequest) error {
	if req.Type == TypeIntraShard || req.Type == TypeCrossShardSender {
		conf.UserLocks[req.Sender%conf.ShardPartitionLength].Lock()
	}
	if req.Type == TypeIntraShard || req.Type == TypeCrossShardReceiver {
		conf.UserLocks[req.Receiver%conf.ShardPartitionLength].Lock()
	}
	return nil
}

func ReleaseLock(conf *config.Config, req *common.TxnRequest) {
	if req.Type == TypeIntraShard || req.Type == TypeCrossShardSender {
		conf.UserLocks[req.Sender%conf.ShardPartitionLength].Unlock()
	}
	if req.Type == TypeIntraShard || req.Type == TypeCrossShardReceiver {
		conf.UserLocks[req.Receiver%conf.ShardPartitionLength].Unlock()
	}
}

func FailureResponse(txn *common.TxnRequest, err error) *common.ProcessTxnResponse {
	return &common.ProcessTxnResponse{
		Txn:    txn,
		Status: StatusFailed,
		Error:  err.Error(),
	}
}

func RolledBackResponse(txn *common.TxnRequest) *common.ProcessTxnResponse {
	return &common.ProcessTxnResponse{
		Txn:    txn,
		Status: StatusRolledBack,
	}
}

func SuccessResponse(txn *common.TxnRequest) *common.ProcessTxnResponse {
	return &common.ProcessTxnResponse{
		Txn:    txn,
		Status: StatusSuccess,
	}
}

func CommitTxn(ctx context.Context, conf *config.Config, txnReq *common.TxnRequest, isSync bool) error {
	fmt.Printf("Committing txn request: %v\n", txnReq)

	txnReq.Status = StatusCommitted
	if txnReq.Type == TypeCrossShardSender || txnReq.Type == TypeCrossShardReceiver {
		if !isSync {
			txnReq.Status = StatusPreparedForCommit
		}
	}

	dbTxn, err := datastore.GetTransactionByTxnID(conf.DataStore, txnReq.TxnID)
	if err != nil && err != sql.ErrNoRows {
		return err
	} else if err == sql.ErrNoRows {
		err = datastore.InsertTransaction(conf.DataStore, txnReq)
		if err != nil {
			return err
		}
	} else if dbTxn != nil && !isSync {
		err = datastore.UpdateTransactionStatus(conf.DataStore, txnReq)
		if err != nil {
			return err
		}
	} else {
		return nil
	}

	if txnReq.Type == TypeIntraShard || txnReq.Type == TypeCrossShardSender {
		senderBalance, err := datastore.GetBalance(conf.DataStore, txnReq.Sender)
		if err != nil {
			return err
		}
		err = datastore.UpdateBalance(conf.DataStore, config.User{User: txnReq.Sender, Balance: senderBalance - txnReq.Amount})
		if err != nil {
			return err
		}
	}

	if txnReq.Type == TypeIntraShard || txnReq.Type == TypeCrossShardReceiver {
		receiverBalance, err := datastore.GetBalance(conf.DataStore, txnReq.Receiver)
		if err != nil {
			return err
		}
		err = datastore.UpdateBalance(conf.DataStore, config.User{User: txnReq.Receiver, Balance: receiverBalance + txnReq.Amount})
		if err != nil {
			return err
		}
	}
	return nil
}

func GetClientAddress() string {
	return "localhost:8000"
}
