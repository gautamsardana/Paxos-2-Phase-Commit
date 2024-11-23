package logstore

import (
	common "GolandProjects/apaxos-gautamsardana/api_common"
)

type LogStore struct {
	Logs map[string]*common.TxnRequest
}

func NewLogStore() *LogStore {
	return &LogStore{
		Logs: make(map[string]*common.TxnRequest),
	}
}

func (store *LogStore) AddTransactionLog(txn *common.TxnRequest) {
	store.Logs[txn.MsgID] = txn
}
