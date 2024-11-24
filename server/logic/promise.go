package logic

import (
	"context"
	"time"

	common "GolandProjects/2pc-gautamsardana/api_common"
	"GolandProjects/2pc-gautamsardana/server/config"
	"GolandProjects/2pc-gautamsardana/server/storage/datastore"
)

func Promise(ctx context.Context, conf *config.Config, req *common.CommonRequest) (*common.Promise, error) {
	promiseResp := &common.Promise{
		Term:              req.Term,
		Server:            conf.ServerNumber,
		LastCommittedTerm: conf.LastCommittedTerm,
	}

	if conf.AcceptVal != nil && time.Since(conf.AcceptVal.AcceptedTime) > 500*time.Millisecond {
		promiseResp.AcceptNum = conf.AcceptVal.TermNumber
		promiseResp.AcceptVal = conf.AcceptVal.Transaction
	} else {
		promiseResp.Txn = req.TxnRequest
		req.TxnRequest.Status = StatusPrepared
		err := datastore.UpdateTransactionStatus(conf.DataStore, req.TxnRequest)
		if err != nil {
			return nil, err
		}
	}
	return promiseResp, nil
}
