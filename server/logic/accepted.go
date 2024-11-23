package logic

import (
	"GolandProjects/2pc-gautamsardana/server/storage/datastore"
	"context"
	"fmt"

	common "GolandProjects/2pc-gautamsardana/api_common"
	"GolandProjects/2pc-gautamsardana/server/config"
)

func Accepted(ctx context.Context, conf *config.Config, req *common.CommonRequest) (*common.CommonRequest, error) {
	acceptedResp := &common.CommonRequest{
		Term:       req.Term,
		Server:     conf.ServerNumber,
		TxnRequest: req.TxnRequest,
	}

	req.TxnRequest.Status = StatusAccepted
	err := datastore.UpdateTransactionStatus(conf.DataStore, req.TxnRequest)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Sending Accepted response: %v\n", acceptedResp)

	return acceptedResp, nil
}
