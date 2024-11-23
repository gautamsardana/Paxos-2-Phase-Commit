package api

import (
	common "GolandProjects/2pc-gautamsardana/api_common"
	"GolandProjects/2pc-gautamsardana/client/config"
	"GolandProjects/2pc-gautamsardana/client/logic"
	"context"
	"fmt"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	common.UnimplementedPaxos2PCServer
	Config *config.Config
}

func (c *Client) ProcessTxnSet(ctx context.Context, req *common.TxnSet) (*emptypb.Empty, error) {
	err := logic.ProcessTxnSet(ctx, req, c.Config)
	if err != nil {
		fmt.Printf("Error processing txn from load balancer: %v", err)
		return nil, err
	}
	return nil, nil
}

func (c *Client) Callback(ctx context.Context, req *common.ProcessTxnResponse) (*emptypb.Empty, error) {
	logic.Callback(ctx, c.Config, req)
	return nil, nil
}

func (c *Client) PrintBalance(ctx context.Context, req *common.PrintBalanceRequest) (*common.PrintBalanceResponse, error) {
	resp, err := logic.PrintBalance(ctx, req, c.Config)
	if err != nil {
		fmt.Printf("Error printing balance: %v", err)
		return nil, err
	}
	return resp, nil
}

func (c *Client) PrintDB(ctx context.Context, req *common.PrintDBRequest) (*common.PrintDBResponse, error) {
	resp, err := logic.PrintDB(ctx, req, c.Config)
	if err != nil {
		fmt.Printf("Error printing logs: %v", err)
		return nil, err
	}
	return resp, nil
}

func (c *Client) Performance(ctx context.Context, req *common.PerformanceRequest) (*common.PerformanceResponse, error) {
	resp, err := logic.Performance(ctx, req, c.Config)
	if err != nil {
		fmt.Printf("Error evaluating performance: %v", err)
		return nil, err
	}
	return resp, nil
}
