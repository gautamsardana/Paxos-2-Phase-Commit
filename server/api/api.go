package api

import (
	"context"
	"fmt"
	"google.golang.org/protobuf/types/known/emptypb"
	"sync"
	"time"

	common "GolandProjects/2pc-gautamsardana/api_common"
	"GolandProjects/2pc-gautamsardana/server/config"
	"GolandProjects/2pc-gautamsardana/server/logic"
)

type Server struct {
	common.UnimplementedPaxos2PCServer
	Config *config.Config
}

func (s *Server) UpdateServerState(ctx context.Context, req *common.UpdateServerStateRequest) (*emptypb.Empty, error) {
	fmt.Printf("Server %d: IsAlive set to %t\n", s.Config.ServerNumber, req.IsAlive)
	s.Config.IsAlive = req.IsAlive
	s.Config.ClusterNumber = req.ClusterNumber
	s.Config.DataItemsPerShard = req.DataItemsPerShard
	s.Config.LatencyQueue = make([]time.Duration, 0)
	s.Config.TxnCount = 0

	s.Config.UserLocks = make([]sync.Mutex, s.Config.DataItemsPerShard)

	for key, cluster := range req.Clusters {
		s.Config.MapClusterToServers[key] = cluster.Values
	}

	return nil, nil
}

func (s *Server) EnqueueTxn(ctx context.Context, req *common.TxnRequest) (*emptypb.Empty, error) {
	logic.EnqueueTxn(ctx, s.Config, req)
	return nil, nil
}

func (s *Server) Prepare(ctx context.Context, req *common.CommonRequest) (*common.Promise, error) {
	promise, err := logic.ReceivePrepare(ctx, s.Config, req)
	if err != nil {
		fmt.Printf("PrepareError: %v\n", err)
		return nil, err
	}
	return promise, nil
}

func (s *Server) Accept(ctx context.Context, req *common.CommonRequest) (*common.CommonRequest, error) {
	accepted, err := logic.ReceiveAccept(ctx, s.Config, req)
	if err != nil {
		fmt.Printf("AcceptError: %v\n", err)
		return nil, err
	}
	return accepted, nil
}

func (s *Server) Commit(ctx context.Context, req *common.CommonRequest) (*emptypb.Empty, error) {
	err := logic.ReceiveCommit(ctx, s.Config, req)
	if err != nil {
		fmt.Printf("CommitError: %v\n", err)
		return nil, err
	}
	return nil, nil
}

func (s *Server) SyncRequest(ctx context.Context, req *common.Sync) (*common.Sync, error) {
	resp, err := logic.SyncRequest(ctx, s.Config, req)
	if err != nil {
		fmt.Printf("SyncRequestError: %v\n", err)
		return nil, err
	}
	return resp, nil
}

func (s *Server) TwoPCCommit(ctx context.Context, req *common.TxnRequest) (*common.ProcessTxnResponse, error) {
	resp, err := logic.Commit(ctx, s.Config, req)
	if err != nil {
		fmt.Printf("TwoPCCommitError: %v\n", err)
		return nil, err
	}
	fmt.Printf("TwoPCCommitResponse: %v\n", resp)
	return resp, nil
}

func (s *Server) TwoPCAbort(ctx context.Context, req *common.TxnRequest) (*common.ProcessTxnResponse, error) {
	resp, err := logic.Abort(ctx, s.Config, req)
	if err != nil {
		fmt.Printf("TwoPCAbortError: %v\n", err)
		return nil, err
	}
	fmt.Printf("TwoPCAbortResponse: %v\n", resp)
	return resp, nil
}

func (s *Server) PrintBalance(ctx context.Context, req *common.PrintBalanceRequest) (*common.PrintBalanceResponse, error) {
	fmt.Printf("Server %d: received PrintBalance request\n", s.Config.ServerNumber)
	resp, err := logic.PrintBalance(ctx, s.Config, req)
	if err != nil {
		fmt.Printf("PrintBalanceError: %v\n", err)
	}
	return resp, nil
}

func (s *Server) PrintDB(ctx context.Context, req *common.PrintDBRequest) (*common.PrintDBResponse, error) {
	fmt.Printf("Server %d: received PrintDB request\n", s.Config.ServerNumber)
	resp, err := logic.PrintDB(ctx, s.Config, req)
	if err != nil {
		fmt.Printf("PrintDBError: %v\n", err)
	}
	return resp, nil
}

func (s *Server) Performance(ctx context.Context, req *common.PerformanceRequest) (*common.PerformanceResponse, error) {
	fmt.Printf("Server %d: received PrintLogs request\n", s.Config.ServerNumber)
	resp := logic.Performance(ctx, s.Config, req)
	return resp, nil
}
