package logic

import (
	"context"
	"math"

	common "GolandProjects/2pc-gautamsardana/api_common"
	"GolandProjects/2pc-gautamsardana/client/config"
)

func PrintBalance(ctx context.Context, req *common.PrintBalanceRequest, conf *config.Config) (*common.PrintBalanceResponse, error) {
	userCluster := int32(math.Ceil(float64(req.User) / float64(conf.DataItemsPerShard)))
	result := map[int32]float32{}

	for _, serverNo := range conf.MapClusterToServers[userCluster] {
		server, err := conf.Pool.GetServer(mapServerNoToServerAddr[serverNo])
		if err != nil {
			return nil, err
		}

		resp, err := server.PrintBalance(context.Background(), req)
		if err != nil {
			return nil, err
		}

		for k, v := range resp.Balance {
			result[k] = v
		}
	}

	return &common.PrintBalanceResponse{Balance: result}, nil
}

func PrintDB(ctx context.Context, req *common.PrintDBRequest, conf *config.Config) (*common.PrintDBResponse, error) {
	serverAddr := mapServerNoToServerAddr[req.Server]
	server, err := conf.Pool.GetServer(serverAddr)
	if err != nil {
		return nil, err
	}
	resp, err := server.PrintDB(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func Performance(ctx context.Context, req *common.PerformanceRequest, conf *config.Config) (*common.PerformanceResponse, error) {
	serverAddr := mapServerNoToServerAddr[req.Server]
	server, err := conf.Pool.GetServer(serverAddr)
	if err != nil {
		return nil, err
	}
	resp, err := server.Performance(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
