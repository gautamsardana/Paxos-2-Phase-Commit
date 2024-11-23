package main

import (
	"context"
	"fmt"

	common "GolandProjects/2pc-gautamsardana/api_common"
)

func PrintBalance(client common.Paxos2PCClient, user int32) {
	resp, err := client.PrintBalance(context.Background(), &common.PrintBalanceRequest{User: user})
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Printf("Balance of user %v: %v\n", user, resp.Balance)
}

func PrintDB(client common.Paxos2PCClient, server int32) {
	resp, err := client.PrintDB(context.Background(), &common.PrintDBRequest{Server: server})
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Printf("\nCommitted DB txns of server %v: \n", server)
	for _, txn := range resp.Txns {
		fmt.Printf("%v\n", txn)
	}
}

func Performance(client common.Paxos2PCClient, serverNo int32) {
	resp, err := client.Performance(context.Background(), &common.PerformanceRequest{Server: serverNo})
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	latency := resp.Latency.AsDuration()

	fmt.Printf("Total Latency till now: %s\n", latency)
	fmt.Printf("Throughput: %.2f transactions/sec\n", resp.Throughput)
}

func ProcessSet(s *common.TxnSet, client common.Paxos2PCClient) {
	_, err := client.ProcessTxnSet(context.Background(), s)
	if err != nil {
		return
	}
}
