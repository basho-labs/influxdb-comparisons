package main

import (
	"fmt"
	"time"
	"strings"

	riak "github.com/basho/riak-go-client"
)

// NewRiakTSCluster creates a new RiakTS Cluster. It is goroutine-safe
// by default, and uses a connection pool.
func NewRiakTSCluster(daemonUrl string, timeout time.Duration) *riak.Cluster {
	var cluster *riak.Cluster
	nodes := []*riak.Node{}

	for _, nodeAddress := range strings.Split(daemonUrl, ",") {
		nodeOpts := &riak.NodeOptions{
			RemoteAddress: nodeAddress,
		}
		var node *riak.Node
		var err error
		if node, err = riak.NewNode(nodeOpts); err != nil {
			fmt.Println(err.Error())
		}
		nodes = append(nodes, node)
	}

	opts := &riak.ClusterOptions{
		Nodes: nodes,
	}

	// Build cluster
	cluster, err := riak.NewCluster(opts)
	if err != nil {
		fmt.Println(err.Error())
	}
	if err = cluster.Start(); err != nil {
		fmt.Println(err.Error())
	}

	ping := &riak.PingCommand{}
	if err = cluster.Execute(ping); err != nil {
		fmt.Println(err.Error())
	}

	return cluster
}