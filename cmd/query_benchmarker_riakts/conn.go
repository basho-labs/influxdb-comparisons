package main

import (
	"fmt"
	"time"

	riak "github.com/basho/riak-go-client"
)

// NewRiakTSCluster creates a new RiakTS Cluster. It is goroutine-safe
// by default, and uses a connection pool.
func NewRiakTSCluster(daemonUrl string, timeout time.Duration) *riak.Cluster {
	var cluster *riak.Cluster

	// Build nodes
	nodeOpts := &riak.NodeOptions{
		RemoteAddress: daemonUrl,
	}
	var node *riak.Node
	var err error
	if node, err = riak.NewNode(nodeOpts); err != nil {
		fmt.Println(err.Error())
	}
	nodes := []*riak.Node{node}
	opts := &riak.ClusterOptions{
		Nodes: nodes,
	}

	// Build cluster
	cluster, err = riak.NewCluster(opts)
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
