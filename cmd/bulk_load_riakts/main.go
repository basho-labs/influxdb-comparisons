// bulk_load_riakts loads a RiakTS daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
	"strings"
	"strconv"

	riak "github.com/basho/riak-go-client"
)

// Program option vars:
var (
	daemonUrl    string
	workers      int
	batchSize    int
	doLoad       bool
	writeTimeout time.Duration
)

// Global vars
var (
	batchChan    chan [][]riak.TsCell
	inputDone    chan struct{}
	workersGroup sync.WaitGroup
)

// Parse args:
func init() {
	flag.StringVar(&daemonUrl, "url", "localhost:8087", "RiakTS URL.")

	flag.IntVar(&batchSize, "batch-size", 100, "Batch size (input items).")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.DurationVar(&writeTimeout, "write-timeout", 10*time.Second, "Write timeout.")

	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")

	flag.Parse()
}

func main() {
	var cluster *riak.Cluster
	if doLoad {
		cluster = buildCluster(daemonUrl)
	}

	if doLoad {
		//createTable(cluster)
	}

	batchChan = make(chan [][]riak.TsCell, workers)
	inputDone = make(chan struct{})

	for i := 0; i < workers; i++ {
		workersGroup.Add(1)
		go processBatches(cluster)
	}

	start := time.Now()
	itemsRead := scan(cluster, batchSize)

	<-inputDone
	close(batchChan)
	workersGroup.Wait()
	end := time.Now()
	took := end.Sub(start)
	rate := float64(itemsRead) / float64(took.Seconds())

	fmt.Printf("loaded %d items in %fsec with %d workers (mean rate %f/sec)\n", itemsRead, took.Seconds(), workers, rate)
}

// scan reads lines from stdin. It expects input in the Cassandra CQL format.
func scan(session *riak.Cluster, itemsPerBatch int) int64 {
	var rows [][]riak.TsCell
	if doLoad {
		rows = [][]riak.TsCell{}
	}

	var n int
	var linesRead int64
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		linesRead++

		if !doLoad {
			continue
		}

		//<series>:<timestamp>:<value>
		dataRow := string(scanner.Bytes())
		splitData := strings.Split(dataRow, ":")
		tsI, _ := strconv.ParseInt(splitData[1], 10, 64)
		timestamp := time.Unix(tsI, 0)
		valD, _ := strconv.ParseFloat(splitData[2], 64)

		row := []riak.TsCell{
			riak.NewStringTsCell(splitData[0]), 
			riak.NewTimestampTsCell(timestamp), 
			riak.NewDoubleTsCell(valD)}

		rows = append(rows, row)
		n++

		if n >= itemsPerBatch {
			batchChan <- rows
			rows = rows[:0]
			n = 0
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if n > 0 {
		batchChan <- rows
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(inputDone)

	// The Cassandra query format uses 1 line per item:
	itemsRead := linesRead

	return itemsRead
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatches(cluster *riak.Cluster) {
	for batch := range batchChan {
		if !doLoad {
			continue
		}

		// Build the write command
		cmd, err := riak.NewTsStoreRowsCommandBuilder().WithTable("usertable").WithRows(batch).Build()
		if err != nil {
			log.Fatalf("Error while building write command: %s\n", err.Error())
		}

		// Execute the write command
		err = cluster.Execute(cmd)
		if err != nil {
			log.Fatalf("Error while writing: %s\n", err.Error())
		}
	}
	workersGroup.Done()
}

func buildCluster(daemon_url string) *riak.Cluster {
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
	defer func() {
		if err = cluster.Stop(); err != nil {
			fmt.Println(err.Error())
		}
	}()
	if err = cluster.Start(); err != nil {
		fmt.Println(err.Error())
	}

	ping := &riak.PingCommand{}
	if err = cluster.Execute(ping); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("ping passed")
	}

	return cluster
}

func createTable(cluster *riak.Cluster) {
	q := `CREATE TABLE usertable (
			series VARCHAR NOT NULL, 
			time TIMESTAMP NOT NULL, 
			value DOUBLE, 
			primary key ((series, quantum(time, 900, s)), series, time))`
	cmd, err := riak.NewTsQueryCommandBuilder().WithQuery(q).Build()
	if err != nil {
		log.Fatalf("Error while building create table command: %s\n", err.Error())
	}

	// Execute the create table command
	err = cluster.Execute(cmd)
	if err != nil {
		log.Fatalf("Error while creating table: %s\n", err.Error())
	}
}
