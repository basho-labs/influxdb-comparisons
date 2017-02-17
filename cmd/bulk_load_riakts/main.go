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
	//"bytes"

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
	batchChan    chan map[string][][]riak.TsCell
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
		defer func() {
			if err := cluster.Stop(); err != nil {
				fmt.Println(err.Error())
			}
		}()
	}

	if doLoad {
		createTable(cluster)
	}

	batchChan = make(chan map[string][][]riak.TsCell, workers)
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
	var rows = make(map[string][][]riak.TsCell)

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
		splitSeries := strings.Split(splitData[0], ",")
		series := splitSeries[0]
		tags := splitData[1]
		tsI, _ := strconv.ParseInt(splitData[1], 10, 64)
		timestamp := time.Unix(0, tsI)

		//var tableBuffer bytes.Buffer
		//tableBuffer.WriteString("CREATE TABLE measurements_")
		//tableBuffer.WriteString(series)
		//tableBuffer.WriteString(" (series VARCHAR NOT NULL, tags VARCHAR NOT NULL, time TIMESTAMP NOT NULL")

		row := []riak.TsCell{
			riak.NewStringTsCell(series),
			riak.NewStringTsCell(tags), 
			riak.NewTimestampTsCell(timestamp)}

		for rowId := 2; rowId < len(splitData); rowId++ {
			//fmt.Println(splitData[rowId])
			splitPair := strings.Split(splitData[rowId], "=")
			valD, _ := strconv.ParseFloat(splitPair[1], 64)
			cell := riak.NewDoubleTsCell(valD)
			row = append(row, cell)

			//tableBuffer.WriteString(", ")
			//tableBuffer.WriteString(splitPair[0])
			//tableBuffer.WriteString(" DOUBLE")
		}

		//tableBuffer.WriteString(", primary key ((quantum(time, 1, h)), time))")
		//fmt.Println(tableBuffer.String())

		//fmt.Println("\n\n")

		rows[series] = append(rows[series], row)
		n++

		if n >= itemsPerBatch {
			batchChan <- rows
			rows = make(map[string][][]riak.TsCell)
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

		for k := range batch {
			// Build the write command
			tablename := fmt.Sprintf("measurements_%s", k)
			cmd, err := riak.NewTsStoreRowsCommandBuilder().WithTable(tablename).WithRows(batch[k]).Build()
			if err != nil {
				log.Fatalf("Error while building write command: %s\n", err.Error())
			} 

			// Execute the write command
			err = cluster.Execute(cmd)
			if err != nil {
				log.Fatalf("Error while writing: %s\n", err.Error())
			}
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
	if err = cluster.Start(); err != nil {
		fmt.Println(err.Error())
	}

	ping := &riak.PingCommand{}
	if err = cluster.Execute(ping); err != nil {
		fmt.Println(err.Error())
	}

	return cluster
}

func createTable(cluster *riak.Cluster) {
	tableDefs := []string {
		"CREATE TABLE measurements_cpu (series VARCHAR NOT NULL, tags VARCHAR NOT NULL, time TIMESTAMP NOT NULL, usage_user DOUBLE, usage_system DOUBLE, usage_idle DOUBLE, usage_nice DOUBLE, usage_iowait DOUBLE, usage_irq DOUBLE, usage_softirq DOUBLE, usage_steal DOUBLE, usage_guest DOUBLE, usage_guest_nice DOUBLE, primary key ((quantum(time, 1, h)), time))",
		"CREATE TABLE measurements_diskio (series VARCHAR NOT NULL, tags VARCHAR NOT NULL, time TIMESTAMP NOT NULL, reads DOUBLE, writes DOUBLE, read_bytes DOUBLE, write_bytes DOUBLE, read_time DOUBLE, write_time DOUBLE, io_time DOUBLE, primary key ((quantum(time, 1, h)), time))",
		"CREATE TABLE measurements_disk (series VARCHAR NOT NULL, tags VARCHAR NOT NULL, time TIMESTAMP NOT NULL, total DOUBLE, free DOUBLE, used DOUBLE, used_percent DOUBLE, inodes_total DOUBLE, inodes_total DOUBLE, inodes_used DOUBLE, primary key ((quantum(time, 1, h)), time))",
		"CREATE TABLE measurements_kernel (series VARCHAR NOT NULL, tags VARCHAR NOT NULL, time TIMESTAMP NOT NULL, boot_time DOUBLE, interrupts DOUBLE, context_switches DOUBLE, processes_forked DOUBLE, disk_pages_in DOUBLE, disk_pages_out DOUBLE, primary key ((quantum(time, 1, h)), time))",
		"CREATE TABLE measurements_mem (series VARCHAR NOT NULL, tags VARCHAR NOT NULL, time TIMESTAMP NOT NULL, total DOUBLE, available DOUBLE, used DOUBLE, free DOUBLE, cached DOUBLE, buffered DOUBLE, used_percent DOUBLE, available_percent DOUBLE, buffered_percent DOUBLE, primary key ((quantum(time, 1, h)), time))",
		"CREATE TABLE measurements_net (series VARCHAR NOT NULL, tags VARCHAR NOT NULL, time TIMESTAMP NOT NULL, total_connections_received DOUBLE, expired_keys DOUBLE, evicted_keys DOUBLE, keyspace_hits DOUBLE, keyspace_misses DOUBLE, instantaneous_ops_per_sec DOUBLE, instantaneous_input_kbps DOUBLE, instantaneous_output_kbps DOUBLE, primary key ((quantum(time, 1, h)), time))",
		"CREATE TABLE measurements_nginx (series VARCHAR NOT NULL, tags VARCHAR NOT NULL, time TIMESTAMP NOT NULL, accepts DOUBLE, active DOUBLE, handled DOUBLE, reading DOUBLE, requests DOUBLE, waiting DOUBLE, writing DOUBLE, primary key ((quantum(time, 1, h)), time))",
		"CREATE TABLE measurements_postgresl (series VARCHAR NOT NULL, tags VARCHAR NOT NULL, time TIMESTAMP NOT NULL, numbackends DOUBLE, xact_commit DOUBLE, xact_rollback DOUBLE, blks_read DOUBLE, blks_hit DOUBLE, tup_returned DOUBLE, tup_fetched DOUBLE, tup_inserted DOUBLE, tup_updated DOUBLE, tup_deleted DOUBLE, conflicts DOUBLE, temp_files DOUBLE, temp_bytes DOUBLE, deadlocks DOUBLE, blk_read_time DOUBLE, blk_write_time DOUBLE, primary key ((quantum(time, 1, h)), time))",
		"CREATE TABLE measurements_redis (series VARCHAR NOT NULL, tags VARCHAR NOT NULL, time TIMESTAMP NOT NULL, uptime_in_seconds DOUBLE, total_connections_received DOUBLE, expired_keys DOUBLE, evicted_keys DOUBLE, keyspace_hits DOUBLE, keyspace_misses DOUBLE, instantaneous_ops_per_sec DOUBLE, instantaneous_input_kbps DOUBLE, instantaneous_output_kbps DOUBLE, connected_clients DOUBLE, used_memory DOUBLE, used_memory_rss DOUBLE, used_memory_peak DOUBLE, used_memory_lua DOUBLE, rdb_changes_since_last_save DOUBLE, sync_full DOUBLE, sync_partial_ok DOUBLE, sync_partial_err DOUBLE, pubsub_channels DOUBLE, pubsub_patterns DOUBLE, latest_fork_usec DOUBLE, connected_slaves DOUBLE, master_repl_offset DOUBLE, repl_backlog_active DOUBLE, repl_backlog_size DOUBLE, repl_backlog_histlen DOUBLE, mem_fragmentation_ratio DOUBLE, used_cpu_sys DOUBLE, used_cpu_user DOUBLE, used_cpu_sys_children DOUBLE, used_cpu_user_children DOUBLE, primary key ((quantum(time, 1, h)), time))",
	}

	for _,q := range tableDefs {
		cmd, err := riak.NewTsQueryCommandBuilder().WithQuery(q).Build()
		if err != nil {
			log.Fatalf("Error while building create table command: %s\n", err.Error())
		}

		// Execute the create table command
		_ = cluster.Execute(cmd)
		// if err != nil {
		// 	log.Fatalf("Error while creating table: %s\n", err.Error())
		// }
	}
}
