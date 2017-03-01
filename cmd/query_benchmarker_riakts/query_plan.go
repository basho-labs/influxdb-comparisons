package main

import (
	"fmt"
	"sort"
	"time"

	riak "github.com/basho/riak-go-client"
)

// A QueryPlan is a strategy used to fulfill an HLQuery.
type QueryPlan interface {
	Execute(*riak.Cluster) ([]RiakTSResult, error)
	DebugQueries(int)
}

// A QueryPlanWithServerAggregation fulfills an HLQuery by performing
// aggregation on both the server and the client. This results in more
// round-trip requests, but uses the server to aggregate over large datasets.
//
// It has 1) an Aggregator, which merges data on the client, and 2) a map of
// time interval buckets to RiakTS queries, which are used to retrieve data
// relevant to each bucket.
type QueryPlanWithServerAggregation struct {
	AggregatorLabel    string
	BucketedRiakTSQueries map[TimeInterval][]RiakTSQuery
}

// NewQueryPlanWithServerAggregation builds a QueryPlanWithServerAggregation.
// It is typically called via (*HLQuery).ToQueryPlanWithServerAggregation.
func NewQueryPlanWithServerAggregation(aggrLabel string, bucketedRiakTSQueries map[TimeInterval][]RiakTSQuery) (*QueryPlanWithServerAggregation, error) {
	qp := &QueryPlanWithServerAggregation{
		AggregatorLabel:    aggrLabel,
		BucketedRiakTSQueries: bucketedRiakTSQueries,
	}
	return qp, nil
}

// Execute runs all RiakTSQueries in the QueryPlan and collects the results.
//
// TODO(rw): support parallel execution.
func (qp *QueryPlanWithServerAggregation) Execute(cluster *riak.Cluster) ([]RiakTSResult, error) {
	// sort the time interval buckets we'll use:
	sortedKeys := make([]TimeInterval, 0, len(qp.BucketedRiakTSQueries))
	for k := range qp.BucketedRiakTSQueries {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Sort(TimeIntervals(sortedKeys))

	// for each bucket, execute its queries while aggregating its results
	// in constant space, then append them to the result set:
	results := make([]RiakTSResult, 0, len(qp.BucketedRiakTSQueries))
	for _, k := range sortedKeys {
		agg, err := GetAggregator(qp.AggregatorLabel)
		if err != nil {
			return nil, err
		}

		for _, q := range qp.BucketedRiakTSQueries[k] {
			cmd, err := riak.NewTsQueryCommandBuilder().WithQuery(q.QueryString).Build()
			if err != nil {
				return nil, err
			}

			err = cluster.Execute(cmd)
			if err != nil {
				return nil, err
			}

			scmd, _ := cmd.(*riak.TsQueryCommand);
			for _, row := range scmd.Response.Rows {
				s := row[0].GetStringValue()
				fmt.Println(s)
			}	
			// Execute one RiakTSQuery and collect its result
			//
			// For server-side aggregation, this will return only
			// one row; for exclusive client-side aggregation this
			// will return a sequence.



			// iter := session.Query(q.PreparableQueryString, q.Args...).Iter()
			// var x float64
			// for iter.Scan(&x) {
			// 	agg.Put(x)
			// }
			// if err := iter.Close(); err != nil {
			// 	return nil, err
			// }
		}
		results = append(results, RiakTSResult{TimeInterval: k, Value: agg.Get()})
	}

	return results, nil
}

// DebugQueries prints debugging information.
func (qp *QueryPlanWithServerAggregation) DebugQueries(level int) {
	if level >= 1 {
		n := 0
		for _, qq := range qp.BucketedRiakTSQueries {
			n += len(qq)
		}
		fmt.Printf("[qpsa] query with server aggregation plan has %d RiakTSQuery objects\n", n)
	}

	if level >= 2 {
		for k, qq := range qp.BucketedRiakTSQueries {
			for i, q := range qq {
				fmt.Printf("[qpsa] Query: %s, %d, %s\n", k, i, q)
			}
		}
	}
}

// A QueryPlanWithoutServerAggregation fulfills an HLQuery by performing
// table scans on the server and aggregating all data on the client. This
// results in higher bandwidth usage but fewer round-trip requests.
//
// It has 1) a map of Aggregators (one for each time bucket) which merge data
// on the client, 2) a GroupByDuration, which is used to reconstruct time
// buckets from a server response, 3) a set of TimeBuckets, which are used to
// store final aggregated items, and 4) a set of RiakTSQueries used to fulfill
// this plan.
type QueryPlanWithoutServerAggregation struct {
	Aggregators     map[TimeInterval]Aggregator
	GroupByDuration time.Duration
	TimeBuckets     []TimeInterval
	RiakTSQueries      []RiakTSQuery
}

// NewQueryPlanWithoutServerAggregation builds a QueryPlanWithoutServerAggregation.
// It is typically called via (*HLQuery).ToQueryPlanWithoutServerAggregation.
func NewQueryPlanWithoutServerAggregation(aggrLabel string, groupByDuration time.Duration, timeBuckets []TimeInterval, queries []RiakTSQuery) (*QueryPlanWithoutServerAggregation, error) {
	aggrs := make(map[TimeInterval]Aggregator, len(timeBuckets))
	for _, ti := range timeBuckets {
		aggr, err := GetAggregator(aggrLabel)
		if err != nil {
			return nil, err
		}

		aggrs[ti] = aggr
	}

	qp := &QueryPlanWithoutServerAggregation{
		Aggregators:     aggrs,
		GroupByDuration: groupByDuration,
		TimeBuckets:     timeBuckets,
		RiakTSQueries:   queries,
	}
	return qp, nil
}

// Execute runs all RiakTSQueries in the QueryPlan and collects the results.
//
// TODO(rw): support parallel execution.
func (qp *QueryPlanWithoutServerAggregation) Execute(cluster *riak.Cluster) ([]RiakTSResult, error) {
	// for each query, execute it, then put each result row into the
	// client-side aggregator that matches its time bucket:
	//for _, q := range qp.RiakTSQueries {
		cmd, err := riak.NewTsQueryCommandBuilder().WithQuery(qp.RiakTSQueries[0].QueryString).Build()
		if err != nil {
			return nil, err
		}

		err = cluster.Execute(cmd)
		if err != nil {
			return nil, err
		}

		scmd, _ := cmd.(*riak.TsQueryCommand);

		for _, row := range scmd.Response.Rows {
			for i := 3; i < len(scmd.Response.Columns); i++ {
				ts := row[2].GetTimeValue().UTC()
				tsTruncated := ts.Truncate(qp.GroupByDuration)
				bucketKey := TimeInterval {
					Start: tsTruncated,	
					End:   tsTruncated.Add(qp.GroupByDuration),
				}
				qp.Aggregators[bucketKey].Put(row[i].GetDoubleValue())
			}
		}
	//}

	// perform client-side aggregation across all buckets:
	results := make([]RiakTSResult, 0, len(qp.TimeBuckets))
	for _, ti := range qp.TimeBuckets {
		acc := qp.Aggregators[ti].Get()
		results = append(results, RiakTSResult{TimeInterval: ti, Value: acc})
	}

	return results, nil
}

// DebugQueries prints debugging information.
func (qp *QueryPlanWithoutServerAggregation) DebugQueries(level int) {
	if level >= 1 {
		fmt.Printf("[qpca] query with client aggregation plan has %d RiakTSQuery objects\n", len(qp.RiakTSQueries))
	}

	if level >= 2 {
		for i, q := range qp.RiakTSQueries {
			fmt.Printf("[qpca] Query: %d, %s\n", i, q)
		}
	}
}
