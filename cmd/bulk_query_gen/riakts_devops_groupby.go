package main

import "time"

// RiakTSDevopsGroupby produces RiakTS-specific queries for the devops groupby case.
type RiakTSDevopsGroupby struct {
	RiakTSDevops
}

func NewRiakTSDevopsGroupBy(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newRiakTSDevopsCommon(dbConfig, start, end).(*RiakTSDevops)
	return &RiakTSDevopsGroupby{
		RiakTSDevops: *underlying,
	}

}

func (d *RiakTSDevopsGroupby) Dispatch(i, scaleVar int) Query {
	q := NewRiakTSQuery() // from pool
	d.MeanCPUUsageDayByHourAllHostsGroupbyHost(q, scaleVar)
	return q
}

