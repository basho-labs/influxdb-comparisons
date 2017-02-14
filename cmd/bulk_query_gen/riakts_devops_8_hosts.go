package main

import "time"

// RiakTSDevops8Hosts produces RiakTS-specific queries for the devops groupby case.
type RiakTSDevops8Hosts struct {
	RiakTSDevops
}

func NewRiakTSDevops8Hosts(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newRiakTSDevopsCommon(dbConfig, start, end).(*RiakTSDevops)
	return &RiakTSDevops8Hosts{
		RiakTSDevops: *underlying,
	}
}

func (d *RiakTSDevops8Hosts) Dispatch(_, scaleVar int) Query {
	q := NewRiakTSQuery() // from pool
	d.MaxCPUUsageHourByMinuteEightHosts(q, scaleVar)
	return q
}
