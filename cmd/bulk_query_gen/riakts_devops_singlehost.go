package main

import "time"

// RiakTSDevopsSingleHost produces RiakTS-specific queries for the devops single-host case.
type RiakTSDevopsSingleHost struct {
	RiakTSDevops
}

func NewRiakTSDevopsSingleHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newRiakTSDevopsCommon(dbConfig, start, end).(*RiakTSDevops)
	return &RiakTSDevopsSingleHost{
		RiakTSDevops: *underlying,
	}
}

func (d *RiakTSDevopsSingleHost) Dispatch(_, scaleVar int) Query {
	q := NewRiakTSQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q, scaleVar)
	return q
}
