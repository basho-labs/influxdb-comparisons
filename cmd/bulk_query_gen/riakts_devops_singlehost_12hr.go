package main

import "time"

// RiakTSDevopsSingleHost12hr produces RiakTS-specific queries for the devops single-host case.
type RiakTSDevopsSingleHost12hr struct {
	RiakTSDevops
}

func NewRiakTSDevopsSingleHost12hr(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newRiakTSDevopsCommon(dbConfig, start, end).(*RiakTSDevops)
	return &RiakTSDevopsSingleHost12hr{
		RiakTSDevops: *underlying,
	}
}

func (d *RiakTSDevopsSingleHost12hr) Dispatch(_, scaleVar int) Query {
	q := NewRiakTSQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q, scaleVar)
	return q
}
