package main

import (
	"fmt"
	"math/rand"
	"time"
)

// RiakTSDevops produces RiakTS-specific queries for all the devops query types.
type RiakTSDevops struct {
	AllInterval  TimeInterval
}

// NewRiakTSDevops makes an RiakTSDevops object ready to generate Queries.
func newRiakTSDevopsCommon(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	if !start.Before(end) {
		panic("bad time order")
	}

	return &RiakTSDevops{
		AllInterval:  NewTimeInterval(start, end),
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *RiakTSDevops) Dispatch(i, scaleVar int) Query {
	q := NewRiakTSQuery() // from pool
	devopsDispatchAll(d, i, q, scaleVar)
	return q
}

func (d *RiakTSDevops) MaxCPUUsageHourByMinuteOneHost(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*RiakTSQuery), scaleVar, 1, time.Hour)
}

func (d *RiakTSDevops) MaxCPUUsageHourByMinuteTwoHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*RiakTSQuery), scaleVar, 2, time.Hour)
}

func (d *RiakTSDevops) MaxCPUUsageHourByMinuteFourHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*RiakTSQuery), scaleVar, 4, time.Hour)
}

func (d *RiakTSDevops) MaxCPUUsageHourByMinuteEightHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*RiakTSQuery), scaleVar, 8, time.Hour)
}

func (d *RiakTSDevops) MaxCPUUsageHourByMinuteSixteenHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*RiakTSQuery), scaleVar, 16, time.Hour)
}

func (d *RiakTSDevops) MaxCPUUsageHourByMinuteThirtyTwoHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*RiakTSQuery), scaleVar, 32, time.Hour)
}

func (d *RiakTSDevops) MaxCPUUsage12HoursByMinuteOneHost(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*RiakTSQuery), scaleVar, 1, 12*time.Hour)
}

// MaxCPUUsageHourByMinuteThirtyTwoHosts populates a Query with a query that looks like:
// SELECT max(usage_user) from cpu where (hostname = '$HOSTNAME_1' or ... or hostname = '$HOSTNAME_N') and time >= '$HOUR_START' and time < '$HOUR_END' group by time(1m)
func (d *RiakTSDevops) maxCPUUsageHourByMinuteNHosts(qi Query, scaleVar, nhosts int, timeRange time.Duration) {
	interval := d.AllInterval.RandWindow(timeRange)
	nn := rand.Perm(scaleVar)[:nhosts]

	tagSets := [][]string{}
	tagSet := []string{}
	for _, n := range nn {
		hostname := fmt.Sprintf("host_%d", n)
		tag := fmt.Sprintf("hostname=%s", hostname)
		tagSet = append(tagSet, tag)
	}
	tagSets = append(tagSets, tagSet)

	humanLabel := fmt.Sprintf("RiakTS max cpu, rand %4d hosts, rand %s by 1m", nhosts, timeRange)
	q := qi.(*RiakTSQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))

	q.AggregationType = []byte("max")
	q.MeasurementName = []byte("cpu")
	q.FieldName = []byte("usage_user")

	q.TimeStart = interval.Start
	q.TimeEnd = interval.End
	q.GroupByDuration = time.Minute

	q.TagSets = tagSets
}

// MeanCPUUsageDayByHourAllHosts populates a Query with a query that looks like:
// SELECT mean(usage_user) from cpu where time >= '$DAY_START' and time < '$DAY_END' group by time(1h),hostname
func (d *RiakTSDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi Query, _ int) {
	interval := d.AllInterval.RandWindow(24*time.Hour)

	humanLabel := "RiakTS mean cpu, all hosts, rand 1day by 1hour"
	q := qi.(*RiakTSQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))

	q.AggregationType = []byte("avg")
	q.MeasurementName = []byte("cpu")
	q.FieldName = []byte("usage_user")

	q.TimeStart = interval.Start
	q.TimeEnd = interval.End
	q.GroupByDuration = time.Hour
}

//func (d *RiakTSDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi Query, _ int) {
//	interval := d.AllInterval.RandWindow(24*time.Hour)
//
//	v := url.Values{}
//	v.Set("db", d.KeyspaceName)
//	v.Set("q", fmt.Sprintf("SELECT count(usage_user) from cpu where time >= '%s' and time < '%s' group by time(1h)", interval.StartString(), interval.EndString()))
//
//	humanLabel := "RiakTS mean cpu, all hosts, rand 1day by 1hour"
//	q := qi.(*RiakTSQuery)
//	q.HumanLabel = []byte(humanLabel)
//	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
//	q.Method = []byte("GET")
//	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
//	q.Body = nil
//}
