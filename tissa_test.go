package tissa
// Copyright (c) 2019 Fred Lewis. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import (
	"testing"
	"os"
)

func TestBasicTimeSeries(t *testing.T) {
	os.RemoveAll("/tmp/timeseries_test/a")
	os.Mkdir("/tmp/timeseries_test", os.ModePerm)
	os.Mkdir("/tmp/timeseries_test/a", os.ModePerm)

	tsc := TimeSeriesConfig{
		Archives: []ArchiveConfig{
			{SECOND, HOUR},
			{MINUTE, DAY},
			{HOUR, 30 * DAY},
		},
	}

	ts, err := NewTimeSeries("/tmp/timeseries_test/a", tsc)
	if err != nil {
		t.Fatalf(err.Error())
	}

	startTime := int64(1560632000)

	for i := 0; i < 6000; i++ {
		if i % 100 == 0 {
			continue
		}
		ts.AddValues(map[string]float64{ "val": float64(i)}, startTime + int64(i))
	}
	d, stamps, err := ts.Averages(startTime, startTime + 6000, SECOND)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(d["val"]) != 6000 {
		t.Errorf("Data is length %d", len(d["val"]))
	}

	// when missing a single timestamp, we copy the last value forward
	if d["val"][100] != 99 {
		t.Errorf("Data[100] is %f", d["val"][100])
	}

	if d["val"][101] != 101 {
		t.Errorf("Data[101] is %f", d["val"][101])
	}

	if stamps[100] != int64(1560632100) {
		t.Errorf("Timestamp[100] is %d", stamps[100])
	}

	d, stamps, err = ts.Averages(startTime, startTime + 6000, MINUTE)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if d["val"][0] != 20.0 {
		t.Errorf("Minute Data[0] is %f", d["val"][0])
	}

	if d["val"][5] != 309.4833333333333333 {
		t.Errorf("Minute Data[5] is %f", d["val"][5])
	}

	d, stamps, err = ts.Maximums(startTime, startTime + 6000, MINUTE)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if d["val"][0] != 39 {
		t.Errorf("Minute Max Data[0] is %f", d["val"][0])
	}

	if d["val"][5] != 339 {
		t.Errorf("Minute Max Data[5] is %f", d["val"][5])
	}

	if stamps[5] != int64(1560632340) {
		t.Errorf("Timestamp[5] is %d", stamps[5])
	}
}

func TestTimeSeriesExample(tst *testing.T) {
	os.RemoveAll("/tmp/timeseries_test/b")
	os.Mkdir("/tmp/timeseries_test", os.ModePerm)
	os.Mkdir("/tmp/timeseries_test/b", os.ModePerm)

	tsc := TimeSeriesConfig{
		Archives: []ArchiveConfig{
			{SECOND, HOUR},
			{MINUTE, DAY},
		},
		DefaultValue: 0,
	}

	ts, _ := NewTimeSeries("/tmp/timeseries_test/b", tsc)

	t := int64(1560632038)
	ts.AddValue("thing1", 100.0, t)
	ts.AddValue("thing2", 200.0, t)

	t++
	ts.AddValue("thing1", 200.0, t)
	ts.AddValue("thing2", 300.0, t)

	t += 5 // crosses minute boundary, and incurs rollups
	ts.AddValue("thing1", 300.0, t)
	ts.AddValue("thing2", 400.0, t)

	averages, timestamps, err := ts.Averages(1560632038, t+1, SECOND)
	if err != nil {
		tst.Fatalf(err.Error())
	}

	if averages["thing1"][0] != 100 {
		tst.Errorf("Second Avg Data[thing1][0] is %f", averages["thing1"][0])
	}

	if averages["thing2"][1] != 300 {
		tst.Errorf("Second Avg Data[thing2][1] is %f", averages["thing2"][1])
	}

	if averages["thing2"][6] != 400 {
		tst.Errorf("Second Avg Data[thing2][6] is %f", averages["thing2"][6])
	}

	averages, timestamps, _ = ts.Averages(1560632038, t+1, MINUTE)
	if err != nil {
		tst.Fatalf(err.Error())
	}

	if len(timestamps) != 1 || timestamps[0] != 1560632040 {
		tst.Errorf("Timestamps is %+v", timestamps)
	}

	if averages["thing2"][0] != 250 {
		tst.Errorf("Minute Avg Data[thing2][0] is %f", averages["thing2"][0])
	}
}
