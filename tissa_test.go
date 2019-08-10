package tissa

import (
	"testing"
	"os"
	"fmt"
)

func TestBasicTimeSeries(t *testing.T) {
	os.RemoveAll("/tmp/timeseries_test")
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
	d, stamps, err := ts.Averages(startTime, startTime + 6000, MINUTE)

	for i, dp := range d["val"] {
		fmt.Printf("%d: %f\n", stamps[i], dp)
	}

	d, stamps, err = ts.Maximums(startTime, startTime + 6000, MINUTE)

	for i, dp := range d["val"] {
		fmt.Printf("%d: %f\n", stamps[i], dp)
	}

	d, stamps, err = ts.Averages(startTime, startTime + 6000, HOUR)

	for i, dp := range d["val"] {
		fmt.Printf("%d: %f\n", stamps[i], dp)
	}
}
