package tissa

// Copyright (c) 2019 Fred Lewis. All rights reserved.
// Use of this source code is governed by a MIT license found in the LICENSE file.

import (
	"github.com/fred-lewis/tissa/internal"
	"fmt"
	"sort"
	"path/filepath"
	"os"
	"github.com/ugorji/go/codec"
	"time"
)

//
// A TimeSeries can track multiple key/value pairs over time.
// TimeSeries are append-only,and data can be rolled up to a
// number of loawer-resolution archives.
//
type TimeSeries struct {
	archives    []*internal.Archive
	config      TimeSeriesConfig
	LastWritten int64
}

//
// One or more ArchiveConfigs is required. DefaultValue is the value
// to use for missing data.
//
type TimeSeriesConfig struct {
	Archives []ArchiveConfig
	DefaultValue float64
}

// Resolution and retention specified in seconds.  Use
// package-level enum (SECOND, TEN_SECOND ...) to ensure
// all resolutions divide evenly
type ArchiveConfig struct {
	Resolution int64
	Retention  int64
}

// Each divisible by all priors
const (
	SECOND int64 = 1
	TEN_SECOND = 10
	THIRTY_SECOND = 30
	MINUTE = 60
	FIVE_MINUTE = 5 * MINUTE
	TEN_MINUTE = 10 * MINUTE
	THIRTY_MINUTE = 30 * MINUTE
	HOUR = 60 * MINUTE
	SIX_HOUR = 6 * HOUR
	TWELVE_HOUR = 12 * HOUR
	DAY = 24 * HOUR
)

const chunkSizeSlots = 2000


//
// Construct a new TimeSeries in the given directory, with the given configuration
//
// At least one archive must be provided.  If multiple archives are provided,
// each resolution must be a multiple of the last, and rollups will be
// populated automatically when new data is inserted.
//
func NewTimeSeries(dir string, config TimeSeriesConfig) (*TimeSeries, error) {
	if config.Archives == nil || len(config.Archives) == 0 {
		return nil, fmt.Errorf("config must specify at least one archive")
	}

	sort.Slice(config.Archives, func(i, j int) bool {
		return config.Archives[i].Resolution < config.Archives[j].Resolution
	})

	os.Mkdir(dir, 0700)
	series := TimeSeries{
		config: config,
	}

	series.archives = make([]*internal.Archive, len(config.Archives))
	last := int64(1)
	for i, a := range config.Archives {
		if a.Resolution % last != 0 {
			return nil, fmt.Errorf("each archive resolution must be divisible by all smaller ones")
		}
		last = a.Resolution

		fp := filepath.Join(dir, fmt.Sprintf("%d", a.Resolution))
		err := os.Mkdir(fp, 0700)
		if err != nil {
			return nil, err
		}
		series.archives[i] = internal.NewArchive(fp, a.Resolution, a.Retention, chunkSizeSlots * a.Resolution)
		series.archives[i].Write()
	}

	fp := filepath.Join(dir, "config")
	err := writeObject(fp, config)
	if err != nil {
		return nil, err
	}

	return &series, nil
}

//
//  Open an existing TimeSeries in the given directory
//
func OpenTimeSeries(dir string) (*TimeSeries, error) {
	fp := filepath.Join(dir, "config")

	var config TimeSeriesConfig
	err := readObject(fp, &config)
	if err != nil {
		return nil, err
	}

	series := TimeSeries{
		config: config,
	}

	series.archives = make([]*internal.Archive, len(config.Archives))
	for i, a := range config.Archives {
		fp := filepath.Join(dir, fmt.Sprintf("%d", a.Resolution))
		series.archives[i], err = internal.OpenArchive(fp)
	}

	return &series, nil
}


//
// Add a single key-value pair to the timeseries with the given
// timestamp.  Timeseries are append-only.  Timestamps will be
// normalized to a multiple of the TimeSeries' base resolution.
//
func (t *TimeSeries) AddValue(key string, val float64, timestamp int64) error {
	valMap := make(map[string]float64, 0)
	valMap[key] = val
	return t.AddValues(valMap, timestamp)
}


//
// Add multiple key-value pairs for the given timestamp. Timestamps
// will be normalized to a multiple of the TimeSeries' base resolution.
//
func (t *TimeSeries) AddValues(vals map[string]float64, timestamp int64) error {
	curArchive := t.baseArchive()
	lastTimestamp := curArchive.EndTime

	convertedMap := make(map[string]interface{}, len(vals))
	for k, v := range vals {
		convertedMap[k] = v
	}

	curArchive.Append(convertedMap, timestamp)

	for i := 1; i < len(t.archives); i++ {
		rollupArchive := t.archives[i]
		rollupIval := rollupArchive.Interval

		if (timestamp / rollupIval) == (lastTimestamp / rollupIval) {
			// done rolling up
			break
		}

		rollupStart := timestamp - (timestamp % rollupIval) - rollupIval
		rollupEnd := rollupStart + rollupIval

		data, _ := curArchive.GetData(rollupStart, rollupEnd)

		var rollups map[string]interface{}
		if i == 1 {
			rollups = rollupRawData(data)
		} else {
			rollups = rollupRollupData(data)
		}

		rollupArchive.Append(rollups, rollupEnd)
		curArchive = rollupArchive
	}

	return nil
}


//
//  Retrieve the latest key-value pairs
//
func (t *TimeSeries) Latest() (val map[string]float64, timestamp int64) {
	d, ts := t.baseArchive().Latest()
	res := make(map[string]float64, len(d))
	for k, v := range d {
		res[k] = v.(float64)
	}
	return res, ts
}

//
//  For querying rollup archives.  Returns average value series for all keys.
//
func (t *TimeSeries) Averages(startTime, endTime, resolution int64) (map[string][]float64, []int64, error) {
	return t.walkData(startTime, endTime, resolution, func(r Rollup) float64 {
		if r.Count > 0 {
			return r.Total / float64(r.Count)
		}
		return 0.0
	})
}

//
//  For querying rollup archives.  Returns maximum value series for all keys.
//
func (t *TimeSeries) Maximums(startTime, endTime, resolution int64) (map[string][]float64, []int64, error) {
	return t.walkData(startTime, endTime, resolution, func(r Rollup) float64 {
		if r.Count > 0 {
			return r.Max
		}
		return 0.0
	})
}

//
//  For querying rollup archives.  Returns minimums value series for all keys.
//
func (t *TimeSeries) Minimums(startTime, endTime, resolution int64) (map[string][]float64, []int64, error) {
	return t.walkData(startTime, endTime, resolution, func(r Rollup) float64 {
		if r.Count > 0 {
			return r.Min
		}
		return 0.0
	})
}

//
//  For querying raw daa from rollup archives.
//
func (t *TimeSeries) Rollups(startTime, endTime, resolution int64) (map[string][]Rollup, []int64, error) {
	if resolution == t.baseArchive().Interval {
		//TODO
		return nil, nil, fmt.Errorf("cannot get rollups from base archive")
	}

	var archive *internal.Archive = nil
	for i := 1; i < len(t.archives); i++ {
		if t.archives[i].Interval == resolution {
			archive = t.archives[i]
		}
	}
	if archive == nil {
		return nil, nil, fmt.Errorf("no matching archive")
	}

	data, ts := archive.GetData(startTime, endTime)
	vals := make(map[string][]Rollup, len(data))
	for k, v := range data {
		vals[k] = make([]Rollup, len(v))
		for i, d := range v {
			if d != nil {
				vals[k][i] = d.(Rollup)
			} else {
				vals[k][i] = Rollup{}
			}
		}
	}

	return vals, ts, nil
}

//
// Write the TimeSeries to disk, and exercise retention (delete any
// chunks that are fully expired).
//
func (t *TimeSeries) Write() error {
	for _, a := range t.archives {
		err := a.Write()
		if err != nil {
			return err
		}
	}
	t.LastWritten = time.Now().Unix()
	return nil
}

func (t *TimeSeries) walkData(startTime, endTime, resolution int64,
	rollupHandler func(Rollup) float64) (map[string][]float64, []int64, error)  {

	l := (endTime - startTime) / resolution
	if (endTime - startTime) % resolution > 0 {
		l++
	}

	if resolution == t.baseArchive().Interval {
		idata, ts := t.baseArchive().GetData(startTime, endTime)
		vals := make(map[string][]float64, len(idata))
		for k, v := range idata {
			vals[k] = make([]float64, l)
			for i, d := range v {
				if d != nil {
					vals[k][i] = d.(float64)
				} else {
					vals[k][i] = t.config.DefaultValue
				}
			}
		}
		return vals, ts, nil
	} else {
		rdata, ts, err := t.Rollups(startTime, endTime, resolution)
		if err != nil {
			return nil, nil, err
		}
		vals := make(map[string][]float64, len(rdata))
		for k, v := range rdata {
			vals[k] = make([]float64, l)
			for i, d := range v {
				vals[k][i] = rollupHandler(d)
			}
		}
		return vals, ts, nil
	}
}

type Rollup struct {
	Total float64
	Count int64
	Min   float64
	Max   float64
}

func rollupRawData(data map[string][]interface{}) map[string]interface{} {
	res := make(map[string]interface{}, len(data))

	for k, v := range data {
		r := Rollup{}
		first := true
		for _, val := range v {
			if val != nil {
				r.Count++
				r.Total += val.(float64)
				if first || val.(float64) > r.Max {
					r.Max = val.(float64)
				}
				if first || val.(float64) < r.Min {
					r.Min = val.(float64)
				}
				first = false
			}
		}
		res[k] = r
	}

	return res
}

func rollupRollupData(data map[string][]interface{}) map[string]interface{} {
	res := make(map[string]interface{}, len(data))

	for k, v := range data {
		r := Rollup{}
		first := true
		for _, val := range v {
			if val != nil {
				rVal := val.(Rollup)
				r.Count += rVal.Count
				r.Total += rVal.Total

				if first || rVal.Max > r.Max {
					r.Max = rVal.Max
				}
				if first || rVal.Min < r.Min {
					r.Min = rVal.Min
				}
				first = false
			}
		}
		res[k] = r
	}
	return res
}

func (t *TimeSeries) baseArchive() *internal.Archive {
	return t.archives[0]
}

var mph = codec.MsgpackHandle{}

func writeObject(filePath string, obj interface{}) error {
	file, err := os.Create(filePath)
	if err == nil {
		enc := codec.NewEncoder(file, &mph)
		err = enc.Encode(obj)
	}
	file.Close()
	return err
}

func readObject(filePath string, v interface{}) error {
	file, err := os.Open(filePath)
	if err == nil {
		dec := codec.NewDecoder(file, &mph)
		err = dec.Decode(&v)
	}
	file.Close()
	return err
}
