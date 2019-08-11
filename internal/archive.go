package internal
// Copyright (c) 2019 Fred Lewis. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import (
	"path/filepath"
	"fmt"
	"sync"
	"os"
	"github.com/ugorji/go/codec"
)

type Archive struct {
	Interval    int64
	ChunkSize   int64
	Dir         string
	Retention   int64
	StartTime   int64
	EndTime     int64
	chunks      []*chunk
	mu          sync.Mutex
	lastWrite   int64
}

func NewArchive(dirPath string, interval, retention, chunkSize int64) *Archive {
	return &Archive{
		Interval: interval,
		ChunkSize: chunkSize,
		Dir: dirPath,
		Retention: retention,
	}
}

func OpenArchive(dirPath string) (*Archive, error) {
	var archive Archive
	fp := filepath.Join(dirPath, "archive")
	err := readObject(fp, &archive)
	if err != nil {
		return nil, err
	}
	if archive.EndTime > 0 {
		var lastChunk chunk
		lastChunkTs := archive.chunkStart(archive.EndTime)
		fp = filepath.Join(dirPath, fmt.Sprintf("%d", lastChunkTs))
		err = readObject(fp, &lastChunk)
		if err != nil {
			return nil, err
		}
		archive.chunks = []*chunk{ &lastChunk }
	}
	return &archive, nil
}

func (a *Archive) Write() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.EndTime > a.lastWrite {
		for _, c := range a.chunks {
			if c.dirty {
				fp := filepath.Join(a.Dir,
					fmt.Sprintf("%d", a.chunkStart(c.StartTime)))
				err := writeObject(fp, c)
				if err != nil {
					return err
				}
			}
		}
		a.chunks = []*chunk{ a.lastChunk() }
		a.exerciseRetention()
	}
	a.lastWrite = a.EndTime
	writeObject(filepath.Join(a.Dir, "archive"), a)
	return nil
}

func (a *Archive) Append(val map[string]interface{}, timestamp int64) {
	timestamp = a.tsNorm(timestamp)
	a.mu.Lock()
	defer a.mu.Unlock()
	lc := a.lastChunk()
	if lc == nil {
		startChunk := timestamp - (timestamp % a.ChunkSize)
		lc = newChunk(a.Interval, startChunk)
		a.chunks = []*chunk{lc}
	} else if a.boundaryCheck(timestamp) {
		nextStart := a.chunkStart(timestamp)
		if lc.EndTime < a.chunkEnd(lc.StartTime) {
			lc.fillTo(nextStart)
		}
		lc = newChunk(a.Interval, nextStart)
		a.chunks = append(a.chunks, lc)
	}
	lc.append(val, timestamp)
	a.EndTime = timestamp
	if a.StartTime == 0 {
		a.StartTime = timestamp
	}
}

func (a *Archive) Latest() (map[string]interface{}, int64) {
	lc := a.lastChunk()
	if lc == nil {
		return nil, 0
	}
	return lc.latest(), lc.EndTime
}

func (a *Archive) GetData(startTime, endTime int64) (map[string][]interface{}, []int64) {
	startTime = a.tsNorm(startTime)
	endTime = a.tsNorm(endTime)

	l := (endTime - startTime) / a.Interval

	chunkStart := a.chunkStart(startTime)
	data := make(map[string][]interface{}, 0)
	stamps := make([]int64, l)

	t := startTime
	for i := int64(0); i < l; i++ {
		stamps[i] = t
		t += a.Interval
	}

	i := int64(0)
	for chunkStart < endTime {
		cStart := chunkStart
		cEnd := chunkStart + a.ChunkSize
		if cStart < startTime {
			cStart = startTime
		}
		if cEnd > endTime {
			cEnd = endTime
		}
		chunk, err := a.getChunkByStartTime(chunkStart)
		if err == nil {
			chunkData, _ := chunk.getData(cStart, cEnd)
			for key, ticks := range chunkData {
				exst, ok := data[key]
				if !ok {
					exst = make([]interface{}, l)
					data[key] = exst
				}
				for j := 0; j < len(ticks); j++ {
					exst[i + int64(j)] = ticks[j]
				}
			}
		}
		i += (cEnd - cStart) / a.Interval
		chunkStart += a.ChunkSize
	}

	return data, stamps
}

func (a *Archive) getChunkByStartTime(ts int64) (*chunk, error) {
	for _, c := range(a.chunks) {
		if c.StartTime == ts {
			return c, nil
		}
	}
	var c chunk
	fp := filepath.Join(a.Dir, fmt.Sprintf("%d", ts))
	err := readObject(fp, &c)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

//
// Round up to the nearest resolution.
//
func (a *Archive) tsNorm(timestamp int64) int64 {
	ts := timestamp - (timestamp % a.Interval)
	if ts < timestamp {
		ts += a.Interval
	}
	return ts
}

func (a *Archive) exerciseRetention() {
	for a.EndTime - a.StartTime > a.Retention {
		fp := filepath.Join(a.Dir,
			fmt.Sprintf("%d", a.chunkStart(a.StartTime)))
		os.Remove(fp)
		a.StartTime = a.chunkStart(a.StartTime) + a.ChunkSize
	}
}

func (a *Archive) lastChunk() *chunk {
	l := len(a.chunks)
	if l > 0 {
		return a.chunks[l - 1]
	}
	return nil
}

func (a *Archive) chunkStart(ts int64) int64 {
	return ts - (ts % a.ChunkSize)
}

func (a *Archive) chunkEnd(ts int64) int64 {
	return a.chunkStart(ts) + a.ChunkSize - a.Interval
}


func (a *Archive) boundaryCheck(ts int64) bool {
	c := a.lastChunk()
	return ts > a.chunkEnd(c.StartTime)
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

//
//
// Chunks are for the most granular data.
// We normalize the data so all timestamps align
// to resolution, and there are no missing ticks
// (we proactively fill in missing data).  This
// way, we don't have to store timestamps with
// every datapoint.
//
//

type chunk struct {
	StartTime   int64
	EndTime     int64
	Resolution  int64
	Data        []map[int]interface{}
	Tags        []string
	tagMap      map[string]int
	dirty       bool
}

func (c *chunk) fillTo(timestamp int64) {
	numToFill := (timestamp - c.EndTime) / c.Resolution

	ts := c.EndTime + c.Resolution
	//
	// For short periods (a couple seconds), we copy the latest
	// tick forward. For longer ones, we use a special value to
	// indicate missing data.
	//
	var fillVal map[int]interface{} = nil
	if numToFill < 3 {
		fillVal = c.latestRaw()
	}
	for ts < timestamp {
		c.Data = append(c.Data, fillVal)
		c.EndTime = ts
		c.dirty = true
		ts += c.Resolution
	}
}

func (c *chunk) empty() bool {
	return c.EndTime == 0
}

func (c *chunk) latestRaw() map[int]interface{} {
	l := len(c.Data)
	if l == 0 {
		return nil
	}
	return c.Data[l - 1]
}

func (c *chunk) latest() map[string]interface{} {
	latestRaw := c.latestRaw()
	if latestRaw == nil {
		return nil
	}
	ret := make(map[string]interface{}, len(latestRaw))
	for i, v := range latestRaw {
		tag := c.Tags[i]
		ret[tag] = v
	}
	return ret
}

func (c *chunk) append(val map[string]interface{}, timestamp int64) {
	if timestamp < c.EndTime {
		return
	}

	if timestamp == c.EndTime {
		latest := c.Data[len(c.Data) - 1]
		for k, v := range c.derefTags(val) {
			latest[k] = v
		}
		return
	}

	if !c.empty() && timestamp > c.EndTime + c.Resolution {
		c.fillTo(timestamp)
	}

	if c.empty() && timestamp != c.StartTime {
		ts := c.StartTime
		for ts < timestamp {
			c.Data = append(c.Data, nil)
			ts += c.Resolution
		}
	}

	c.Data = append(c.Data, c.derefTags(val))
	c.EndTime = timestamp
	c.dirty = true
}

func (c *chunk) getData(startTime int64, endTime int64) (map[string][]interface{}, []int64) {
	l := int((endTime - startTime) / c.Resolution)

	data := make(map[int][]interface{})
	stamps := make([]int64, l)

	ct := startTime
	i := 0
	for ct < endTime {
		idx := c.tsIndex(ct)

		if idx >= 0 && idx < len(c.Data) && c.Data[idx] != nil {
			tick := c.Data[idx]

			for tag, val := range tick {
				ser := data[tag]
				if ser == nil {
					ser = make([]interface{}, l)
					data[tag] = ser
				}
				ser[i] = val
			}

		}

		stamps[i] = ct
		ct += c.Resolution
		i++
	}

	return c.resolveTags(data), stamps
}

func (c *chunk) resolveTags(val map[int][]interface{}) map[string][]interface{} {
	ret := make(map[string][]interface{}, len(val))
	for i, v := range val {
		tag := c.Tags[i]
		ret[tag] = v
	}
	return ret
}

func (c *chunk) derefTags(val map[string]interface{}) map[int]interface{} {
	ret := make(map[int]interface{}, len(val))
	for tag, v := range val {
		i, ok := c.tagMap[tag]
		if !ok {
			c.tagMap[tag] = len(c.Tags)
			i = len(c.Tags)
			c.Tags = append(c.Tags, tag)
		}
		ret[i] = v
	}
	return ret
}

func (c *chunk) tsIndex(ts int64) int {
	return int((ts - c.StartTime) / c.Resolution)
}

func newChunk(resolution, startTime int64) *chunk {
	return &chunk{
		StartTime: startTime,
		EndTime: 0,
		Data: make([]map[int]interface{}, 0),
		Resolution: resolution,
		Tags: make([]string, 0),
		tagMap: make(map[string]int),
		dirty: true,
	}
}
