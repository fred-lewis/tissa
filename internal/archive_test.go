package internal
// Copyright (c) 2019 Fred Lewis. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import (
	"testing"
	"os"
)

func TestDataOneSecond(t *testing.T) {
	startTime := int64(1560632000)
	c := newChunk(1, startTime)
	for i := 0; i < 1000; i++ {
		vals := map[string]interface{} {
			"val": float64(i),
		}
		c.append(vals, startTime + int64(i))
	}

	d, ts := c.getData(1560632000, 1560633000)
	if len(d["val"]) != 1000 {
		t.Errorf("Data is length %d", len(d["val"]))
	}
	if len(ts) != 1000 {
		t.Errorf("Timestamps is length %d", len(d["val"]))
	}
	if ts[0] != startTime {
		t.Errorf("First timestamp is %d", ts[0])
	}
	if ts[len(ts)-1] != startTime + 999 {
		t.Errorf("Last timestamp is %d", ts[len(ts)-1])
	}

	d, ts = c.getData(1560632100, 1560632200)
	if len(d["val"]) != 100 {
		t.Errorf("Data is length %d", len(d))
	}
	if len(ts) != 100 {
		t.Errorf("Timestamps is length %d", len(d))
	}
	if ts[0] != 1560632100 {
		t.Errorf("First timestamp is %d", ts[0])
	}
	if ts[len(ts)-1] != 1560632199 {
		t.Errorf("Last timestamp is %d", ts[len(ts)-1])
	}
}

func TestDataFiveSecond(t *testing.T) {
	startTime := int64(1560632000)
	c := newChunk(5, startTime)
	for i := 0; i < 1000; i++ {
		vals := map[string]interface{} {
			"val": float64(i),
		}
		c.append(vals, startTime + int64(5 * i))
	}

	d, ts := c.getData(1560632000, 1560637000)
	if len(d["val"]) != 1000 {
		t.Errorf("Data is length %d", len(d))
	}
	if len(ts) != 1000 {
		t.Errorf("Timestamps is length %d", len(d))
	}
	if ts[0] != startTime {
		t.Errorf("First timestamp is %d", ts[0])
	}
	if ts[len(ts)-1] != startTime + 4995 {
		t.Errorf("Last timestamp is %d", ts[len(ts)-1])
	}

	d, ts = c.getData(1560632100, 1560632200)
	if len(d["val"]) != 20 {
		t.Errorf("Data is length %d", len(d))
	}
	if len(ts) != 20 {
		t.Errorf("Timestamps is length %d", len(d))
	}
	if ts[0] != 1560632100 {
		t.Errorf("First timestamp is %d", ts[0])
	}
	if ts[len(ts)-1] != 1560632195 {
		t.Errorf("Last timestamp is %d", ts[len(ts)-1])
	}
}

func TestDataFiveSecondGap(t *testing.T) {
	startTime := int64(1560632000)
	c := newChunk(5, startTime)
	c.append(map[string]interface{} { "val": 100.0 }, startTime)
	c.append(map[string]interface{} { "val": 200.0 }, startTime+100)

	d, ts := c.getData(1560632000, 1560632105)
	if len(d["val"]) != 21 {
		t.Errorf("Data is length %d", len(d))
	}
	if len(ts) != 21 {
		t.Errorf("Timestamps is length %d", len(d))
	}
	if ts[0] != startTime {
		t.Errorf("First timestamp is %d", ts[0])
	}
	if ts[len(ts)-1] != startTime+100 {
		t.Errorf("Last timestamp is %d", ts[len(ts)-1])
	}
}

func TestArchiveRollover(t *testing.T) {
	os.RemoveAll("/tmp/archive_test")
	os.Mkdir("/tmp/archive_test", os.ModePerm)
	os.Mkdir("/tmp/archive_test/a", os.ModePerm)
	a := NewArchive("/tmp/archive_test/a", 1, 3600, 600)
	startTime := int64(1560632000)
	for i := 0; i < 6000; i++ {
		a.Append(map[string]interface{} { "val": float64(i) }, startTime + int64(i))
	}

	d, ts := a.GetData(1560634000, 1560637000)
	a.Write()

	if len(d["val"]) != 3000 {
		t.Errorf("Data length is %d", len(d))
	}
	if len(ts) != 3000 {
		t.Errorf("Timestamp length is %d", len(d))
	}
	if d["val"][0] != 2000.0 {
		t.Errorf("Expected 2000: %f", d["val"][0])
	}
	if d["val"][len(d["val"]) - 1] != 4999.0 {
		t.Errorf("Expected 4999: %f", d["val"][len(d["val"]) - 1])
	}

	a, err := OpenArchive("/tmp/archive_test/a")
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	d, ts = a.GetData(1560634000, 1560637000)
	if len(d["val"]) != 3000 {
		t.Errorf("Data length is %d", len(d))
	}
	if len(ts) != 3000 {
		t.Errorf("Timestamp length is %d", len(d))
	}
	if d["val"][0] != nil {
		t.Errorf("Expected 0.0: %f", d["val"][0])
	}
	if d["val"][len(d["val"]) - 1] != 4999.0 {
		t.Errorf("Expected 4999: %f", d["val"][len(d["val"]) - 1])
	}
	if d["val"][799] != nil || d["val"][800] == nil {
		t.Errorf("Retention didn't work: %f, %f", d["val"][799], d["val"][800])
	}
}

func TestFillIns(t *testing.T) {
	os.RemoveAll("/tmp/archive_test")
	os.Mkdir("/tmp/archive_test", os.ModePerm)
	os.Mkdir("/tmp/archive_test/a", os.ModePerm)

	a := NewArchive("/tmp/archive_test/a", 5, 10000, 600)

	a.Append(map[string]interface{} { "val": 100.0 }, 1560632000)
	a.Append(map[string]interface{} { "val": 100.0 },  1560637800)
	a.Write()

	a, err := OpenArchive("/tmp/archive_test/a")
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	d, ts := a.GetData(1560634000, 1560638000)

	if len(d["val"]) != 800 {
		t.Errorf("Data length is %d", len(d))
	}
	if len(ts) != 800 {
		t.Errorf("Timestamp length is %d", len(d))
	}
	if ts[0] != 1560634000 {
		t.Errorf("Excpected 1560634000: %d", ts[0])
	}
	if ts[len(ts) - 1] != 1560637995 {
		t.Errorf("Excpected 1560637995: %d", ts[len(ts) - 1])
	}
	if d["val"][0] != nil {
		t.Errorf("Expected 0: %f", d["val"][0])
	}
	if d["val"][760] != 100.0 {
		t.Errorf("Expected 100: %f", d["val"][759])
	}
}
