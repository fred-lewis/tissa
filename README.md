##Tissa
#### Time Series Storage Archive

Package tissa provides a simple embedded time series storage engine.

A Tissa TimeSeries can track a number of key-value pairs over time.
When declaring a timeseries, you can specify one or more archives,
each with its own granularity and retention.  Rollups are done
automatically upon insertion of new data.  Unlike RRDs, keys
need not be static over the lifecycle of a timeseries.  This
makes Tissa work well for tracking entities that come and go.

Tissa supports persistence, but only when Write() is called.  There's no
WAL or immediate consistency. Tissa tracks data in "chunks", of (default)
2000 intervals, and all reads and writes are of an entire chunk.  So,
while you can call Write() as often as you like, do realize that each
write call writes a full chunk. Retentions are also exercised whenever
Write() is called.  Chunks that are fully beyond the retention time are
deleted.

##### Things to know:

- Tissa normalizes datapoints as they are added.  Timestamps are aligned to
interval boundaries, and any missing intervals are filled in with the
specified defaul value
- The latest chunk of each archive is cached in-memory

Example:
```
tsc := TimeSeriesConfig{
	Archives: []ArchiveConfig{
		{SECOND, HOUR}, // retain 1-second data for 1 hour
		{MINUTE, DAY}, // retain 1-minute rollups for 1 day
	},
	DefaultValue: 0,
}

os.RemoveAll("/tmp/my_timeseries")
ts, _ := NewTimeSeries("/tmp/my_timeseries", tsc)

t := int64(1560632038)
ts.AddValue("thing1", 100.0, t)
ts.AddValue("thing2", 200.0, t)

t++
ts.AddValue("thing1", 200.0, t)
ts.AddValue("thing2", 300.0, t)

t += 5
ts.AddValue("thing1", 300.0, t)
ts.AddValue("thing2", 400.0, t)
```


The last timestamp increment crosses a minute boundary and incurs rollups
for the minute ending 1560632040.

```
averages, timestamps, _ := ts.Averages(1560632038, t+1, SECOND)
fmt.Printf("%+v\n%+v\n", timestamps, averages)
```
Output:
```
[1560632038 1560632039 1560632040 1560632041 1560632042 1560632043 1560632044]
map[thing1:[100 200 0 0 0 0 300] thing2:[200 300 0 0 0 0 400]]
```

```
averages, timestamps, _ = ts.Averages(1560632038, t+1, MINUTE)
fmt.Printf("%+v\n%+v\n", timestamps, averages)
```
Output:
```
[1560632040]
map[thing1:[150] thing2:[250]]
```