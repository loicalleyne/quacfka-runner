Quacfka-Runner 🏹🦆
===================

Run queries in DuckDB files emitted by Quacfka-Service.
Walks output directory and uploads to Google Cloud Storage.

## 💡 Usage

```bash
usage: quafka-runner [<flags>]

Flags:
      --[no-]help        Show context-sensitive help (also try --help-long and --help-man).
  -r, --address="./gorpc-sock.unix"
                         socket address ($RUNNER_SOCKET)
      --config="./config/server.toml"
                         Path to config
  -p, --parquetpath="./parquet/"
                         path to parquets ($PARQUET_PATH)
  -b, --bucket="bucket"  GCS bucket name ($GCS_BUCKET)
```

## QuafkaRunner RPC 
Runner will use a Unix socket on Linux and TCP socket on other platforms. 
Set TCP address and port in config TOML file.
```toml
[rpc]
host = "127.0.0.1"
port = 9090
```
### RPC Usage
```go
	gorpc.RegisterType(rpc.Request{})
	gorpc.RegisterType(rpc.Response{})
    addr = "./gorpc-sock.unix"
	client := gorpc.NewUnixClient(addr)
	client.Start()

	// partition query
	partitionQuery := `select
			datepart('year', epoch_ms(timestamp.seconds * 1000))::STRING as year,
			datepart('month', epoch_ms(timestamp.seconds * 1000))::STRING as month,
			datepart('day', epoch_ms(timestamp.seconds * 1000))::STRING as day,
			datepart('hour', epoch_ms(timestamp.seconds * 1000))::STRING as hour
		from bidreq
		group by all
		ORDER BY 1,2,3,4`
	// export_raw.sql
	rawQuery := `COPY (
		SELECT *
		FROM bidreq
		WHERE
		datepart('year', epoch_ms(((timestamp.seconds * 1000) + (timestamp.nanos/1000000))::BIGINT)) = {{year}}
		and datepart('month', epoch_ms(((timestamp.seconds * 1000) + (timestamp.nanos/1000000))::BIGINT)) = {{month}}
		and datepart('day', epoch_ms(((timestamp.seconds * 1000) + (timestamp.nanos/1000000))::BIGINT)) = {{day}}
		and datepart('hour', epoch_ms(((timestamp.seconds * 1000) + (timestamp.nanos/1000000))::BIGINT)) = {{hour}} ) TO '{{exportpath}}/{{logname}}/{{queryname}}/year={{year}}/month={{month}}/day={{day}}/hour={{hour}}/bidreq_raw_{{rand}}.parquet' (format PARQUET, compression zstd, ROW_GROUP_SIZE_BYTES 100_000_000, OVERWRITE_OR_IGNORE)`
	hourlyRequestsAggQuery := `COPY (
			select
			datetrunc('day', epoch_ms(event_time*1000))::DATE date,
			extract('hour' FROM epoch_ms(event_time*1000)) as hour,
			bidreq_norm.pub_id,
			bidreq_norm.device_id,
			CONCAT(width::string, 'x', height::string) resolution,
			deal,
			count(distinct bidreq_id) requests,
			from bidreq_norm
			where
			datepart('year', epoch_ms(event_time * 1000)) = {{year}}
			and datepart('month', epoch_ms(event_time * 1000)) = {{month}}
			and datepart('day', epoch_ms(event_time * 1000)) = {{day}}
			and datepart('hour', epoch_ms(event_time * 1000)) = {{hour}}
			group by all)
			TO '{{exportpath}}/{{logname}}/{{queryname}}/year={{year}}/month={{month}}/day={{day}}/hour={{hour}}/bidreq_hourly_requests_agg_{{rand}}.parquet' (format PARQUET, compression zstd, ROW_GROUP_SIZE_BYTES 100_000_000, OVERWRITE_OR_IGNORE)`
	logName := "ortb.bid-requests"
	queries := []string{rawQuery, hourlyRequestsAggQuery}
	queriesNames := []string{"raw", "hourly_requests_agg"}
	execQueries := []string{"SET threads = 32", "SET allocator_background_threads = true"}
	execQueriesNames := []string{"", ""}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for dPath := range o.DuckPaths() {
			path, err := filepath.Abs(dPath)
			if err != nil {
				log.Printf("dPath error: %v\n", err)
			}
            // Requests should be validated first with Call, then the request in the validated Response sent with Send.
			resp, err := client.Call(rpc.Request{
				Type:             rpc.REQUEST_VALIDATE,
				Path:             path,
				ExportPath:       *parquetPath,
				LogName:          logName,
				ExecQueries:      execQueries,
				ExecQueriesNames: execQueriesNames,
				PartitionQuery:   partitionQuery,
				Queries:          queries,
				QueriesNames:     queriesNames,
			})
			if err != nil {
				log.Printf(" runner request issue: %v\n", err)
			} else {
				req := resp.(rpc.Response).Request
				req.Type = rpc.REQUEST_RUN
				err = client.Send(req)
				if err != nil {
					log.Printf("rpc send error: %v\n", err)
				}
			}
			d := 0
			for i := 0; i <= 60; i++ {
				c, _ := dbFileCount("./")
				if c > 3 {
					delay := time.NewTimer(1 * time.Second)
					<-delay.C
					d++
				} else {
					if d > 0 {
						log.Printf("delayed by %d sec\n", d)
					}
					break
				}
			}
		}
	}()
```

### Message schema
```go
// The type Request is a struct containing an rpc request.
// Requests are sent to the server by the client.
type Request struct {
	// Type is an integer that contains the type of request
	// that is being sent from the client to the server.
	Type             int
	// Path to DuckDB file
	Path             string
	// Placeholder {{logname}} in Queries is replaced with this value.
	LogName          string
	// Placeholder {{exportpath}} in Queries is replaced with this value.
	ExportPath       string
	// Queries where no result is expected, or not requiring partitioning by year, month, day, hour.
	ExecQueries      []string
	// Names of exec queries. ExecQueries and ExecQueriesNames must have matching lengths.
	ExecQueriesNames []string
	// Query to retrieve values for partition by year, month, day, hour.
	PartitionQuery   string
	// Queries partitioned by year, month, day, hour.
	// Placeholders {{year}}, {{month}}, {{day}}, {{hour}} in queries will be replaced by values returned by PartitionQuery.
	Queries          []string
	// Names of queries with partitioning. Queries and QueriesNames length must match.
	QueriesNames     []string
}
```

## 💫 Show your support

Give a ⭐️ if this project helped you!
Feedback and PRs welcome.

## Licence

Quacfka-Runner is released under the Apache 2.0 license. See [LICENCE](LICENCE)