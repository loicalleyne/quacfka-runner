package runner

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/loicalleyne/quacfka-runner/rpc"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/loicalleyne/couac"
)

func RunPartitionedQueries(d rpc.Request) error {
	if len(d.Queries) == 0 {
		return fmt.Errorf("no queries specified")
	}
	f, err := os.Stat(d.Path)
	if err != nil {
		return fmt.Errorf("file stat error %w", err)
	}
	if f.Size() < 1024*1024 {
		return fmt.Errorf("file is only %v", f.Size())
	}
	var driverPath string
	switch runtime.GOOS {
	case "darwin":
		driverPath = "/usr/local/lib/libduckdb.so.dylib"
	case "linux":
		driverPath = "/usr/local/lib/libduckdb.so"
	case "windows":
		h, _ := os.UserHomeDir()
		driverPath = h + "\\Downloads\\libduckdb-windows-amd64\\duckdb.dll"
	default:
	}
	db, err := couac.NewDuck(couac.WithPath(d.Path), couac.WithDriverPath(driverPath))
	if err != nil {
		return fmt.Errorf("db open error %w", err)
	}
	defer db.Close()
	log.Printf("opening %s\n", d.Path)
	conn, err := db.NewConnection()
	if err != nil {
		return fmt.Errorf("new conn error: %w", err)
	}
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		return fmt.Errorf("new statement error: %w", err)
	}
	defer stmt.Close()

	for i, q := range d.ExecQueries {
		tick := time.Now()
		var qname string
		if i < len(d.ExecQueriesNames) {
			qname = d.ExecQueriesNames[i]
		}
		_, err = conn.Exec(context.Background(), q)
		if err != nil {

			return fmt.Errorf("exec query %d - %s error: %s", i, qname, q)
		}
		if qname != "" {
			log.Printf("exec query %s: %9.3f secs", qname, time.Since(tick).Seconds())
		}
	}

	_, err = conn.Exec(context.Background(), "SET allocator_background_threads = true")
	if err != nil {
		return fmt.Errorf("query 0 error: %s", d.Queries[0])
	}

	// SET preserve_insertion_order=false for PARQUET writing
	_, err = conn.Exec(context.Background(), "SET preserve_insertion_order=false")
	if err != nil {
		return fmt.Errorf("query 0 error: %s", d.Queries[0])
	}

	if d.PartitionQuery != "" {
		err = stmt.SetSqlQuery(d.PartitionQuery)
		if err != nil {
			return fmt.Errorf("query set partition query error: %w", err)
		}
		tick := time.Now()
		recordReader, _, err := stmt.ExecuteQuery(context.Background())
		if err != nil {
			return fmt.Errorf("partitionquery error: %w", err)
		}
		defer recordReader.Release()
		var b bool
		for recordReader.Next() {
			record := recordReader.Record()
			if !b {
				log.Printf("partitionquery : %v secs - %v results\n", time.Since(tick).Seconds(), record.NumRows())
				b = true
			}
			for i := 0; i < int(record.NumRows()); i++ {
				year := record.Column(0).(*array.String).ValueStr(i)
				month := record.Column(1).(*array.String).ValueStr(i)
				day := record.Column(2).(*array.String).ValueStr(i)
				hour := record.Column(3).(*array.String).ValueStr(i)
				for i, q := range d.Queries {
					tick := time.Now()
					query := q
					randText := fmt.Sprintf("%x", time.Now().Nanosecond())
					err := os.MkdirAll(fmt.Sprintf("%s/%s/%s/year=%s/month=%s/day=%s/hour=%s/", d.ExportPath, d.LogName, d.QueriesNames[i], year, month, day, hour), os.ModePerm)
					if err != nil {
						return fmt.Errorf("error creating path")
					}
					query = strings.ReplaceAll(query, "{{exportpath}}", d.ExportPath)
					query = strings.ReplaceAll(query, "{{logname}}", d.LogName)
					query = strings.ReplaceAll(query, "{{queryname}}", d.QueriesNames[i])
					query = strings.ReplaceAll(query, "{{year}}", year)
					query = strings.ReplaceAll(query, "{{month}}", month)
					query = strings.ReplaceAll(query, "{{day}}", day)
					query = strings.ReplaceAll(query, "{{hour}}", hour)
					query = strings.ReplaceAll(query, "{{rand}}", randText)
					_, err = conn.Exec(context.Background(), query)
					if err != nil {
						return fmt.Errorf("query %d error: %s", i, query)
					}
					log.Printf("query %d %s-%s-%s %s:00 : %v secs\n", i, year, month, day, hour, time.Since(tick).Seconds())
				}
			}
		}
	}

	return nil
}
