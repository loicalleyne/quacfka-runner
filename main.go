package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/loicalleyne/quacfka-runner/config"
	"github.com/loicalleyne/quacfka-runner/rpc"

	"github.com/alecthomas/kingpin/v2"
	"github.com/valyala/gorpc"
)

var (
	conf        config.Server
	reqChan     chan rpc.Request
	addr        = kingpin.Flag("address", "socket address").Default("./gorpc-sock.unix").Envar("RUNNER_SOCKET").Short('r').String()
	server      *gorpc.Server
	configPath  = kingpin.Flag("config", "Path to config").Default("./config/server.toml").String()
	parquetPath = kingpin.Flag("parquetpath", "path to parquets").Default("./parquet/").Envar("PARQUET_PATH").Short('p').String()
	bucketName  = kingpin.Flag("bucket", "GCS bucket name").Default("bucket").Envar("GCS_BUCKET").Short('b').String()
)

func main() {
	// Parse flags
	kingpin.Parse()
	err := config.ReadServer(*configPath, &conf)
	if err != nil {
		log.Fatalf("could not read config in %s : %v\n", *configPath, err)
	}
	*parquetPath = strings.TrimSuffix(*parquetPath, "/") + "/"
	if _, err := os.Stat(*parquetPath); err != nil {
		log.Fatalf("parquet path error for %s : %v\n", *parquetPath, err)
	}
	log.Printf("watching path: %s\nbucket: %s\n", *parquetPath, *bucketName)
	go sweep(context.Background(), *parquetPath, *bucketName)
	reqChan = make(chan rpc.Request, 100)
	defer close(reqChan)
	go feedRequests()
	registerTypes()
	switch runtime.GOOS {
	case "linux":
		_, err := os.Stat(*addr)
		if err == nil {
			os.Remove(*addr)
		}
		server = gorpc.NewUnixServer(*addr, handleQueryRequests)
		log.Printf("starting Unix server on %s\n", *addr)
	default:
		*addr = fmt.Sprintf("%s:%d", conf.RPC.Host, conf.RPC.Port)
		server = gorpc.NewTCPServer(*addr, handleQueryRequests)
		log.Printf("starting TCP server on %s\n", *addr)
	}

	go ctrlC()

	if err := server.Serve(); err != nil {
		log.Printf("tpc rpc server stopped %v\n", err)
		os.Exit(2)
	}
}

func ctrlC() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		server.Stop()
		log.Printf("Closing")
		fmt.Fprintf(os.Stdout, "Bye👋\n")
	}()
}

func feedRequests() {
	for req := range reqChan {
		tick := time.Now()
		log.Printf("starting queries on %s\n", filepath.Base(req.Path))
		err := RunPartitionedQueries(req)
		if err != nil {
			log.Printf("error: %v\n", err)
		}
		log.Printf("queries completed, run time %v sec\n", time.Since(tick).Seconds())
		err = os.Remove(req.Path)
		if err != nil {
			log.Printf("deletion error: %v\n", err)
		}
		log.Printf("%s deleted\n", req.Path)
	}
}

func handleQueryRequests(clientAddr string, r any) any {
	req, ok := r.(rpc.Request)
	if !ok {
		fmt.Printf("server: \n%T %+v\n", r, r)
		return nil
	}
	switch req.Type {
	case rpc.REQUEST_PING:
		log.Println("ping received", req)
		return pong(req)
	case rpc.REQUEST_RUN:
		reqChan <- req
		return successfulRequest(req)
	case rpc.REQUEST_VALIDATE:
		return validateRequest(req)
	default:
	}

	// If we get this far then we have an invalid request.
	log.Println("invalid request", "request", req)
	return invalidRequest(req, nil)
}

func validateRequest(r rpc.Request) rpc.Response {
	if r.Path == "" {
		return invalidRequest(r, fmt.Errorf("invalid path"))
	}
	if r.ExportPath == "" {
		return invalidRequest(r, fmt.Errorf("invalid export path"))
	}
	if len(r.ExecQueries) != len(r.ExecQueriesNames) {
		return invalidRequest(r, fmt.Errorf("execqueries and execqueriesnames mismatch"))
	}
	if len(r.Queries) != len(r.QueriesNames) {
		return invalidRequest(r, fmt.Errorf("queries and queriesnames mismatch"))
	}
	return successfulRequest(r)
}

func registerTypes() {
	gorpc.RegisterType(rpc.Request{})
	gorpc.RegisterType(rpc.Response{})
}

func runnerError(request rpc.Request, err error) rpc.Response {
	return rpc.Response{
		Request: request,
		Status:  rpc.RESPONSE_RUNNER_ERROR,
		Error:   err,
	}
}

// The unexported func pong just sends a response back to the
// client with the original request and a RESPONSE_OK. This
// is to be used if the client requests a pong (by sending
// a ping).
func pong(request rpc.Request) rpc.Response {
	return rpc.Response{
		Request: request,
		Status:  rpc.RESPONSE_OK,
	}
}

// The unexported func invalidRequest just sends a resposne back
// to the client with the original request and the status
// RESPONSE_REQUEST_TYPE_INVALID. This is to be used if
// a client sends an unknown request.
func invalidRequest(request rpc.Request, err error) rpc.Response {
	return rpc.Response{
		Request: request,
		Status:  rpc.RESPONSE_REQUEST_TYPE_INVALID,
		Error:   err,
	}
}

func successfulRequest(request rpc.Request) rpc.Response {
	return rpc.Response{
		Request: request,
		Status:  rpc.RESPONSE_OK,
	}
}
