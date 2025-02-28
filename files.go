package main

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/TFMV/filewalker"
)

type processedFile struct {
	path string
	uri  string
}

func sweep(ctx context.Context, parquetPath, sinkbucket string) {
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("gcsClient error: %v\n", err)
	}
	defer gcsClient.Close()
	var opt filewalker.WalkOptions
	opt.Filter = filewalker.FilterOptions{
		IncludeTypes: []string{".parquet"},
	}
	for {
		delay := time.NewTimer(20 * time.Second)
		select {
		case <-ctx.Done():
			return
		case <-delay.C:
			opt.Filter.ModifiedBefore = time.Now().Add(-5 * time.Second)
			err = filewalker.WalkLimitWithOptions(context.Background(), parquetPath, func(path string, info fs.FileInfo, err error) error {
				if info.IsDir() {
					return nil
				}
				if filepath.Ext(info.Name()) == ".parquet" && time.Since(info.ModTime()) > (5*time.Second) {
					var f processedFile
					f.path = path
					f.uri = strings.TrimPrefix(path, parquetPath)
					gcsUpload(context.Background(), gcsClient, f, sinkbucket, info.Size())
				}
				return nil
			}, opt)
			if err != nil {
				log.Printf("error: %v\n", err)
			}
			for range 4 {
				err = filepath.Walk(parquetPath, func(path string, info fs.FileInfo, err error) error {
					if err != nil {
						fmt.Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err)
						return err
					}
					if info.IsDir() && path != parquetPath && time.Since(info.ModTime()) > (5*time.Second) {
						processDir(path)
					}
					return nil
				})
				if err != nil {
					log.Printf("error: %v\n", err)
				}
			}
		}
	}
}

func processDir(path string) {
	files, err := os.ReadDir(path)
	if err != nil {
		log.Printf("error: %v\n", err)
		return
	}

	if len(files) != 0 {
		return
	}

	err = os.Remove(path)
	if err != nil {
		log.Printf("error: %v\n", err)
		return
	}
	log.Println(path, "removed")
}

func gcsUpload(ctx context.Context, gcsClient *storage.Client, f processedFile, sinkbucket string, size int64) {
	log.Printf("Upload START %s %s\n", f.uri, formatBytes(size))
	// transfer parquet file to sink bucket
	bkt := gcsClient.Bucket(sinkbucket)
	file, err := os.Open(f.path)
	if err != nil {
		log.Printf("Upload ERROR %s : %v\n", f.uri, err)
	}
	defer file.Close()
	obj := bkt.Object(f.uri)
	w := obj.NewWriter(ctx)
	if _, err := io.Copy(w, file); err != nil {
		log.Printf("Upload ERROR %s : %v\n", f.uri, err)
	}

	if err := w.Close(); err != nil {
		log.Printf("Upload ERROR %s : %v\n", f.uri, err)
	}
	file.Close()
	// clean up local file
	if err := os.Remove(f.path); err != nil {
		log.Printf("Upload ERROR %s : %v\n", f.path, err)
	}
	log.Printf("Upload DONE %s\n", f.uri)
}
