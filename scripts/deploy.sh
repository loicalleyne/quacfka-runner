#!/bin/bash
go build -o quacfka-runner
mkdir -p /usr/local/bin/quacfka-service
cp ./quacfka-runner /usr/local/bin/quacfka-service/
mkdir -p /usr/local/bin/quacfka-service/parquet
mkdir -p /usr/local/bin/quacfka-service/config
cp ./config/server.toml /usr/local/bin/quacfka-service/config/
systemctl stop quacfka-runner.service
systemctl disable quacfka-runner.service
cp quacfka-runner.service /lib/systemd/system/quacfka-runner.service
systemctl enable quacfka-runner.service && systemctl start quacfka-runner.service