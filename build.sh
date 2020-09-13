#!/bin/bash
export CGO_ENABLED=1
go build --tags 'icu json fts5 json1'