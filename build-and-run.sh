#!/bin/bash

echo "options: $@"

go build -buildvcs=false . && ./quill --config config-docker.yaml $@
