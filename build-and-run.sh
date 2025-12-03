#!/bin/bash

echo "options: $@"

go build -buildvcs=false . && ./quill $@
