# here we will test the quill
FROM golang:1.25-bookworm

RUN apt-get update && apt-get install -y pkg-config ca-certificates git

# Create workspace directory
WORKDIR /workspace
