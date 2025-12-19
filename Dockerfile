FROM debian:12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY quill /app/quill
RUN chmod +x /app/quill

EXPOSE 8081

CMD ["/app/quill", "--config", "/app/config.yaml"]
