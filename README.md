# Dockerization

how to build image
```
docker build -t quill .
```

how to run test environment
```
docker compose -f docker-compose-local-test.yaml up -d
```

after that we can start container with our tool. Please note, that we provide a config dedicated for local test and password is the default as well. On production the config and password should be different:
```
docker run -e QUILL_DB_PASSWORD=quill -p 8081:8081 --network=quill_quill_network -v $(pwd)/config-docker.yaml:/app/config.yaml:ro quill
```
